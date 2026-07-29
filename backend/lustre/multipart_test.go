// Copyright 2026 Versity Software
// This file is licensed under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//go:build cgo && linux

package lustre

import (
	"bytes"
	"context"
	"crypto/md5"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/versity/versitygw/backend"
	"github.com/versity/versitygw/backend/meta"
	"github.com/versity/versitygw/backend/posix"
	"github.com/versity/versitygw/s3err"
	"github.com/versity/versitygw/s3response"
)

const testBucket = "testbucket"

// newTestBackend builds a Lustre backend over a temporary gateway root with
// sqlite metadata. The posix constructor changes the process working
// directory, so these tests cannot run in parallel with each other.
func newTestBackend(t *testing.T, opts Opts) *Lustre {
	t.Helper()

	cwd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	t.Cleanup(func() { os.Chdir(cwd) })

	gwroot := t.TempDir()
	metadir := t.TempDir()

	ms, err := meta.NewSQLite(metadir)
	if err != nil {
		t.Fatalf("NewSQLite: %v", err)
	}

	if opts.Posix.NewDirPerm == 0 {
		opts.Posix.NewDirPerm = 0755
	}
	if opts.PartSize == 0 && !opts.DisableDirectMultipart {
		opts.PartSize = backend.MinPartSize
	}
	opts.MetaStore = ms

	be, err := New(gwroot, ms, opts)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(be.Shutdown)

	bucket := testBucket
	err = be.CreateBucket(context.Background(), &s3.CreateBucketInput{
		Bucket:                    &bucket,
		CreateBucketConfiguration: &types.CreateBucketConfiguration{},
	}, []byte(`{"Owner":"testuser"}`))
	if err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}

	return be
}

func randBytes(n int) []byte {
	b := make([]byte, n)
	// Deterministic content keeps a failure reproducible.
	r := rand.New(rand.NewSource(int64(n)))
	r.Read(b)
	return b
}

func inode(t *testing.T, path string) uint64 {
	t.Helper()

	fi, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %q: %v", path, err)
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		t.Fatal("no stat_t available")
	}
	return st.Ino
}

// uploadParts pushes the payloads named by order as parts, and returns the
// completed part list covering every payload. Parts left out of order are
// assumed to be on the server already, which is what the re-upload test needs.
func uploadParts(t *testing.T, be *Lustre, key, uploadID string, payloads [][]byte, order []int) []types.CompletedPart {
	t.Helper()

	ctx := context.Background()
	bucket := testBucket

	etags := make([]string, len(payloads))
	for i, body := range payloads {
		etags[i] = fmt.Sprintf("\"%x\"", md5.Sum(body))
	}

	for _, idx := range order {
		part := int32(idx + 1)
		body := payloads[idx]
		length := int64(len(body))

		out, err := be.UploadPart(ctx, &s3.UploadPartInput{
			Bucket:        &bucket,
			Key:           &key,
			UploadId:      &uploadID,
			PartNumber:    &part,
			ContentLength: &length,
			Body:          bytes.NewReader(body),
		})
		if err != nil {
			t.Fatalf("UploadPart %d: %v", part, err)
		}
		if *out.ETag != etags[idx] {
			t.Errorf("part %d etag = %s, want %s", part, *out.ETag, etags[idx])
		}
	}

	parts := make([]types.CompletedPart, 0, len(payloads))
	for i := range payloads {
		part := int32(i + 1)
		parts = append(parts, types.CompletedPart{
			PartNumber: &part,
			ETag:       &etags[i],
		})
	}

	return parts
}

func readObject(t *testing.T, be *Lustre, key string) []byte {
	t.Helper()

	bucket := testBucket
	out, err := be.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &key,
	})
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	defer out.Body.Close()

	b, err := io.ReadAll(out.Body)
	if err != nil {
		t.Fatalf("read object: %v", err)
	}
	return b
}

// runMultipart drives a whole upload and returns the object contents along
// with the inode the staging file had before completion.
func runMultipart(t *testing.T, be *Lustre, key string, payloads [][]byte, order []int) (data []byte, stagingIno uint64, etag string) {
	t.Helper()

	ctx := context.Background()
	bucket := testBucket

	mpu, err := be.CreateMultipartUpload(ctx, s3response.CreateMultipartUploadInput{
		Bucket: &bucket,
		Key:    &key,
	})
	if err != nil {
		t.Fatalf("CreateMultipartUpload: %v", err)
	}

	parts := uploadParts(t, be, key, mpu.UploadId, payloads, order)

	stagingIno = inode(t, stagingPath(filepath.Join(bucket, uploadDir(key, mpu.UploadId))))

	res, _, err := be.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:          &bucket,
		Key:             &key,
		UploadId:        &mpu.UploadId,
		MultipartUpload: &types.CompletedMultipartUpload{Parts: parts},
	})
	if err != nil {
		t.Fatalf("CompleteMultipartUpload: %v", err)
	}

	wantEtag, err := backend.GetMultipartMD5(parts)
	if err != nil {
		t.Fatalf("GetMultipartMD5: %v", err)
	}
	if res.ETag == nil || *res.ETag != wantEtag {
		t.Errorf("etag = %v, want %v", res.ETag, wantEtag)
	}

	return readObject(t, be, key), stagingIno, wantEtag
}

// assertAPIError checks that err is the S3 error the client should see, rather
// than an internal failure that would surface as a 500.
func assertAPIError(t *testing.T, err error, code string, status int) {
	t.Helper()

	var s3e s3err.S3Error
	if !errors.As(err, &s3e) {
		t.Fatalf("got %T (%v), want an s3err.S3Error", err, err)
	}
	if got := s3e.BaseError().Code; got != code {
		t.Errorf("error code = %q, want %q (%v)", got, code, s3e.BaseError().Description)
	}
	if got := s3e.StatusCode(); got != status {
		t.Errorf("status = %d, want %d", got, status)
	}
}

func assertUploadCleanedUp(t *testing.T, key string) {
	t.Helper()

	dir := filepath.Join(testBucket, objdir(key))
	if _, err := os.Stat(dir); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("upload directory %q survived completion: %v", dir, err)
	}

	// Nothing may be left over in the bucket scratch directory either.
	ents, err := os.ReadDir(filepath.Join(testBucket, metaTmpDir))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("read scratch dir: %v", err)
	}
	for _, e := range ents {
		if e.Name() != "multipart" {
			t.Errorf("leftover file in scratch dir: %s", e.Name())
		}
	}
}

// TestMultipartUniformIsZeroCopy is the case every real uploader produces:
// equally sized parts with a shorter last one. The finished object must be the
// staging file itself, so its inode is unchanged and no byte was copied.
func TestMultipartUniformIsZeroCopy(t *testing.T) {
	be := newTestBackend(t, Opts{})

	key := "uniform/object"
	payloads := [][]byte{
		randBytes(backend.MinPartSize),
		randBytes(backend.MinPartSize),
		randBytes(1024),
	}

	data, stagingIno, _ := runMultipart(t, be, key, payloads, []int{0, 1, 2})

	want := bytes.Join(payloads, nil)
	if !bytes.Equal(data, want) {
		t.Fatalf("object contents differ: got %d bytes, want %d", len(data), len(want))
	}

	if got := inode(t, filepath.Join(testBucket, key)); got != stagingIno {
		t.Errorf("object inode %d != staging inode %d, the data was copied", got, stagingIno)
	}

	assertUploadCleanedUp(t, key)
}

// TestMultipartOutOfOrderIsZeroCopy checks that parts arriving in any order
// still land in their slots, since uploaders send them concurrently.
func TestMultipartOutOfOrderIsZeroCopy(t *testing.T) {
	be := newTestBackend(t, Opts{})

	key := "out-of-order"
	payloads := [][]byte{
		randBytes(backend.MinPartSize),
		randBytes(backend.MinPartSize),
		randBytes(backend.MinPartSize),
		randBytes(4096),
	}

	data, stagingIno, _ := runMultipart(t, be, key, payloads, []int{2, 0, 3, 1})

	if !bytes.Equal(data, bytes.Join(payloads, nil)) {
		t.Fatal("object contents differ")
	}
	if got := inode(t, filepath.Join(testBucket, key)); got != stagingIno {
		t.Errorf("object inode %d != staging inode %d, the data was copied", got, stagingIno)
	}
}

// TestMultipartRaggedRejected covers a non-final part that is shorter than the
// configured size. Its successors are already sitting at their slot offsets, so
// the object cannot be assembled without a copy and the request is refused
// rather than silently taking the slow path.
func TestMultipartRaggedRejected(t *testing.T) {
	be := newTestBackend(t, Opts{PartSize: 2 * backend.MinPartSize})

	ctx := context.Background()
	bucket := testBucket
	key := "ragged"

	mpu, err := be.CreateMultipartUpload(ctx, s3response.CreateMultipartUploadInput{
		Bucket: &bucket,
		Key:    &key,
	})
	if err != nil {
		t.Fatalf("CreateMultipartUpload: %v", err)
	}

	// Part 1 is a legal S3 part size but not the configured one, which
	// leaves every later part sitting past where it belongs.
	payloads := [][]byte{
		randBytes(backend.MinPartSize),
		randBytes(2 * backend.MinPartSize),
		randBytes(2048),
	}
	parts := uploadParts(t, be, key, mpu.UploadId, payloads, []int{0, 1, 2})

	_, _, err = be.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:          &bucket,
		Key:             &key,
		UploadId:        &mpu.UploadId,
		MultipartUpload: &types.CompletedMultipartUpload{Parts: parts},
	})
	if err == nil {
		t.Fatal("completing with a short non-final part should fail")
	}
	assertAPIError(t, err, "InvalidRequest", 400)
}

// TestMultipartGapRejected covers completing with a subset that leaves a hole
// in the part numbers. Plain S3 allows it, but the surviving parts would then
// need to move.
func TestMultipartGapRejected(t *testing.T) {
	be := newTestBackend(t, Opts{})

	ctx := context.Background()
	bucket := testBucket
	key := "gapped"

	mpu, err := be.CreateMultipartUpload(ctx, s3response.CreateMultipartUploadInput{
		Bucket: &bucket,
		Key:    &key,
	})
	if err != nil {
		t.Fatalf("CreateMultipartUpload: %v", err)
	}

	payloads := [][]byte{
		randBytes(backend.MinPartSize),
		randBytes(backend.MinPartSize),
		randBytes(1024),
	}
	all := uploadParts(t, be, key, mpu.UploadId, payloads, []int{0, 1, 2})

	// Complete with parts 2 and 3 only.
	_, _, err = be.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:          &bucket,
		Key:             &key,
		UploadId:        &mpu.UploadId,
		MultipartUpload: &types.CompletedMultipartUpload{Parts: all[1:]},
	})
	if err == nil {
		t.Fatal("completing with a gap in the part numbers should fail")
	}
	assertAPIError(t, err, "InvalidRequest", 400)
}

// TestOversizedPartRejected checks that a part larger than the configured size
// is refused at upload time, before it can run into its neighbour's region.
func TestOversizedPartRejected(t *testing.T) {
	be := newTestBackend(t, Opts{PartSize: backend.MinPartSize})

	ctx := context.Background()
	bucket := testBucket
	key := "oversized"

	mpu, err := be.CreateMultipartUpload(ctx, s3response.CreateMultipartUploadInput{
		Bucket: &bucket,
		Key:    &key,
	})
	if err != nil {
		t.Fatalf("CreateMultipartUpload: %v", err)
	}

	body := randBytes(backend.MinPartSize + 1)
	length := int64(len(body))
	part := int32(1)

	_, err = be.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:        &bucket,
		Key:           &key,
		UploadId:      &mpu.UploadId,
		PartNumber:    &part,
		ContentLength: &length,
		Body:          bytes.NewReader(body),
	})
	if err == nil {
		t.Fatal("uploading a part larger than the configured size should fail")
	}
	assertAPIError(t, err, "EntityTooLarge", 400)

	// The rejected part must leave nothing behind.
	updir := filepath.Join(bucket, uploadDir(key, mpu.UploadId))
	if _, err := os.Stat(partMarker(updir, part)); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("rejected part left a marker behind: %v", err)
	}
}

// TestPartSizeChangedUnderUpload covers a gateway restarted with a different
// part size while an upload is in flight. The parts already written are laid
// out for the old size, so the upload must not be continued.
func TestPartSizeChangedUnderUpload(t *testing.T) {
	be := newTestBackend(t, Opts{PartSize: backend.MinPartSize})

	ctx := context.Background()
	bucket := testBucket
	key := "restarted"

	mpu, err := be.CreateMultipartUpload(ctx, s3response.CreateMultipartUploadInput{
		Bucket: &bucket,
		Key:    &key,
	})
	if err != nil {
		t.Fatalf("CreateMultipartUpload: %v", err)
	}

	// Stand in for a restart under a different --mpu-part-size.
	be.partSize = backend.MinPartSize * 2

	body := randBytes(1024)
	length := int64(len(body))
	part := int32(1)

	_, err = be.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:        &bucket,
		Key:           &key,
		UploadId:      &mpu.UploadId,
		PartNumber:    &part,
		ContentLength: &length,
		Body:          bytes.NewReader(body),
	})
	if err == nil {
		t.Fatal("uploading into an upload laid out for another part size should fail")
	}
	assertAPIError(t, err, "InvalidRequest", 400)
}

// TestMultipartShortPartFirst confirms the configured size holds regardless of
// arrival order, including when the short final part lands first.
func TestMultipartShortPartFirst(t *testing.T) {
	be := newTestBackend(t, Opts{PartSize: backend.MinPartSize})

	key := "pinned"
	payloads := [][]byte{
		randBytes(backend.MinPartSize),
		randBytes(backend.MinPartSize),
		randBytes(512),
	}

	// The last, short part arrives first.
	data, stagingIno, _ := runMultipart(t, be, key, payloads, []int{2, 1, 0})

	if !bytes.Equal(data, bytes.Join(payloads, nil)) {
		t.Fatal("object contents differ")
	}
	if got := inode(t, filepath.Join(testBucket, key)); got != stagingIno {
		t.Errorf("object inode %d != staging inode %d, the data was copied", got, stagingIno)
	}
}

// TestMultipartSinglePart covers uploads small enough to be one part, where
// the minimum part size does not apply.
func TestMultipartSinglePart(t *testing.T) {
	be := newTestBackend(t, Opts{})

	key := "single"
	payloads := [][]byte{randBytes(1234)}

	data, stagingIno, _ := runMultipart(t, be, key, payloads, []int{0})

	if !bytes.Equal(data, payloads[0]) {
		t.Fatal("object contents differ")
	}
	if got := inode(t, filepath.Join(testBucket, key)); got != stagingIno {
		t.Errorf("object inode %d != staging inode %d, the data was copied", got, stagingIno)
	}
}

// TestMultipartReupload overwrites a part before completing, which clients do
// when a part upload fails and is retried.
func TestMultipartReupload(t *testing.T) {
	be := newTestBackend(t, Opts{})

	ctx := context.Background()
	bucket := testBucket
	key := "reuploaded"

	mpu, err := be.CreateMultipartUpload(ctx, s3response.CreateMultipartUploadInput{
		Bucket: &bucket,
		Key:    &key,
	})
	if err != nil {
		t.Fatalf("CreateMultipartUpload: %v", err)
	}

	first := randBytes(backend.MinPartSize)
	second := randBytes(4096)

	uploadParts(t, be, key, mpu.UploadId, [][]byte{first, second}, []int{0, 1})

	// Replace part 2 with different content of a different length.
	replacement := randBytes(8192)
	parts := uploadParts(t, be, key, mpu.UploadId, [][]byte{first, replacement}, []int{1})

	if _, _, err := be.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:          &bucket,
		Key:             &key,
		UploadId:        &mpu.UploadId,
		MultipartUpload: &types.CompletedMultipartUpload{Parts: parts},
	}); err != nil {
		t.Fatalf("CompleteMultipartUpload: %v", err)
	}

	want := append(append([]byte{}, first...), replacement...)
	if got := readObject(t, be, key); !bytes.Equal(got, want) {
		t.Fatalf("object contents differ: got %d bytes, want %d", len(got), len(want))
	}
}

// TestListPartsReportsSizes checks that the sparse markers give listings the
// same view the posix backend would produce from real part files.
func TestListPartsReportsSizes(t *testing.T) {
	be := newTestBackend(t, Opts{})

	ctx := context.Background()
	bucket := testBucket
	key := "listed"

	mpu, err := be.CreateMultipartUpload(ctx, s3response.CreateMultipartUploadInput{
		Bucket: &bucket,
		Key:    &key,
	})
	if err != nil {
		t.Fatalf("CreateMultipartUpload: %v", err)
	}

	payloads := [][]byte{
		randBytes(backend.MinPartSize),
		randBytes(2048),
	}
	uploadParts(t, be, key, mpu.UploadId, payloads, []int{0, 1})

	maxParts := int32(100)
	res, err := be.ListParts(ctx, &s3.ListPartsInput{
		Bucket:   &bucket,
		Key:      &key,
		UploadId: &mpu.UploadId,
		MaxParts: &maxParts,
	})
	if err != nil {
		t.Fatalf("ListParts: %v", err)
	}

	if len(res.Parts) != len(payloads) {
		t.Fatalf("got %d parts, want %d", len(res.Parts), len(payloads))
	}
	for i, p := range res.Parts {
		if p.PartNumber != i+1 {
			t.Errorf("part %d has number %d", i, p.PartNumber)
		}
		if p.Size != int64(len(payloads[i])) {
			t.Errorf("part %d size = %d, want %d", p.PartNumber, p.Size, len(payloads[i]))
		}
	}
}

// TestAbortRemovesStaging makes sure an aborted upload frees the blocks the
// staging file was holding.
func TestAbortRemovesStaging(t *testing.T) {
	be := newTestBackend(t, Opts{})

	ctx := context.Background()
	bucket := testBucket
	key := "aborted"

	mpu, err := be.CreateMultipartUpload(ctx, s3response.CreateMultipartUploadInput{
		Bucket: &bucket,
		Key:    &key,
	})
	if err != nil {
		t.Fatalf("CreateMultipartUpload: %v", err)
	}

	uploadParts(t, be, key, mpu.UploadId, [][]byte{randBytes(backend.MinPartSize)}, []int{0})

	updir := filepath.Join(bucket, uploadDir(key, mpu.UploadId))
	if _, err := os.Stat(stagingPath(updir)); err != nil {
		t.Fatalf("staging file missing before abort: %v", err)
	}

	err = be.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
		Bucket:   &bucket,
		Key:      &key,
		UploadId: &mpu.UploadId,
	})
	if err != nil {
		t.Fatalf("AbortMultipartUpload: %v", err)
	}

	if _, err := os.Stat(updir); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("upload directory survived abort: %v", err)
	}
}

// TestCompleteIsIdempotent covers a client retrying a complete request whose
// response it never saw.
func TestCompleteIsIdempotent(t *testing.T) {
	be := newTestBackend(t, Opts{})

	ctx := context.Background()
	bucket := testBucket
	key := "retried"

	mpu, err := be.CreateMultipartUpload(ctx, s3response.CreateMultipartUploadInput{
		Bucket: &bucket,
		Key:    &key,
	})
	if err != nil {
		t.Fatalf("CreateMultipartUpload: %v", err)
	}

	payloads := [][]byte{randBytes(backend.MinPartSize), randBytes(64)}
	parts := uploadParts(t, be, key, mpu.UploadId, payloads, []int{0, 1})

	input := &s3.CompleteMultipartUploadInput{
		Bucket:          &bucket,
		Key:             &key,
		UploadId:        &mpu.UploadId,
		MultipartUpload: &types.CompletedMultipartUpload{Parts: parts},
	}

	first, _, err := be.CompleteMultipartUpload(ctx, input)
	if err != nil {
		t.Fatalf("CompleteMultipartUpload: %v", err)
	}

	second, _, err := be.CompleteMultipartUpload(ctx, input)
	if err != nil {
		t.Fatalf("CompleteMultipartUpload retry: %v", err)
	}

	if *first.ETag != *second.ETag {
		t.Errorf("retry etag = %v, want %v", *second.ETag, *first.ETag)
	}
	if !bytes.Equal(readObject(t, be, key), bytes.Join(payloads, nil)) {
		t.Error("object contents differ after retry")
	}
}

// TestCompleteRejectsUnknownUpload checks the error for an upload that never
// existed.
func TestCompleteRejectsUnknownUpload(t *testing.T) {
	be := newTestBackend(t, Opts{})

	bucket := testBucket
	key := "missing"
	uploadID := "does-not-exist"
	etag := "\"d41d8cd98f00b204e9800998ecf8427e\""
	num := int32(1)

	_, _, err := be.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{
		Bucket:   &bucket,
		Key:      &key,
		UploadId: &uploadID,
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: []types.CompletedPart{{PartNumber: &num, ETag: &etag}},
		},
	})
	if err == nil {
		t.Fatal("completing an unknown upload should fail")
	}
}

// TestPartTooSmall enforces the S3 rule that only the last part may be under
// the minimum size.
func TestPartTooSmall(t *testing.T) {
	be := newTestBackend(t, Opts{})

	ctx := context.Background()
	bucket := testBucket
	key := "too-small"

	mpu, err := be.CreateMultipartUpload(ctx, s3response.CreateMultipartUploadInput{
		Bucket: &bucket,
		Key:    &key,
	})
	if err != nil {
		t.Fatalf("CreateMultipartUpload: %v", err)
	}

	payloads := [][]byte{randBytes(1024), randBytes(1024)}
	parts := uploadParts(t, be, key, mpu.UploadId, payloads, []int{0, 1})

	_, _, err = be.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:          &bucket,
		Key:             &key,
		UploadId:        &mpu.UploadId,
		MultipartUpload: &types.CompletedMultipartUpload{Parts: parts},
	})
	if err == nil {
		t.Fatal("completing with an undersized non-final part should fail")
	}
}

// TestDisableDirectMultipart makes sure the escape hatch really hands the work
// back to the posix implementation.
func TestDisableDirectMultipart(t *testing.T) {
	be := newTestBackend(t, Opts{DisableDirectMultipart: true})

	ctx := context.Background()
	bucket := testBucket
	key := "posix-path"

	mpu, err := be.CreateMultipartUpload(ctx, s3response.CreateMultipartUploadInput{
		Bucket: &bucket,
		Key:    &key,
	})
	if err != nil {
		t.Fatalf("CreateMultipartUpload: %v", err)
	}

	// No staging file is created when the direct path is off.
	updir := filepath.Join(bucket, uploadDir(key, mpu.UploadId))
	if _, err := os.Stat(stagingPath(updir)); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("staging file created with direct multipart disabled: %v", err)
	}

	payloads := [][]byte{randBytes(backend.MinPartSize), randBytes(77)}
	parts := uploadParts(t, be, key, mpu.UploadId, payloads, []int{0, 1})

	if _, _, err := be.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:          &bucket,
		Key:             &key,
		UploadId:        &mpu.UploadId,
		MultipartUpload: &types.CompletedMultipartUpload{Parts: parts},
	}); err != nil {
		t.Fatalf("CompleteMultipartUpload: %v", err)
	}

	if !bytes.Equal(readObject(t, be, key), bytes.Join(payloads, nil)) {
		t.Error("object contents differ")
	}
}

func TestNewRejectsVersioning(t *testing.T) {
	cwd, _ := os.Getwd()
	t.Cleanup(func() { os.Chdir(cwd) })

	_, err := New(t.TempDir(), meta.NoMeta{}, Opts{
		Posix: posix.PosixOpts{VersioningDir: t.TempDir(), NewDirPerm: 0755},
	})
	if err == nil {
		t.Error("direct multipart with versioning should be rejected")
	}
}

func TestNewRequiresMetaStore(t *testing.T) {
	cwd, _ := os.Getwd()
	t.Cleanup(func() { os.Chdir(cwd) })

	if _, err := New(t.TempDir(), nil, Opts{}); err == nil {
		t.Error("a nil metadata storer should be rejected")
	}
}

func TestMisplacedPart(t *testing.T) {
	tests := []struct {
		name  string
		parts []completedPart
		want  int32
	}{
		{
			name: "uniform slots",
			parts: []completedPart{
				{number: 1, size: 100, wantOffset: 0, slotOffset: 0},
				{number: 2, size: 100, wantOffset: 100, slotOffset: 100},
				{number: 3, size: 20, wantOffset: 200, slotOffset: 200},
			},
			want: 0,
		},
		{
			name: "short middle part",
			parts: []completedPart{
				{number: 1, size: 100, wantOffset: 0, slotOffset: 0},
				{number: 2, size: 50, wantOffset: 100, slotOffset: 100},
				{number: 3, size: 100, wantOffset: 150, slotOffset: 200},
			},
			want: 3,
		},
		{
			name: "gap from a skipped part number",
			parts: []completedPart{
				{number: 1, size: 100, wantOffset: 0, slotOffset: 0},
				{number: 3, size: 100, wantOffset: 100, slotOffset: 200},
			},
			want: 3,
		},
		{
			name: "does not start at part 1",
			parts: []completedPart{
				{number: 2, size: 100, wantOffset: 0, slotOffset: 100},
			},
			want: 2,
		},
		{
			name:  "no parts",
			parts: nil,
			want:  0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, bad := misplacedPart(tt.parts)
			if !bad {
				if tt.want != 0 {
					t.Errorf("got no misplaced part, want part %d", tt.want)
				}
				return
			}
			if tt.want == 0 {
				t.Fatalf("got misplaced part %d, want none", p.number)
			}
			if p.number != tt.want {
				t.Errorf("misplaced part = %d, want %d", p.number, tt.want)
			}
		})
	}
}

func TestSlotSizeRecord(t *testing.T) {
	dir := t.TempDir()

	if got, err := readSlotSize(dir); err != nil || got != 0 {
		t.Errorf("readSlotSize on a fresh dir = %d, %v; want 0, nil", got, err)
	}

	if err := writeSlotSize(dir, 4096); err != nil {
		t.Fatalf("writeSlotSize: %v", err)
	}

	got, err := readSlotSize(dir)
	if err != nil {
		t.Fatalf("readSlotSize: %v", err)
	}
	if got != 4096 {
		t.Errorf("readSlotSize = %d, want 4096", got)
	}

	// The record is written once when the upload is created.
	if err := writeSlotSize(dir, 8192); err == nil {
		t.Error("writeSlotSize over an existing record should fail")
	}
}

func TestNewRequiresPartSize(t *testing.T) {
	cwd, _ := os.Getwd()
	t.Cleanup(func() { os.Chdir(cwd) })

	if _, err := New(t.TempDir(), meta.NoMeta{}, Opts{
		Posix: posix.PosixOpts{NewDirPerm: 0755},
	}); err == nil {
		t.Error("direct multipart without a part size should be rejected")
	}

	// The copying path has no fixed layout, so it does not need one.
	if _, err := New(t.TempDir(), meta.NoMeta{}, Opts{
		Posix:                  posix.PosixOpts{NewDirPerm: 0755},
		DisableDirectMultipart: true,
	}); err != nil {
		t.Errorf("copying multipart without a part size: %v", err)
	}
}
