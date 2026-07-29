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

package lustre

import (
	"context"
	"crypto/md5"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/versity/versitygw/auth"
	"github.com/versity/versitygw/backend"
	"github.com/versity/versitygw/backend/meta"
	"github.com/versity/versitygw/debuglogger"
	"github.com/versity/versitygw/s3api/middlewares"
	"github.com/versity/versitygw/s3api/utils"
	"github.com/versity/versitygw/s3err"
	"github.com/versity/versitygw/s3response"
)

// CreateMultipartUpload prepares an upload the same way the posix backend
// does and adds the staging file the parts will be written into.
func (l *Lustre) CreateMultipartUpload(ctx context.Context, mpu s3response.CreateMultipartUploadInput) (s3response.InitiateMultipartUploadResult, error) {
	res, err := l.Posix.CreateMultipartUpload(ctx, mpu)
	if err != nil || !l.directMultipart {
		return res, err
	}

	updir := filepath.Join(*mpu.Bucket, uploadDir(*mpu.Key, res.UploadId))
	if err := l.initStaging(updir); err != nil {
		// Do not leave an upload behind that parts cannot be written to.
		_ = l.Posix.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
			Bucket:   mpu.Bucket,
			Key:      mpu.Key,
			UploadId: &res.UploadId,
		})
		return s3response.InitiateMultipartUploadResult{}, err
	}

	return res, nil
}

// initStaging creates the sparse file that holds every part payload, and
// pins the slot stride when one was configured.
func (l *Lustre) initStaging(updir string) error {
	f, err := os.OpenFile(stagingPath(updir), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		return fmt.Errorf("create staging file: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("create staging file: %w", err)
	}

	if l.partSize > 0 {
		if _, err := claimSlotSize(updir, l.partSize); err != nil {
			return err
		}
	}

	return nil
}

// partWriter streams a part body into its region of the staging file, or into
// a spill file when the part does not fit its slot. It enforces the declared
// content length the way the posix temporary file does.
type partWriter struct {
	f       *os.File
	off     int64
	remain  int64
	spilled bool
}

func (w *partWriter) Write(b []byte) (int, error) {
	if int64(len(b)) > w.remain {
		return 0, fmt.Errorf("write exceeds content length %v", w.remain)
	}

	n, err := w.f.WriteAt(b, w.off)
	w.off += int64(n)
	w.remain -= int64(n)
	return n, err
}

func (w *partWriter) Close() error { return w.f.Close() }

// openPartWriter places part in the staging file and returns a writer for its
// payload.
//
// The slot stride is whatever the first part of the upload to arrive declared,
// unless it was pinned up front. Every uploader in practice sends parts of one
// fixed size with a possibly shorter final part, so parts land exactly where
// they belong in the finished object and completing the upload copies nothing.
// A part that does not fit its slot is written to its own file instead, and
// completion falls back to assembling by copy, which is what the posix backend
// does for every upload.
func (l *Lustre) openPartWriter(updir string, part int32, length int64) (*partWriter, error) {
	slot, err := readSlotSize(updir)
	if err != nil {
		return nil, err
	}
	if slot == 0 && length > 0 {
		slot, err = claimSlotSize(updir, length)
		if err != nil {
			return nil, err
		}
	}

	spill := spillPath(updir, part)

	if length > slot {
		debuglogger.Logf("lustre: part %v of %v does not fit the %v byte slot, spilling",
			part, updir, slot)

		f, err := os.OpenFile(spill, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
		if err != nil {
			return nil, fmt.Errorf("open spill file: %w", err)
		}
		return &partWriter{f: f, remain: length, spilled: true}, nil
	}

	// Drop a spill left behind by an earlier attempt at this part number so
	// that completion does not pick up stale data.
	if err := os.Remove(spill); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("remove stale spill file: %w", err)
	}

	f, err := os.OpenFile(stagingPath(updir), os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("open staging file: %w", err)
	}

	return &partWriter{f: f, off: slotOffset(slot, part), remain: length}, nil
}

// markPart publishes a part by creating the sparse marker that gives it a
// size and a modification time for listings. It is the last step of a part
// upload, so a marker only appears once the payload is on disk.
func markPart(updir string, part int32, length int64) error {
	name := partMarker(updir, part)

	f, err := os.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("create part marker: %w", err)
	}
	defer f.Close()

	if err := f.Truncate(length); err != nil {
		return fmt.Errorf("size part marker: %w", err)
	}

	return nil
}

// UploadPart writes a part directly into the region of the staging file that
// it will occupy in the finished object.
func (l *Lustre) UploadPart(ctx context.Context, input *s3.UploadPartInput) (*s3.UploadPartOutput, error) {
	if !l.directMultipart {
		return l.Posix.UploadPart(ctx, input)
	}

	release, err := l.acquireActionSlot(ctx)
	if err != nil {
		return nil, err
	}
	defer release()

	if input.Key == nil {
		return nil, s3err.GetAPIError(s3err.ErrNoSuchKey)
	}

	bucket := *input.Bucket
	object := *input.Key
	uploadID := *input.UploadId
	part := *input.PartNumber

	length := int64(0)
	if input.ContentLength != nil {
		length = *input.ContentLength
	}
	r := input.Body

	if _, err := os.Stat(bucket); errors.Is(err, fs.ErrNotExist) {
		return nil, s3err.GetBucketErr(s3err.ErrNoSuchBucket, bucket)
	} else if err != nil {
		return nil, fmt.Errorf("stat bucket: %w", err)
	}

	mpPath := uploadDir(object, uploadID)
	updir := filepath.Join(bucket, mpPath)

	if _, err := os.Stat(updir); errors.Is(err, fs.ErrNotExist) {
		return nil, s3err.GetNoSuchUploadErr(uploadID)
	} else if err != nil {
		return nil, fmt.Errorf("stat uploadid: %w", err)
	}

	partPath := filepath.Join(mpPath, fmt.Sprint(part))

	w, err := l.openPartWriter(updir, part, length)
	if err != nil {
		if errors.Is(err, syscall.EDQUOT) {
			drainBody(r)
			return nil, s3err.GetAPIError(s3err.ErrQuotaExceeded)
		}
		if errors.Is(err, syscall.ENOSPC) {
			drainBody(r)
			return nil, s3err.GetAPIError(s3err.ErrNoSpaceLeftOnDevice)
		}
		return nil, err
	}
	defer w.Close()

	hash := md5.New()
	tr := io.TeeReader(r, hash)

	chRdr, chunkUpload := input.Body.(middlewares.ChecksumReader)
	isTrailingChecksum := chunkUpload && chRdr.Algorithm() != ""

	// user input checksum algorithm: either with chunk uploads or with
	// request headers
	var inputChAlgo utils.HashType
	// user input checksum value specified with request headers
	var inputSum string

	if !isTrailingChecksum {
		for _, config := range []hashConfig{
			{input.ChecksumCRC32, utils.HashTypeCRC32},
			{input.ChecksumCRC32C, utils.HashTypeCRC32C},
			{input.ChecksumSHA1, utils.HashTypeSha1},
			{input.ChecksumSHA256, utils.HashTypeSha256},
			{input.ChecksumCRC64NVME, utils.HashTypeCRC64NVME},
			{input.ChecksumSHA512, utils.HashTypeSha512},
			{input.ChecksumMD5, utils.HashTypeMd5},
			{input.ChecksumXXHASH64, utils.HashTypeXXHASH64},
			{input.ChecksumXXHASH3, utils.HashTypeXXHASH3},
			{input.ChecksumXXHASH128, utils.HashTypeXXHASH128},
		} {
			if config.value != nil {
				inputChAlgo = config.hashType
				inputSum = *config.value
				break
			}
		}
	} else {
		inputChAlgo = utils.HashType(chRdr.Algorithm())
	}

	exposeChecksum := inputChAlgo != ""

	checksums, err := l.retrieveChecksums(nil, bucket, mpPath)
	if err != nil && !errors.Is(err, meta.ErrNoSuchKey) {
		return nil, fmt.Errorf("retrieve mp checksum: %w", err)
	}

	// If checksum isn't provided for the part, but it has been provided on
	// mp initialization and checksum type is 'COMPOSITE', return mismatch
	// error
	if inputChAlgo == "" && checksums.Type == types.ChecksumTypeComposite {
		return nil, s3err.GetChecksumTypeMismatchErr(checksums.Algorithm, "null")
	}

	// Check if the provided checksum algorithm matches the one specified on
	// mp initialization
	if inputChAlgo != "" && checksums.Type != "" {
		algo := types.ChecksumAlgorithm(strings.ToUpper(string(inputChAlgo)))
		if checksums.Algorithm != algo {
			return nil, s3err.GetChecksumTypeMismatchErr(checksums.Algorithm, algo)
		}
	}

	if inputChAlgo == "" {
		// default to crc64nvme
		inputChAlgo = utils.HashTypeCRC64NVME
	}

	// hashRdr calculates and validates the user input checksum
	var hashRdr *utils.HashReader
	// crc64nvmeRdr calculates the part crc64nvme for internal use only
	var crc64nvmeRdr *utils.HashReader

	if checksums.Type == "" {
		if inputChAlgo != utils.HashTypeCRC64NVME {
			crc64nvmeRdr, err = utils.NewHashReader(tr, "", utils.HashTypeCRC64NVME)
			if err != nil {
				return nil, fmt.Errorf("initialize crc64nvme hash reader: %w", err)
			}
			tr = crc64nvmeRdr
		}

		if !isTrailingChecksum {
			hashRdr, err = utils.NewHashReader(tr, inputSum, inputChAlgo)
			if err != nil {
				return nil, fmt.Errorf("initialize hash reader: %w", err)
			}
			tr = hashRdr
		}
	} else if !isTrailingChecksum {
		chAlgo := utils.HashType(strings.ToLower(string(checksums.Algorithm)))
		hashRdr, err = utils.NewHashReader(tr, inputSum, chAlgo)
		if err != nil {
			return nil, fmt.Errorf("initialize hash reader: %w", err)
		}
		tr = hashRdr
	}

	if _, err := io.Copy(w, tr); err != nil {
		if errors.Is(err, syscall.EDQUOT) {
			drainBody(tr)
			return nil, s3err.GetAPIError(s3err.ErrQuotaExceeded)
		}
		if errors.Is(err, syscall.ENOSPC) {
			drainBody(tr)
			return nil, s3err.GetAPIError(s3err.ErrNoSpaceLeftOnDevice)
		}
		if _, ok := err.(s3err.S3Error); ok {
			return nil, err
		}
		return nil, fmt.Errorf("write part data: %w", err)
	}

	if err := w.Close(); err != nil {
		return nil, fmt.Errorf("write part data: %w", err)
	}

	// Publish the part before its attributes so that the attribute writes
	// have a file to attach to when the metadata storer keeps them on the
	// inode.
	if err := markPart(updir, part, length); err != nil {
		return nil, err
	}

	etag := backend.GenerateEtag(hash)
	if err := l.metastore.StoreAttribute(nil, bucket, partPath, etagkey, []byte(etag)); err != nil {
		return nil, fmt.Errorf("set etag attr: %w", err)
	}

	res := &s3.UploadPartOutput{ETag: &etag}

	// if a checksum algorithm has been provided on mp initiation the
	// checksums should be stored, otherwise only returned in the response
	// without storing
	if checksums.Type != "" {
		checksum := s3response.Checksum{Algorithm: checksums.Algorithm}

		var sum string
		if isTrailingChecksum {
			sum = chRdr.Checksum()
		}
		if hashRdr != nil {
			sum = hashRdr.Sum()
		}

		setStoredChecksum(&checksum, checksums.Algorithm, &sum)
		setUploadPartChecksum(res, checksums.Algorithm, &sum)

		if err := l.storeChecksums(nil, bucket, partPath, checksum); err != nil {
			return nil, fmt.Errorf("store checksum: %w", err)
		}

		return res, nil
	}

	var internalCrc64NvmeSum string
	if inputChAlgo == utils.HashTypeCRC64NVME {
		if isTrailingChecksum {
			internalCrc64NvmeSum = chRdr.Checksum()
		} else {
			internalCrc64NvmeSum = hashRdr.Sum()
		}
	} else {
		internalCrc64NvmeSum = crc64nvmeRdr.Sum()
	}

	err = l.metastore.StoreAttribute(nil, bucket, partPath, partCrc64nvme,
		[]byte(internalCrc64NvmeSum))
	if err != nil {
		return nil, fmt.Errorf("store part internal crc64nvme: %w", err)
	}

	if exposeChecksum {
		var sumToReturn string
		if isTrailingChecksum {
			sumToReturn = chRdr.Checksum()
		} else {
			sumToReturn = hashRdr.Sum()
		}

		setUploadPartChecksum(res,
			types.ChecksumAlgorithm(strings.ToUpper(string(inputChAlgo))), &sumToReturn)
	}

	return res, nil
}

// UploadPartCopy copies a byte range of an existing object into a part.
//
// The copy is delegated to the posix backend, which writes the payload as a
// standalone part file and works out the etag and checksums. That file is then
// turned into a spill so the layout stays the one this backend expects.
// Completion assembles such an upload by copy, exactly as posix would, since
// there is no request body to place in the staging file in the first place.
func (l *Lustre) UploadPartCopy(ctx context.Context, upi *s3.UploadPartCopyInput) (s3response.CopyPartResult, error) {
	if !l.directMultipart {
		return l.Posix.UploadPartCopy(ctx, upi)
	}

	res, err := l.Posix.UploadPartCopy(ctx, upi)
	if err != nil {
		return res, err
	}

	bucket := *upi.Bucket
	part := *upi.PartNumber
	mpPath := uploadDir(*upi.Key, *upi.UploadId)
	updir := filepath.Join(bucket, mpPath)
	partPath := filepath.Join(mpPath, fmt.Sprint(part))

	partFile := partMarker(updir, part)
	fi, err := os.Stat(partFile)
	if err != nil {
		return res, fmt.Errorf("stat copied part: %w", err)
	}

	// Read the attributes while they still belong to the payload file, so
	// they survive for storers that keep them on the inode.
	saved := make(map[string][]byte)
	for _, attr := range []string{etagkey, checksumsKey, partCrc64nvme} {
		v, err := l.metastore.RetrieveAttribute(nil, bucket, partPath, attr)
		if errors.Is(err, meta.ErrNoSuchKey) {
			continue
		}
		if err != nil {
			return res, fmt.Errorf("get copied part %v attr: %w", attr, err)
		}
		saved[attr] = v
	}

	if err := os.Rename(partFile, spillPath(updir, part)); err != nil {
		return res, fmt.Errorf("stage copied part: %w", err)
	}

	if err := markPart(updir, part, fi.Size()); err != nil {
		return res, err
	}

	for attr, v := range saved {
		if err := l.metastore.StoreAttribute(nil, bucket, partPath, attr, v); err != nil {
			return res, fmt.Errorf("set copied part %v attr: %w", attr, err)
		}
	}

	return res, nil
}

// completedPart is a part of a completing upload, resolved against what is
// actually on disk.
type completedPart struct {
	number int32
	size   int64
	// wantOffset is where the part must sit in the finished object.
	wantOffset int64
	// slotOffset is where its payload currently sits in the staging file.
	slotOffset int64
	spilled    bool
}

// CompleteMultipartUpload assembles the finished object. When every part
// landed on the offset it needs to occupy, which is the case whenever the
// uploader used one part size, the staging file already is the object and
// completion is a truncate and a rename. Otherwise the parts are copied into
// a new file, matching what the posix backend always does.
func (l *Lustre) CompleteMultipartUpload(ctx context.Context, input *s3.CompleteMultipartUploadInput) (s3response.CompleteMultipartUploadResult, string, error) {
	if !l.directMultipart {
		return l.Posix.CompleteMultipartUpload(ctx, input)
	}

	var res s3response.CompleteMultipartUploadResult

	release, err := l.acquireActionSlot(ctx)
	if err != nil {
		return res, "", err
	}
	defer release()

	acct, ok := ctx.Value("account").(auth.Account)
	if !ok {
		acct = auth.Account{}
	}

	if input.Key == nil {
		return res, "", s3err.GetAPIError(s3err.ErrNoSuchKey)
	}
	if input.UploadId == nil {
		return res, "", s3err.GetAPIError(s3err.ErrNoSuchUpload)
	}
	if input.MultipartUpload == nil {
		return res, "", s3err.GetAPIError(s3err.ErrMalformedXML)
	}

	bucket := *input.Bucket
	object := *input.Key
	uploadID := *input.UploadId
	parts := input.MultipartUpload.Parts

	if _, err := os.Stat(bucket); errors.Is(err, fs.ErrNotExist) {
		return res, "", s3err.GetBucketErr(s3err.ErrNoSuchBucket, bucket)
	} else if err != nil {
		return res, "", fmt.Errorf("stat bucket: %w", err)
	}

	// Returned as is: a malformed part list surfaces as the matching S3
	// error, and wrapping it would turn a 400 into a 500.
	s3MD5, err := backend.GetMultipartMD5(parts)
	if err != nil {
		return res, "", err
	}

	objRelDir := objdir(object)
	uploadRel := filepath.Join(objRelDir, uploadID)
	activeName := fmt.Sprintf("%s.%s%s", uploadID, strings.Trim(s3MD5, "\""), inProgressSuffix)
	activeRel := filepath.Join(objRelDir, activeName)

	// Renaming the upload directory is what serialises concurrent completes
	// of the same upload: only one of them can win the rename.
	err = os.Rename(filepath.Join(bucket, uploadRel), filepath.Join(bucket, activeRel))
	if errors.Is(err, fs.ErrNotExist) {
		return l.completeIdempotent(bucket, object, uploadID, activeRel, s3MD5)
	}
	if err != nil {
		return res, "", fmt.Errorf("mark upload in progress: %w", err)
	}

	if err := l.metastore.RenameObject(bucket, uploadRel, activeRel); err != nil {
		os.Rename(filepath.Join(bucket, activeRel), filepath.Join(bucket, uploadRel))
		return res, "", fmt.Errorf("rename upload metadata: %w", err)
	}

	// Put the upload back if anything below fails, so the client can retry.
	// Both are best effort and become no-ops once the upload is cleaned up
	// on the success path.
	completed := false
	defer func() {
		if completed {
			return
		}
		if err := os.Rename(filepath.Join(bucket, activeRel), filepath.Join(bucket, uploadRel)); err != nil {
			return
		}
		_ = l.metastore.RenameObject(bucket, activeRel, uploadRel)
	}()

	b, err := l.metastore.RetrieveAttribute(nil, bucket, object, etagkey)
	if err != nil && !errors.Is(err, meta.ErrNoSuchKey) {
		return res, "", fmt.Errorf("get object etag: %w", err)
	}
	objExists := err == nil
	if err := backend.EvaluateObjectPutPreconditions(string(b),
		input.IfMatch, input.IfNoneMatch, objExists); err != nil {
		return res, "", err
	}

	checksums, err := l.retrieveChecksums(nil, bucket, activeRel)
	if err != nil && !errors.Is(err, meta.ErrNoSuchKey) {
		return res, "", fmt.Errorf("get mp checksums: %w", err)
	}

	// ChecksumType should be the same as specified on CreateMultipartUpload
	if input.ChecksumType != "" && checksums.Type != input.ChecksumType {
		checksumType := checksums.Type
		if checksumType == "" {
			checksumType = types.ChecksumType("null")
		}
		return res, "", s3err.GetChecksumTypeMismatchOnMpErr(checksumType)
	}

	// mpChecksumType holds the multipart upload checksum type
	mpChecksumType := checksums.Type

	// The checksum type/algorithm defaults to FULL_OBJECT(crc64nvme)
	if checksums.Type == "" {
		checksums.Type = types.ChecksumTypeFullObject
		checksums.Algorithm = types.ChecksumAlgorithmCrc64nvme
	}

	var compositeChecksumRdr *utils.CompositeChecksumReader
	if checksums.Type == types.ChecksumTypeComposite {
		compositeChecksumRdr, err = utils.NewCompositeChecksumReader(
			utils.HashType(strings.ToLower(string(checksums.Algorithm))))
		if err != nil {
			return res, "", fmt.Errorf("initialize composite checksum reader: %w", err)
		}
	}

	slot, err := readSlotSize(filepath.Join(bucket, activeRel))
	if err != nil {
		return res, "", err
	}

	resolved, totalsize, partSizes, composableCsum, err := l.resolveParts(
		bucket, activeRel, uploadID, slot, parts, checksums, mpChecksumType,
		compositeChecksumRdr)
	if err != nil {
		return res, "", err
	}

	if input.MpuObjectSize != nil && totalsize != *input.MpuObjectSize {
		return res, "", s3err.GetIncorrectMpObjectSizeErr(totalsize, *input.MpuObjectSize)
	}

	// Compute the final checksum value.
	var value string
	switch checksums.Type {
	case types.ChecksumTypeComposite:
		value = fmt.Sprintf("%s-%v", compositeChecksumRdr.Sum(), len(parts))
	case types.ChecksumTypeFullObject:
		value = composableCsum
	}

	gotSum := completionChecksum(input, &checksums, &res, value)

	// Check if the provided checksum and the calculated one are the same.
	if mpChecksumType != "" && gotSum != nil {
		s := *gotSum
		if checksums.Type == types.ChecksumTypeComposite && !strings.Contains(s, "-") {
			s = fmt.Sprintf("%s-%v", s, len(parts))
		}
		if s != value {
			return res, "", s3err.GetChecksumBadDigestErr(checksums.Algorithm)
		}
	}

	assembledRel, err := l.assemble(bucket, activeRel, object, resolved, totalsize)
	if err != nil {
		return res, "", err
	}

	if err := l.finishObject(bucket, object, activeRel, assembledRel, acct,
		checksums, s3MD5, uploadID, partSizes); err != nil {
		return res, "", err
	}

	completed = true

	// Drop what is left of the upload. The staging file has either been
	// renamed away or was copied from, so only bookkeeping remains.
	_ = os.RemoveAll(filepath.Join(bucket, activeRel))
	_ = os.Remove(filepath.Join(bucket, objRelDir))
	_ = l.metastore.DeleteAttributes(bucket, activeRel)

	res.Bucket = &bucket
	res.Key = &object
	res.ETag = &s3MD5
	if mpChecksumType != "" {
		res.ChecksumType = &checksums.Type
	}

	return res, "", nil
}

// completeIdempotent decides what a complete request means when it lost the
// race for the upload directory, either to a concurrent request or to an
// earlier attempt by the same client that it never saw the response to.
func (l *Lustre) completeIdempotent(bucket, object, uploadID, activeRel, s3MD5 string) (s3response.CompleteMultipartUploadResult, string, error) {
	var none s3response.CompleteMultipartUploadResult

	// Every request completing this upload computes the same etag, so
	// reporting success without having done the work is still truthful.
	success := s3response.CompleteMultipartUploadResult{
		Bucket: &bucket,
		Key:    &object,
		ETag:   &s3MD5,
	}

	// Another request claimed the upload and is still assembling it.
	if _, err := os.Stat(filepath.Join(bucket, activeRel)); err == nil {
		return success, "", nil
	}

	// The upload is gone because a concurrent request already finished it.
	if _, err := os.Stat(filepath.Join(bucket, object)); err == nil {
		return success, "", nil
	}

	// That stat can lose a race with the winning request moving the object
	// into place, so fall back to the upload id recorded on the object.
	b, err := l.metastore.RetrieveAttribute(nil, bucket, object, mpMetaKey)
	if err != nil {
		return none, "", s3err.GetAPIError(s3err.ErrNoSuchUpload)
	}

	mpMeta, err := backend.UnmarshalMpUploadMetadata(b, false)
	if err != nil {
		return none, "", fmt.Errorf("parse object multipart metadata: %w", err)
	}

	// The object may be the result of a different upload that overwrote it.
	if mpMeta.UploadID != uploadID {
		return none, "", s3err.GetAPIError(s3err.ErrNoSuchUpload)
	}

	return success, "", nil
}

// resolveParts validates the requested parts against what is on disk and
// works out where each one currently lives.
func (l *Lustre) resolveParts(bucket, activeRel, uploadID string, slot int64,
	parts []types.CompletedPart, checksums s3response.Checksum,
	mpChecksumType types.ChecksumType, compositeChecksumRdr *utils.CompositeChecksumReader,
) (resolved []completedPart, totalsize int64, partSizes []int64, composableCsum string, err error) {
	last := len(parts) - 1

	// The initial value is the lower limit of partNumber: 0
	var partNumber int32
	for i, part := range parts {
		if part.PartNumber == nil {
			return nil, 0, nil, "", s3err.GetAPIError(s3err.ErrMalformedXML)
		}
		if *part.PartNumber < 1 {
			return nil, 0, nil, "", s3err.GetInvalidArgumentErr(
				s3err.InvalidArgCompleteMpPartNumber, fmt.Sprint(*part.PartNumber))
		}
		if *part.PartNumber <= partNumber {
			return nil, 0, nil, "", s3err.GetAPIError(s3err.ErrInvalidPartOrder)
		}
		partNumber = *part.PartNumber

		partObjPath := filepath.Join(activeRel, fmt.Sprint(partNumber))
		fi, err := os.Lstat(filepath.Join(bucket, partObjPath))
		if err != nil {
			return nil, 0, nil, "", s3err.GetInvalidPartErr(uploadID, partNumber,
				backend.GetStringFromPtr(part.ETag))
		}

		spilled := false
		if _, err := os.Lstat(spillPath(filepath.Join(bucket, activeRel), partNumber)); err == nil {
			spilled = true
		}

		resolved = append(resolved, completedPart{
			number:     partNumber,
			size:       fi.Size(),
			wantOffset: totalsize,
			slotOffset: slotOffset(slot, partNumber),
			spilled:    spilled,
		})

		totalsize += fi.Size()
		partSizes = append(partSizes, totalsize)

		// all parts except the last need to be greater than or equal to
		// the minimum allowed size (5 MiB)
		if i < last && fi.Size() < backend.MinPartSize {
			return nil, 0, nil, "", s3err.GetEntityTooSmallErr(fi.Size(), backend.MinPartSize)
		}

		b, err := l.metastore.RetrieveAttribute(nil, bucket, partObjPath, etagkey)
		etag := string(b)
		if err != nil {
			etag = ""
		}
		if part.ETag == nil {
			return nil, 0, nil, "", s3err.GetAPIError(s3err.ErrMalformedXML)
		}
		if !backend.AreEtagsSame(etag, *part.ETag) {
			return nil, 0, nil, "", s3err.GetInvalidPartErr(uploadID, partNumber, etag)
		}

		partChecksum, err := l.retrieveChecksums(nil, bucket, partObjPath)
		if err != nil && !errors.Is(err, meta.ErrNoSuchKey) {
			return nil, 0, nil, "", fmt.Errorf("get part checksum: %w", err)
		}

		if err := validatePartChecksum(partChecksum, part, uploadID); err != nil {
			return nil, 0, nil, "", err
		}

		switch checksums.Type {
		case types.ChecksumTypeFullObject:
			var pcs string
			if mpChecksumType != "" {
				pcs = getPartChecksum(checksums.Algorithm, part)
			} else {
				crc64nvme, err := l.metastore.RetrieveAttribute(nil, bucket, partObjPath, partCrc64nvme)
				if err != nil {
					return nil, 0, nil, "", fmt.Errorf("retrieve part internal crc64nvme: %w", err)
				}
				pcs = string(crc64nvme)
			}
			if i == 0 {
				composableCsum = pcs
			} else {
				composableCsum, err = utils.AddCRCChecksum(checksums.Algorithm,
					composableCsum, pcs, fi.Size())
				if err != nil {
					return nil, 0, nil, "", fmt.Errorf("add part %v checksum: %w", partNumber, err)
				}
			}
		case types.ChecksumTypeComposite:
			if err := compositeChecksumRdr.Process(getPartChecksum(checksums.Algorithm, part)); err != nil {
				return nil, 0, nil, "", fmt.Errorf("process %v part checksum: %w", partNumber, err)
			}
		}
	}

	return resolved, totalsize, partSizes, composableCsum, nil
}

// isContiguous reports whether every part already sits at the offset it needs
// to occupy in the finished object, which makes the staging file the object.
func isContiguous(parts []completedPart) bool {
	for _, p := range parts {
		if p.spilled || p.slotOffset != p.wantOffset {
			return false
		}
	}
	return true
}

// assemble produces the finished object contents and returns its bucket
// relative path.
func (l *Lustre) assemble(bucket, activeRel, object string, parts []completedPart, totalsize int64) (string, error) {
	staging := stagingPath(activeRel)

	if isContiguous(parts) {
		// The staging file already holds the object, byte for byte. Cut it
		// back to the requested parts and it is done.
		if err := os.Truncate(filepath.Join(bucket, staging), totalsize); err != nil {
			return "", fmt.Errorf("truncate staging file: %w", err)
		}
		return staging, nil
	}

	debuglogger.Logf("lustre: parts of %v/%v are not laid out contiguously, assembling by copy",
		bucket, object)

	return l.assembleByCopy(bucket, activeRel, object, parts)
}

// assembleByCopy builds the object by copying each part out of the staging
// file or its spill file. This is the fallback for uploads whose parts did not
// arrive in uniform sizes.
func (l *Lustre) assembleByCopy(bucket, activeRel, object string, parts []completedPart) (string, error) {
	tmpdir := filepath.Join(bucket, metaTmpDir)
	if err := backend.MkdirAll(tmpdir, 0, 0, false, l.newDirPerm); err != nil {
		return "", fmt.Errorf("create temp dir: %w", err)
	}

	dst, err := os.CreateTemp(tmpdir, objHash(object)+".")
	if err != nil {
		return "", fmt.Errorf("create assembly file: %w", err)
	}
	defer dst.Close()

	assembledRel, err := filepath.Rel(bucket, dst.Name())
	if err != nil {
		os.Remove(dst.Name())
		return "", fmt.Errorf("resolve assembly file: %w", err)
	}

	staging, err := os.Open(stagingPath(filepath.Join(bucket, activeRel)))
	if err != nil {
		os.Remove(dst.Name())
		return "", fmt.Errorf("open staging file: %w", err)
	}
	defer staging.Close()

	for _, p := range parts {
		var src io.Reader
		if p.spilled {
			sf, err := os.Open(spillPath(filepath.Join(bucket, activeRel), p.number))
			if err != nil {
				os.Remove(dst.Name())
				return "", fmt.Errorf("open spill file: %w", err)
			}
			src = io.LimitReader(sf, p.size)
			_, err = io.Copy(dst, src)
			sf.Close()
			if err != nil {
				os.Remove(dst.Name())
				return "", fmt.Errorf("copy part %v: %w", p.number, err)
			}
			continue
		}

		src = io.NewSectionReader(staging, p.slotOffset, p.size)
		if _, err := io.Copy(dst, src); err != nil {
			os.Remove(dst.Name())
			return "", fmt.Errorf("copy part %v: %w", p.number, err)
		}
	}

	if err := dst.Close(); err != nil {
		os.Remove(dst.Name())
		return "", fmt.Errorf("close assembly file: %w", err)
	}

	return assembledRel, nil
}

// finishObject copies the upload attributes onto the assembled file, moves it
// into the namespace and moves its metadata with it.
func (l *Lustre) finishObject(bucket, object, activeRel, assembledRel string,
	acct auth.Account, checksums s3response.Checksum, s3MD5, uploadID string,
	partSizes []int64,
) error {
	// Attributes are written against the assembled file and moved onto the
	// object afterwards. That keeps storers that live on the inode and
	// storers keyed by path both correct, and it means the object never
	// appears without them.
	for _, attr := range []string{
		metadataHdr, contentTypeHdr, contentEncHdr, contentDispHdr,
		contentLangHdr, cacheCtrlHdr, expiresHdr, websiteRedirectHdr,
		tagHdr, objectLegalHoldKey, objectRetentionKey,
	} {
		v, err := l.metastore.RetrieveAttribute(nil, bucket, activeRel, attr)
		if errors.Is(err, meta.ErrNoSuchKey) {
			continue
		}
		if err != nil {
			return fmt.Errorf("get upload %v attr: %w", attr, err)
		}
		if err := l.metastore.StoreAttribute(nil, bucket, assembledRel, attr, v); err != nil {
			return fmt.Errorf("set object %v attr: %w", attr, err)
		}
	}

	if err := l.storeChecksums(nil, bucket, assembledRel, checksums); err != nil {
		return fmt.Errorf("store checksums: %w", err)
	}

	if err := l.metastore.StoreAttribute(nil, bucket, assembledRel, etagkey, []byte(s3MD5)); err != nil {
		return fmt.Errorf("set etag attr: %w", err)
	}

	mpMetaBytes, err := backend.MarshalMpUploadMetadata(backend.MpUploadMetadata{
		UploadID: uploadID,
		Parts:    partSizes,
	}, false)
	if err != nil {
		return fmt.Errorf("marshal multipart metadata: %w", err)
	}
	if err := l.metastore.StoreAttribute(nil, bucket, assembledRel, mpMetaKey, mpMetaBytes); err != nil {
		return fmt.Errorf("set multipart metadata attr: %w", err)
	}

	objPath := filepath.Join(bucket, object)
	uid, gid, doChown := l.getChownIDs(acct)
	if err := backend.MkdirAll(filepath.Dir(objPath), uid, gid, doChown, l.newDirPerm); err != nil {
		return fmt.Errorf("create object parent dir: %w", err)
	}

	src := filepath.Join(bucket, assembledRel)
	if err := os.Chmod(src, 0644); err != nil {
		return fmt.Errorf("set object permissions: %w", err)
	}
	if doChown {
		if err := os.Chown(src, uid, gid); err != nil {
			return fmt.Errorf("chown object: %w", err)
		}
	}

	if err := os.Rename(src, objPath); err != nil {
		return fmt.Errorf("move object into namespace: %w", err)
	}

	if err := l.metastore.RenameObject(bucket, assembledRel, object); err != nil {
		return fmt.Errorf("move object metadata: %w", err)
	}

	return nil
}

// completionChecksum records the computed value on both the stored checksums
// and the response, and returns the value the client claimed, if any.
func completionChecksum(input *s3.CompleteMultipartUploadInput,
	checksums *s3response.Checksum, res *s3response.CompleteMultipartUploadResult,
	value string,
) *string {
	var gotSum *string

	switch checksums.Algorithm {
	case types.ChecksumAlgorithmCrc32:
		gotSum = input.ChecksumCRC32
		checksums.CRC32 = &value
		res.ChecksumCRC32 = &value
	case types.ChecksumAlgorithmCrc32c:
		gotSum = input.ChecksumCRC32C
		checksums.CRC32C = &value
		res.ChecksumCRC32C = &value
	case types.ChecksumAlgorithmSha1:
		gotSum = input.ChecksumSHA1
		checksums.SHA1 = &value
		res.ChecksumSHA1 = &value
	case types.ChecksumAlgorithmSha256:
		gotSum = input.ChecksumSHA256
		checksums.SHA256 = &value
		res.ChecksumSHA256 = &value
	case types.ChecksumAlgorithmCrc64nvme:
		gotSum = input.ChecksumCRC64NVME
		checksums.CRC64NVME = &value
		res.ChecksumCRC64NVME = &value
	case types.ChecksumAlgorithmSha512:
		gotSum = input.ChecksumSHA512
		checksums.SHA512 = &value
		res.ChecksumSHA512 = &value
	case types.ChecksumAlgorithmMd5:
		gotSum = input.ChecksumMD5
		checksums.MD5 = &value
		res.ChecksumMD5 = &value
	case types.ChecksumAlgorithmXxhash64:
		gotSum = input.ChecksumXXHASH64
		checksums.XXHASH64 = &value
		res.ChecksumXXHASH64 = &value
	case types.ChecksumAlgorithmXxhash3:
		gotSum = input.ChecksumXXHASH3
		checksums.XXHASH3 = &value
		res.ChecksumXXHASH3 = &value
	case types.ChecksumAlgorithmXxhash128:
		gotSum = input.ChecksumXXHASH128
		checksums.XXHASH128 = &value
		res.ChecksumXXHASH128 = &value
	}

	return gotSum
}

// drainBody consumes and discards all remaining bytes from r so that the
// client can read the error response before the connection is shut down.
func drainBody(r io.Reader) {
	if r == nil {
		return
	}
	_, _ = io.Copy(io.Discard, r)
}
