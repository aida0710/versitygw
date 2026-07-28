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

//go:build cgo

package meta

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
)

func newTestSQLite(t *testing.T) *SQLiteMeta {
	t.Helper()

	s, err := NewSQLite(t.TempDir())
	if err != nil {
		t.Fatalf("NewSQLite: %v", err)
	}
	t.Cleanup(func() { s.Close() })

	return s
}

func mustRetrieve(t *testing.T, s *SQLiteMeta, bucket, object, attr string) []byte {
	t.Helper()

	v, err := s.RetrieveAttribute(nil, bucket, object, attr)
	if err != nil {
		t.Fatalf("RetrieveAttribute(%q, %q, %q): %v", bucket, object, attr, err)
	}
	return v
}

func assertNoSuchKey(t *testing.T, s *SQLiteMeta, bucket, object, attr string) {
	t.Helper()

	_, err := s.RetrieveAttribute(nil, bucket, object, attr)
	if !errors.Is(err, ErrNoSuchKey) {
		t.Fatalf("RetrieveAttribute(%q, %q, %q) = %v, want ErrNoSuchKey",
			bucket, object, attr, err)
	}
}

func TestSQLiteInterface(t *testing.T) {
	var _ MetadataStorer = &SQLiteMeta{}
}

func TestSQLiteRoundTrip(t *testing.T) {
	s := newTestSQLite(t)

	tests := []struct {
		name   string
		bucket string
		object string
		attr   string
		value  []byte
	}{
		{"bucket level", "mybucket", "", "acl", []byte(`{"Owner":"me"}`)},
		{"object level", "mybucket", "a/b/c", "etag", []byte(`"abc123"`)},
		{"empty value", "mybucket", "obj", "delete-marker", []byte{}},
		{"binary value", "mybucket", "obj", "mp-metadata", []byte{0x1f, 0x8b, 0x00, 0xff}},
		{"path bucket", "/versions/mybucket/key", "null", "etag", []byte(`"v1"`)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := s.StoreAttribute(nil, tt.bucket, tt.object, tt.attr, tt.value)
			if err != nil {
				t.Fatalf("StoreAttribute: %v", err)
			}

			got := mustRetrieve(t, s, tt.bucket, tt.object, tt.attr)
			if string(got) != string(tt.value) {
				t.Errorf("got %q, want %q", got, tt.value)
			}
		})
	}
}

func TestSQLiteOverwrite(t *testing.T) {
	s := newTestSQLite(t)

	if err := s.StoreAttribute(nil, "b", "o", "etag", []byte("first")); err != nil {
		t.Fatalf("StoreAttribute: %v", err)
	}
	if err := s.StoreAttribute(nil, "b", "o", "etag", []byte("second")); err != nil {
		t.Fatalf("StoreAttribute: %v", err)
	}

	if got := mustRetrieve(t, s, "b", "o", "etag"); string(got) != "second" {
		t.Errorf("got %q, want %q", got, "second")
	}
}

func TestSQLiteMissing(t *testing.T) {
	s := newTestSQLite(t)

	// No database file exists at all yet.
	assertNoSuchKey(t, s, "nobucket", "noobject", "etag")

	if err := s.StoreAttribute(nil, "b", "o", "etag", []byte("x")); err != nil {
		t.Fatalf("StoreAttribute: %v", err)
	}

	// Database exists, but neither the key nor the attribute do.
	assertNoSuchKey(t, s, "b", "o", "missing-attr")
	assertNoSuchKey(t, s, "b", "missing-object", "etag")
}

func TestSQLiteDeleteAttribute(t *testing.T) {
	s := newTestSQLite(t)

	if err := s.StoreAttribute(nil, "b", "o", "etag", []byte("x")); err != nil {
		t.Fatalf("StoreAttribute: %v", err)
	}

	if err := s.DeleteAttribute("b", "o", "etag"); err != nil {
		t.Fatalf("DeleteAttribute: %v", err)
	}
	assertNoSuchKey(t, s, "b", "o", "etag")

	// Deleting again reports the attribute as missing, matching the xattr
	// and sidecar storers.
	if err := s.DeleteAttribute("b", "o", "etag"); !errors.Is(err, ErrNoSuchKey) {
		t.Errorf("DeleteAttribute on missing attr = %v, want ErrNoSuchKey", err)
	}
	if err := s.DeleteAttribute("nobucket", "o", "etag"); !errors.Is(err, ErrNoSuchKey) {
		t.Errorf("DeleteAttribute on missing bucket = %v, want ErrNoSuchKey", err)
	}
}

func TestSQLiteListAttributes(t *testing.T) {
	s := newTestSQLite(t)

	for _, attr := range []string{"etag", "content-type", "checksums"} {
		if err := s.StoreAttribute(nil, "b", "o", attr, []byte("x")); err != nil {
			t.Fatalf("StoreAttribute: %v", err)
		}
	}
	// A nested key must not leak into the parent's attribute list.
	if err := s.StoreAttribute(nil, "b", "o/child", "etag", []byte("x")); err != nil {
		t.Fatalf("StoreAttribute: %v", err)
	}
	// Neither may a bucket level attribute.
	if err := s.StoreAttribute(nil, "b", "", "acl", []byte("x")); err != nil {
		t.Fatalf("StoreAttribute: %v", err)
	}

	got, err := s.ListAttributes("b", "o")
	if err != nil {
		t.Fatalf("ListAttributes: %v", err)
	}
	sort.Strings(got)

	want := []string{"checksums", "content-type", "etag"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("got %v, want %v", got, want)
	}

	// Listing a container with no attributes yields an empty list, not an
	// error.
	empty, err := s.ListAttributes("nobucket", "noobject")
	if err != nil {
		t.Fatalf("ListAttributes on missing bucket: %v", err)
	}
	if len(empty) != 0 {
		t.Errorf("got %v, want empty", empty)
	}
}

func TestSQLiteDeleteAttributesRecursive(t *testing.T) {
	s := newTestSQLite(t)

	// The posix backend stores multipart part attributes underneath the
	// upload directory and drops the whole subtree in one call.
	upload := ".sgwtmp/multipart/deadbeef/upload-1"
	for _, key := range []string{upload, upload + "/1", upload + "/2"} {
		if err := s.StoreAttribute(nil, "b", key, "etag", []byte("x")); err != nil {
			t.Fatalf("StoreAttribute: %v", err)
		}
	}
	// A sibling upload must survive.
	sibling := ".sgwtmp/multipart/deadbeef/upload-2"
	if err := s.StoreAttribute(nil, "b", sibling+"/1", "etag", []byte("keep")); err != nil {
		t.Fatalf("StoreAttribute: %v", err)
	}

	if err := s.DeleteAttributes("b", upload); err != nil {
		t.Fatalf("DeleteAttributes: %v", err)
	}

	for _, key := range []string{upload, upload + "/1", upload + "/2"} {
		assertNoSuchKey(t, s, "b", key, "etag")
	}
	if got := mustRetrieve(t, s, "b", sibling+"/1", "etag"); string(got) != "keep" {
		t.Errorf("sibling upload was removed: got %q", got)
	}
}

func TestSQLiteDeleteAttributesDropsBucket(t *testing.T) {
	s := newTestSQLite(t)

	if err := s.StoreAttribute(nil, "b", "", "acl", []byte("x")); err != nil {
		t.Fatalf("StoreAttribute: %v", err)
	}
	if err := s.StoreAttribute(nil, "b", "some/object", "etag", []byte("x")); err != nil {
		t.Fatalf("StoreAttribute: %v", err)
	}
	if err := s.StoreAttribute(nil, "other", "obj", "etag", []byte("keep")); err != nil {
		t.Fatalf("StoreAttribute: %v", err)
	}

	dbpath := s.dbPath("b")
	if _, err := os.Stat(dbpath); err != nil {
		t.Fatalf("stat bucket db: %v", err)
	}

	if err := s.DeleteAttributes("b", ""); err != nil {
		t.Fatalf("DeleteAttributes: %v", err)
	}

	if _, err := os.Stat(dbpath); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("bucket db still present: %v", err)
	}
	assertNoSuchKey(t, s, "b", "", "acl")
	assertNoSuchKey(t, s, "b", "some/object", "etag")

	if got := mustRetrieve(t, s, "other", "obj", "etag"); string(got) != "keep" {
		t.Errorf("unrelated bucket was affected: got %q", got)
	}

	// The bucket must be usable again after being dropped.
	if err := s.StoreAttribute(nil, "b", "", "acl", []byte("new")); err != nil {
		t.Fatalf("StoreAttribute after drop: %v", err)
	}
	if got := mustRetrieve(t, s, "b", "", "acl"); string(got) != "new" {
		t.Errorf("got %q, want %q", got, "new")
	}
}

func TestSQLiteDeleteAttributesMissing(t *testing.T) {
	s := newTestSQLite(t)

	// Removing attributes that were never stored is not an error.
	if err := s.DeleteAttributes("nobucket", "noobject"); err != nil {
		t.Errorf("DeleteAttributes on missing object: %v", err)
	}
	if err := s.DeleteAttributes("nobucket", ""); err != nil {
		t.Errorf("DeleteAttributes on missing bucket: %v", err)
	}
}

func TestSQLiteRenameObject(t *testing.T) {
	s := newTestSQLite(t)

	const (
		objdir = ".sgwtmp/multipart/deadbeef"
		oldObj = objdir + "/upload-1"
		newObj = objdir + "/upload-1.md5-2.inprogress"
	)

	if err := s.StoreAttribute(nil, "b", oldObj, "checksums", []byte("cs")); err != nil {
		t.Fatalf("StoreAttribute: %v", err)
	}
	if err := s.StoreAttribute(nil, "b", oldObj+"/1", "etag", []byte("p1")); err != nil {
		t.Fatalf("StoreAttribute: %v", err)
	}
	if err := s.StoreAttribute(nil, "b", oldObj+"/2", "etag", []byte("p2")); err != nil {
		t.Fatalf("StoreAttribute: %v", err)
	}

	if err := s.RenameObject("b", oldObj, newObj); err != nil {
		t.Fatalf("RenameObject: %v", err)
	}

	if got := mustRetrieve(t, s, "b", newObj, "checksums"); string(got) != "cs" {
		t.Errorf("got %q, want %q", got, "cs")
	}
	if got := mustRetrieve(t, s, "b", newObj+"/1", "etag"); string(got) != "p1" {
		t.Errorf("got %q, want %q", got, "p1")
	}
	if got := mustRetrieve(t, s, "b", newObj+"/2", "etag"); string(got) != "p2" {
		t.Errorf("got %q, want %q", got, "p2")
	}
	assertNoSuchKey(t, s, "b", oldObj, "checksums")
	assertNoSuchKey(t, s, "b", oldObj+"/1", "etag")

	// Renaming back restores the original layout, which is what the posix
	// backend does when completing a multipart upload fails.
	if err := s.RenameObject("b", newObj, oldObj); err != nil {
		t.Fatalf("RenameObject back: %v", err)
	}
	if got := mustRetrieve(t, s, "b", oldObj+"/1", "etag"); string(got) != "p1" {
		t.Errorf("got %q, want %q", got, "p1")
	}
}

func TestSQLiteRenameObjectMultibyte(t *testing.T) {
	s := newTestSQLite(t)

	// substr() and length() in SQLite count characters, so a multi-byte
	// prefix must not be trimmed by its byte length.
	const oldObj = "日本語/ディレクトリ"
	const newObj = "renamed"

	if err := s.StoreAttribute(nil, "b", oldObj+"/child", "etag", []byte("p1")); err != nil {
		t.Fatalf("StoreAttribute: %v", err)
	}

	if err := s.RenameObject("b", oldObj, newObj); err != nil {
		t.Fatalf("RenameObject: %v", err)
	}

	if got := mustRetrieve(t, s, "b", newObj+"/child", "etag"); string(got) != "p1" {
		t.Errorf("got %q, want %q", got, "p1")
	}
}

func TestSQLiteRenameObjectMissing(t *testing.T) {
	s := newTestSQLite(t)

	if err := s.RenameObject("nobucket", "old", "new"); err != nil {
		t.Errorf("RenameObject with no stored metadata: %v", err)
	}
}

// TestSQLiteLikeWildcards checks that a key containing LIKE wildcards is not
// treated as a pattern by the recursive delete and rename.
func TestSQLiteLikeWildcards(t *testing.T) {
	s := newTestSQLite(t)

	if err := s.StoreAttribute(nil, "b", "a%c", "etag", []byte("wild")); err != nil {
		t.Fatalf("StoreAttribute: %v", err)
	}
	if err := s.StoreAttribute(nil, "b", "abc/x", "etag", []byte("keep")); err != nil {
		t.Fatalf("StoreAttribute: %v", err)
	}
	if err := s.StoreAttribute(nil, "b", "a_c/y", "etag", []byte("keep2")); err != nil {
		t.Fatalf("StoreAttribute: %v", err)
	}

	if err := s.DeleteAttributes("b", "a%c"); err != nil {
		t.Fatalf("DeleteAttributes: %v", err)
	}

	assertNoSuchKey(t, s, "b", "a%c", "etag")
	if got := mustRetrieve(t, s, "b", "abc/x", "etag"); string(got) != "keep" {
		t.Errorf("wildcard delete removed abc/x")
	}
	if got := mustRetrieve(t, s, "b", "a_c/y", "etag"); string(got) != "keep2" {
		t.Errorf("wildcard delete removed a_c/y")
	}
}

// TestSQLitePathBucketIsolation verifies that callers addressing a container
// by path, such as the versioning directory, land in the shared database and
// do not create a database file per object.
func TestSQLitePathBucketIsolation(t *testing.T) {
	s := newTestSQLite(t)

	versionPaths := []string{
		"/versions/bucket/objectA",
		"/versions/bucket/objectB",
		"/versions/bucket/deep/objectC",
	}
	for _, vp := range versionPaths {
		if err := s.StoreAttribute(nil, vp, "01JABC", "etag", []byte(vp)); err != nil {
			t.Fatalf("StoreAttribute: %v", err)
		}
	}

	for _, vp := range versionPaths {
		if got := mustRetrieve(t, s, vp, "01JABC", "etag"); string(got) != vp {
			t.Errorf("got %q, want %q", got, vp)
		}
	}

	ents, err := os.ReadDir(s.dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	var dbs []string
	for _, e := range ents {
		if filepath.Ext(e.Name()) == dbSuffix {
			dbs = append(dbs, e.Name())
		}
	}
	if len(dbs) != 1 || dbs[0] != sharedDB+dbSuffix {
		t.Errorf("got db files %v, want only %v", dbs, sharedDB+dbSuffix)
	}

	// Dropping one version path must not disturb the others.
	if err := s.DeleteAttributes(versionPaths[0], ""); err != nil {
		t.Fatalf("DeleteAttributes: %v", err)
	}
	assertNoSuchKey(t, s, versionPaths[0], "01JABC", "etag")
	if got := mustRetrieve(t, s, versionPaths[1], "01JABC", "etag"); string(got) != versionPaths[1] {
		t.Errorf("unrelated version path was affected")
	}
}

func TestSQLiteConcurrentAccess(t *testing.T) {
	s := newTestSQLite(t)

	const (
		workers = 16
		perWork = 25
	)

	var wg sync.WaitGroup
	errs := make(chan error, workers*perWork)

	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range perWork {
				object := fmt.Sprintf("obj-%d-%d", w, i)
				value := []byte(object)
				if err := s.StoreAttribute(nil, "b", object, "etag", value); err != nil {
					errs <- fmt.Errorf("store %s: %w", object, err)
					continue
				}
				got, err := s.RetrieveAttribute(nil, "b", object, "etag")
				if err != nil {
					errs <- fmt.Errorf("retrieve %s: %w", object, err)
					continue
				}
				if string(got) != object {
					errs <- fmt.Errorf("retrieve %s: got %q", object, got)
				}
			}
		}()
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		t.Error(err)
	}
}

func TestSQLiteNewSQLiteErrors(t *testing.T) {
	if _, err := NewSQLite(filepath.Join(t.TempDir(), "missing")); err == nil {
		t.Error("NewSQLite on a missing directory should fail")
	}

	file := filepath.Join(t.TempDir(), "afile")
	if err := os.WriteFile(file, nil, 0600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if _, err := NewSQLite(file); err == nil {
		t.Error("NewSQLite on a regular file should fail")
	}
}

// TestSQLiteReadDoesNotCreateDB guards against read paths littering the
// metadata directory with empty databases for buckets that do not exist.
func TestSQLiteReadDoesNotCreateDB(t *testing.T) {
	s := newTestSQLite(t)

	assertNoSuchKey(t, s, "nobucket", "obj", "etag")
	if _, err := s.ListAttributes("nobucket", "obj"); err != nil {
		t.Fatalf("ListAttributes: %v", err)
	}

	ents, err := os.ReadDir(s.dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(ents) != 0 {
		t.Errorf("read path created %d files in the metadata dir", len(ents))
	}
}

func TestSQLiteReopen(t *testing.T) {
	dir := t.TempDir()

	s1, err := NewSQLite(dir)
	if err != nil {
		t.Fatalf("NewSQLite: %v", err)
	}
	if err := s1.StoreAttribute(nil, "b", "o", "etag", []byte("persisted")); err != nil {
		t.Fatalf("StoreAttribute: %v", err)
	}
	if err := s1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	s2, err := NewSQLite(dir)
	if err != nil {
		t.Fatalf("NewSQLite reopen: %v", err)
	}
	defer s2.Close()

	if got := mustRetrieve(t, s2, "b", "o", "etag"); string(got) != "persisted" {
		t.Errorf("got %q, want %q", got, "persisted")
	}
}
