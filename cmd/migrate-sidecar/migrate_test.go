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

package main

import (
	"reflect"
	"testing"

	"github.com/versity/versitygw/backend/meta"
)

func newSideCar(t *testing.T) (meta.SideCar, string) {
	t.Helper()
	dir := t.TempDir()
	sc, err := meta.NewSideCar(dir)
	if err != nil {
		t.Fatalf("NewSideCar: %v", err)
	}
	return sc, dir
}

func newSQLiteMeta(t *testing.T) *meta.SQLiteMeta {
	t.Helper()
	ms, err := meta.NewSQLite(t.TempDir())
	if err != nil {
		t.Fatalf("NewSQLite: %v", err)
	}
	t.Cleanup(func() { ms.Close() })
	return ms
}

func TestDiscoverEntriesFindsObjectsAndBuckets(t *testing.T) {
	sc, dir := newSideCar(t)

	if err := sc.StoreAttribute(nil, "bucket1", "obj/a", "etag", []byte("etag-a")); err != nil {
		t.Fatalf("store: %v", err)
	}
	if err := sc.StoreAttribute(nil, "bucket1", "obj/b", "etag", []byte("etag-b")); err != nil {
		t.Fatalf("store: %v", err)
	}
	// バケットレベルの属性(object == "")
	if err := sc.StoreAttribute(nil, "bucket1", "", "acl", []byte("bucket-acl")); err != nil {
		t.Fatalf("store: %v", err)
	}
	if err := sc.StoreAttribute(nil, "bucket2", "solo", "etag", []byte("etag-solo")); err != nil {
		t.Fatalf("store: %v", err)
	}

	entries, err := discoverEntries(dir)
	if err != nil {
		t.Fatalf("discoverEntries: %v", err)
	}

	want := []entry{
		{bucket: "bucket1", object: ""},
		{bucket: "bucket1", object: "obj/a"},
		{bucket: "bucket1", object: "obj/b"},
		{bucket: "bucket2", object: "solo"},
	}
	if !reflect.DeepEqual(entries, want) {
		t.Fatalf("entries = %+v, want %+v", entries, want)
	}
}

func TestDiscoverEntriesSkipsMultipartStaging(t *testing.T) {
	sc, dir := newSideCar(t)

	if err := sc.StoreAttribute(nil, "bucket1", "finished-object", "etag", []byte("v")); err != nil {
		t.Fatalf("store: %v", err)
	}
	// 進行中のマルチパートアップロードのメタデータを模擬する。posix は
	// これを <bucket>/.sgwtmp/multipart/<hash>/<uploadid>/ 配下に置く。
	if err := sc.StoreAttribute(nil, "bucket1", ".sgwtmp/multipart/abc123/upload-1", "part-1", []byte("junk")); err != nil {
		t.Fatalf("store: %v", err)
	}

	entries, err := discoverEntries(dir)
	if err != nil {
		t.Fatalf("discoverEntries: %v", err)
	}

	want := []entry{{bucket: "bucket1", object: "finished-object"}}
	if !reflect.DeepEqual(entries, want) {
		t.Fatalf("entries = %+v, want %+v (staging entry should be excluded)", entries, want)
	}
}

func TestDiscoverEntriesEmptySidecar(t *testing.T) {
	_, dir := newSideCar(t)

	entries, err := discoverEntries(dir)
	if err != nil {
		t.Fatalf("discoverEntries: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("entries = %+v, want empty", entries)
	}
}

func TestMigrateEntryCopiesAllAttributes(t *testing.T) {
	sc, _ := newSideCar(t)
	sq := newSQLiteMeta(t)

	if err := sc.StoreAttribute(nil, "bucket1", "obj", "etag", []byte("etagvalue")); err != nil {
		t.Fatalf("store: %v", err)
	}
	if err := sc.StoreAttribute(nil, "bucket1", "obj", "content-type", []byte("text/plain")); err != nil {
		t.Fatalf("store: %v", err)
	}

	n, err := migrateEntry(sc, sq, entry{bucket: "bucket1", object: "obj"})
	if err != nil {
		t.Fatalf("migrateEntry: %v", err)
	}
	if n != 2 {
		t.Fatalf("copied %d attributes, want 2", n)
	}

	got, err := sq.RetrieveAttribute(nil, "bucket1", "obj", "etag")
	if err != nil {
		t.Fatalf("retrieve etag: %v", err)
	}
	if string(got) != "etagvalue" {
		t.Fatalf("etag = %q, want %q", got, "etagvalue")
	}

	got, err = sq.RetrieveAttribute(nil, "bucket1", "obj", "content-type")
	if err != nil {
		t.Fatalf("retrieve content-type: %v", err)
	}
	if string(got) != "text/plain" {
		t.Fatalf("content-type = %q, want %q", got, "text/plain")
	}
}

func TestMigrateEntryHandlesBucketLevelAttributes(t *testing.T) {
	sc, _ := newSideCar(t)
	sq := newSQLiteMeta(t)

	if err := sc.StoreAttribute(nil, "bucket1", "", "versioning", []byte("Enabled")); err != nil {
		t.Fatalf("store: %v", err)
	}

	n, err := migrateEntry(sc, sq, entry{bucket: "bucket1", object: ""})
	if err != nil {
		t.Fatalf("migrateEntry: %v", err)
	}
	if n != 1 {
		t.Fatalf("copied %d attributes, want 1", n)
	}

	got, err := sq.RetrieveAttribute(nil, "bucket1", "", "versioning")
	if err != nil {
		t.Fatalf("retrieve: %v", err)
	}
	if string(got) != "Enabled" {
		t.Fatalf("versioning = %q, want %q", got, "Enabled")
	}
}

func TestMigrateEndToEnd(t *testing.T) {
	sc, dir := newSideCar(t)
	sq := newSQLiteMeta(t)

	fixtures := []struct {
		bucket, object, attr, value string
	}{
		{"bucket1", "", "acl", "bucket1-acl"},
		{"bucket1", "a.txt", "etag", "etag-a"},
		{"bucket1", "dir/b.txt", "etag", "etag-b"},
		{"bucket2", "c.bin", "etag", "etag-c"},
		{"bucket2", "c.bin", "content-type", "application/octet-stream"},
	}
	for _, f := range fixtures {
		if err := sc.StoreAttribute(nil, f.bucket, f.object, f.attr, []byte(f.value)); err != nil {
			t.Fatalf("seed store %+v: %v", f, err)
		}
	}

	res, err := migrate(sc, sq, dir, false, nil)
	if err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if res.entries != 4 {
		t.Errorf("res.entries = %d, want 4", res.entries)
	}
	if res.attributes != 5 {
		t.Errorf("res.attributes = %d, want 5", res.attributes)
	}

	for _, f := range fixtures {
		got, err := sq.RetrieveAttribute(nil, f.bucket, f.object, f.attr)
		if err != nil {
			t.Fatalf("retrieve %+v: %v", f, err)
		}
		if string(got) != f.value {
			t.Errorf("%+v: got %q, want %q", f, got, f.value)
		}
	}
}

func TestMigrateDryRunWritesNothing(t *testing.T) {
	sc, dir := newSideCar(t)
	sq := newSQLiteMeta(t)

	if err := sc.StoreAttribute(nil, "bucket1", "obj", "etag", []byte("v")); err != nil {
		t.Fatalf("store: %v", err)
	}

	res, err := migrate(sc, sq, dir, true, nil)
	if err != nil {
		t.Fatalf("migrate dry-run: %v", err)
	}
	if res.entries != 1 || res.attributes != 1 {
		t.Fatalf("dry-run counts = %+v, want entries=1 attributes=1", res)
	}

	if _, err := sq.RetrieveAttribute(nil, "bucket1", "obj", "etag"); err != meta.ErrNoSuchKey {
		t.Fatalf("dry-run should not have written anything, got err=%v", err)
	}
}

func TestMigrateIsIdempotent(t *testing.T) {
	sc, dir := newSideCar(t)
	sq := newSQLiteMeta(t)

	if err := sc.StoreAttribute(nil, "bucket1", "obj", "etag", []byte("v1")); err != nil {
		t.Fatalf("store: %v", err)
	}

	if _, err := migrate(sc, sq, dir, false, nil); err != nil {
		t.Fatalf("first migrate: %v", err)
	}
	// ソース側の値が変わった後に再実行しても、上書きされることを確認する
	// (中断からの再開を安全にするため)。
	if err := sc.StoreAttribute(nil, "bucket1", "obj", "etag", []byte("v2")); err != nil {
		t.Fatalf("update source: %v", err)
	}
	if _, err := migrate(sc, sq, dir, false, nil); err != nil {
		t.Fatalf("second migrate: %v", err)
	}

	got, err := sq.RetrieveAttribute(nil, "bucket1", "obj", "etag")
	if err != nil {
		t.Fatalf("retrieve: %v", err)
	}
	if string(got) != "v2" {
		t.Fatalf("etag = %q, want %q (re-run should overwrite)", got, "v2")
	}
}

func TestMigrateProgressCallback(t *testing.T) {
	sc, dir := newSideCar(t)
	sq := newSQLiteMeta(t)

	if err := sc.StoreAttribute(nil, "bucket1", "obj", "etag", []byte("v")); err != nil {
		t.Fatalf("store: %v", err)
	}

	var calls []entry
	_, err := migrate(sc, sq, dir, false, func(e entry, attrs int) {
		calls = append(calls, e)
		if attrs != 1 {
			t.Errorf("progress attrs = %d, want 1", attrs)
		}
	})
	if err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if len(calls) != 1 || calls[0].bucket != "bucket1" || calls[0].object != "obj" {
		t.Fatalf("progress calls = %+v, want one call for bucket1/obj", calls)
	}
}
