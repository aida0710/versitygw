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
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
)

// The multipart layout deliberately mirrors the posix backend so that the
// two can share a gateway root, with one difference: part payloads are not
// stored as one file per part. They are written straight into a single
// sparse staging file at the offset the part will occupy in the finished
// object, which is what removes the second write of every byte.
//
//	<bucket>/.sgwtmp/multipart/<sha256hex(key)>/           carries attr "objname"
//	<bucket>/.sgwtmp/multipart/<sha256hex(key)>/<uploadID>/
//	    data          the staging file, sparse, holds every part payload
//	    slotsize      the configured part size the upload was created under
//	    <N>           empty sparse marker sized to part N, for enumeration
const (
	metaTmpDir          = ".sgwtmp"
	metaTmpMultipartDir = metaTmpDir + "/multipart"

	inProgressSuffix = ".inprogress"

	stagingName  = "data"
	slotSizeName = "slotsize"
)

// Attribute keys, mirrored from backend/posix where they are unexported. The
// values are part of the on-disk format, so they must not drift.
const (
	etagkey            = "etag"
	checksumsKey       = "checksums"
	partCrc64nvme      = "part-crc64nvme"
	mpMetaKey          = "mp-metadata"
	tagHdr             = "X-Amz-Tagging"
	metadataHdr        = "metadata"
	contentTypeHdr     = "content-type"
	contentEncHdr      = "content-encoding"
	contentLangHdr     = "content-language"
	contentDispHdr     = "content-disposition"
	cacheCtrlHdr       = "cache-control"
	expiresHdr         = "expires"
	websiteRedirectHdr = "website-redirect-location"
	objectRetentionKey = "object-retention"
	objectLegalHoldKey = "object-legal-hold"
)

// objHash is the directory name that groups every in-flight upload for one
// object key.
func objHash(object string) string {
	sum := sha256.Sum256([]byte(object))
	return fmt.Sprintf("%x", sum)
}

// objdir is the bucket relative directory holding all uploads for a key.
func objdir(object string) string {
	return filepath.Join(metaTmpMultipartDir, objHash(object))
}

// uploadDir is the bucket relative directory of a single upload.
func uploadDir(object, uploadID string) string {
	return filepath.Join(objdir(object), uploadID)
}

// partMarker is the bucket relative path of the sparse marker for a part.
// The marker exists so that parts can be enumerated and sized with a readdir
// and a stat, exactly as the posix backend does, while the payload itself
// lives in the staging file.
func partMarker(updir string, part int32) string {
	return filepath.Join(updir, strconv.Itoa(int(part)))
}

// stagingPath is the bucket relative path of the single file every part is
// written into.
func stagingPath(updir string) string {
	return filepath.Join(updir, stagingName)
}

// readSlotSize returns the part size an upload was created under, or 0 when
// the record is missing.
func readSlotSize(dir string) (int64, error) {
	b, err := os.ReadFile(filepath.Join(dir, slotSizeName))
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("read slot size: %w", err)
	}

	n, err := strconv.ParseInt(string(b), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse slot size %q: %w", b, err)
	}

	return n, nil
}

// writeSlotSize records the part size an upload is created under. Parts are
// placed by that size, so a gateway restarted with a different one must not
// silently keep filling an upload that was laid out for the old value.
func writeSlotSize(dir string, size int64) error {
	name := filepath.Join(dir, slotSizeName)

	f, err := os.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		return fmt.Errorf("create slot size: %w", err)
	}

	_, werr := f.WriteString(strconv.FormatInt(size, 10))
	cerr := f.Close()
	if werr != nil || cerr != nil {
		os.Remove(name)
		return fmt.Errorf("write slot size: %w", errors.Join(werr, cerr))
	}

	return nil
}

// slotOffset is where part n is written.
func slotOffset(slot int64, part int32) int64 {
	return int64(part-1) * slot
}
