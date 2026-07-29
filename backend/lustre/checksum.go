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
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/versity/versitygw/backend"
	"github.com/versity/versitygw/s3api/utils"
	"github.com/versity/versitygw/s3err"
	"github.com/versity/versitygw/s3response"
)

// ここのヘルパーは backend/posix にある非公開の対応物を写したものである。
// 読み書きする属性のペイロードが同一なので、1 つのゲートウェイルート上で両者を
// 入れ替えても動作する。

func (l *Lustre) storeChecksums(f *os.File, bucket, object string, chs s3response.Checksum) error {
	checksums, err := json.Marshal(chs)
	if err != nil {
		return fmt.Errorf("parse checksum: %w", err)
	}

	return l.metastore.StoreAttribute(f, bucket, object, checksumsKey, checksums)
}

func (l *Lustre) retrieveChecksums(f *os.File, bucket, object string) (checksums s3response.Checksum, err error) {
	checksumsAtr, err := l.metastore.RetrieveAttribute(f, bucket, object, checksumsKey)
	if err != nil {
		return checksums, err
	}

	err = json.Unmarshal(checksumsAtr, &checksums)
	return checksums, err
}

// hashConfig はリクエストヘッダのチェックサム値とそのアルゴリズムの対である。
type hashConfig struct {
	value    *string
	hashType utils.HashType
}

func getPartChecksum(algo types.ChecksumAlgorithm, part types.CompletedPart) string {
	switch algo {
	case types.ChecksumAlgorithmCrc32:
		return backend.GetStringFromPtr(part.ChecksumCRC32)
	case types.ChecksumAlgorithmCrc32c:
		return backend.GetStringFromPtr(part.ChecksumCRC32C)
	case types.ChecksumAlgorithmSha1:
		return backend.GetStringFromPtr(part.ChecksumSHA1)
	case types.ChecksumAlgorithmSha256:
		return backend.GetStringFromPtr(part.ChecksumSHA256)
	case types.ChecksumAlgorithmCrc64nvme:
		return backend.GetStringFromPtr(part.ChecksumCRC64NVME)
	case types.ChecksumAlgorithmSha512:
		return backend.GetStringFromPtr(part.ChecksumSHA512)
	case types.ChecksumAlgorithmMd5:
		return backend.GetStringFromPtr(part.ChecksumMD5)
	case types.ChecksumAlgorithmXxhash64:
		return backend.GetStringFromPtr(part.ChecksumXXHASH64)
	case types.ChecksumAlgorithmXxhash3:
		return backend.GetStringFromPtr(part.ChecksumXXHASH3)
	case types.ChecksumAlgorithmXxhash128:
		return backend.GetStringFromPtr(part.ChecksumXXHASH128)
	default:
		return ""
	}
}

func setStoredChecksum(checksum *s3response.Checksum, algo types.ChecksumAlgorithm, sum *string) {
	if sum == nil {
		return
	}

	switch algo {
	case types.ChecksumAlgorithmCrc32:
		checksum.CRC32 = sum
	case types.ChecksumAlgorithmCrc32c:
		checksum.CRC32C = sum
	case types.ChecksumAlgorithmSha1:
		checksum.SHA1 = sum
	case types.ChecksumAlgorithmSha256:
		checksum.SHA256 = sum
	case types.ChecksumAlgorithmCrc64nvme:
		checksum.CRC64NVME = sum
	case types.ChecksumAlgorithmSha512:
		checksum.SHA512 = sum
	case types.ChecksumAlgorithmMd5:
		checksum.MD5 = sum
	case types.ChecksumAlgorithmXxhash64:
		checksum.XXHASH64 = sum
	case types.ChecksumAlgorithmXxhash3:
		checksum.XXHASH3 = sum
	case types.ChecksumAlgorithmXxhash128:
		checksum.XXHASH128 = sum
	}
}

func setUploadPartChecksum(res *s3.UploadPartOutput, algo types.ChecksumAlgorithm, sum *string) {
	if sum == nil {
		return
	}

	switch algo {
	case types.ChecksumAlgorithmCrc32:
		res.ChecksumCRC32 = sum
	case types.ChecksumAlgorithmCrc32c:
		res.ChecksumCRC32C = sum
	case types.ChecksumAlgorithmSha1:
		res.ChecksumSHA1 = sum
	case types.ChecksumAlgorithmSha256:
		res.ChecksumSHA256 = sum
	case types.ChecksumAlgorithmCrc64nvme:
		res.ChecksumCRC64NVME = sum
	case types.ChecksumAlgorithmSha512:
		res.ChecksumSHA512 = sum
	case types.ChecksumAlgorithmMd5:
		res.ChecksumMD5 = sum
	case types.ChecksumAlgorithmXxhash64:
		res.ChecksumXXHASH64 = sum
	case types.ChecksumAlgorithmXxhash3:
		res.ChecksumXXHASH3 = sum
	case types.ChecksumAlgorithmXxhash128:
		res.ChecksumXXHASH128 = sum
	}
}

func validatePartChecksum(checksum s3response.Checksum, part types.CompletedPart, uploadId string) error {
	n, argValue := numberOfChecksums(part)
	if n > 1 {
		return s3err.GetInvalidArgumentErr(s3err.InvalidArgChecksumPart, argValue)
	}
	if checksum.Algorithm == "" {
		if n != 0 {
			return s3err.GetInvalidPartErr(uploadId, *part.PartNumber, *part.ETag)
		}

		return nil
	}

	algo := checksum.Algorithm
	if n == 0 {
		return s3err.APIError{
			Code:           "InvalidRequest",
			Description:    fmt.Sprintf("The upload was created using a %v checksum. The complete request must include the checksum for each part. It was missing for part %v in the request.", strings.ToLower(string(algo)), *part.PartNumber),
			HTTPStatusCode: http.StatusBadRequest,
		}
	}

	for _, cs := range []struct {
		checksum         *string
		expectedChecksum string
		algo             types.ChecksumAlgorithm
	}{
		{part.ChecksumCRC32, backend.GetStringFromPtr(checksum.CRC32), types.ChecksumAlgorithmCrc32},
		{part.ChecksumCRC32C, backend.GetStringFromPtr(checksum.CRC32C), types.ChecksumAlgorithmCrc32c},
		{part.ChecksumSHA1, backend.GetStringFromPtr(checksum.SHA1), types.ChecksumAlgorithmSha1},
		{part.ChecksumSHA256, backend.GetStringFromPtr(checksum.SHA256), types.ChecksumAlgorithmSha256},
		{part.ChecksumCRC64NVME, backend.GetStringFromPtr(checksum.CRC64NVME), types.ChecksumAlgorithmCrc64nvme},
		{part.ChecksumSHA512, backend.GetStringFromPtr(checksum.SHA512), types.ChecksumAlgorithmSha512},
		{part.ChecksumMD5, backend.GetStringFromPtr(checksum.MD5), types.ChecksumAlgorithmMd5},
		{part.ChecksumXXHASH64, backend.GetStringFromPtr(checksum.XXHASH64), types.ChecksumAlgorithmXxhash64},
		{part.ChecksumXXHASH3, backend.GetStringFromPtr(checksum.XXHASH3), types.ChecksumAlgorithmXxhash3},
		{part.ChecksumXXHASH128, backend.GetStringFromPtr(checksum.XXHASH128), types.ChecksumAlgorithmXxhash128},
	} {
		if cs.checksum == nil {
			continue
		}

		if !utils.IsValidChecksum(*cs.checksum, cs.algo) {
			return s3err.GetInvalidArgumentErr(s3err.InvalidArgChecksumPart, *cs.checksum)
		}

		if *cs.checksum != cs.expectedChecksum {
			if algo == cs.algo {
				return s3err.GetInvalidPartErr(uploadId, *part.PartNumber, *part.ETag)
			}

			return s3err.APIError{
				Code:           "BadDigest",
				Description:    fmt.Sprintf("The %v you specified for part %v did not match what we received.", strings.ToLower(string(cs.algo)), *part.PartNumber),
				HTTPStatusCode: http.StatusBadRequest,
			}
		}
	}

	return nil
}

func numberOfChecksums(part types.CompletedPart) (int, string) {
	counter := 0
	builder := &strings.Builder{}

	for _, ch := range []struct {
		algo  types.ChecksumAlgorithm
		value *string
	}{
		{types.ChecksumAlgorithmCrc32, part.ChecksumCRC32},
		{types.ChecksumAlgorithmCrc32c, part.ChecksumCRC32C},
		{types.ChecksumAlgorithmCrc64nvme, part.ChecksumCRC64NVME},
		{types.ChecksumAlgorithmSha1, part.ChecksumSHA1},
		{types.ChecksumAlgorithmSha256, part.ChecksumSHA256},
	} {
		if backend.GetStringFromPtr(ch.value) != "" {
			counter++
			fmt.Fprintf(builder, "%s:%s;", string(ch.algo), backend.GetStringFromPtr(ch.value))
		}
	}
	for _, v := range []*string{
		part.ChecksumSHA512,
		part.ChecksumMD5,
		part.ChecksumXXHASH64,
		part.ChecksumXXHASH3,
		part.ChecksumXXHASH128,
	} {
		if backend.GetStringFromPtr(v) != "" {
			counter++
		}
	}

	return counter, builder.String()
}
