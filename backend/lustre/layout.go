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

// マルチパートのレイアウトは posix バックエンドに意図的に揃えてあり、1 つの
// ゲートウェイルートを両者で共有できる。違いは part のペイロードを part ごとの
// ファイルに置かない点で、最終オブジェクト内で占めることになるオフセットへ
// 直接書き込む。これが全バイトの 2 回目の書き込みを無くしている。
//
//	<bucket>/.sgwtmp/multipart/<sha256hex(key)>/           属性 "objname" を持つ
//	<bucket>/.sgwtmp/multipart/<sha256hex(key)>/<uploadID>/
//	    data          ステージングファイル。スパースで全 part のペイロードを保持
//	    slotsize      このアップロードが作成された時点の part サイズ
//	    <N>           part N のサイズを持つ空のスパースなマーカー。列挙用
const (
	metaTmpDir          = ".sgwtmp"
	metaTmpMultipartDir = metaTmpDir + "/multipart"

	inProgressSuffix = ".inprogress"

	stagingName  = "data"
	slotSizeName = "slotsize"
)

// 属性キー。backend/posix では非公開なのでここに写している。値はオンディスク
// フォーマットの一部なので、posix 側とズレさせてはいけない。
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

// objHash は 1 つのオブジェクトキーに対する進行中の全アップロードをまとめる
// ディレクトリ名を返す。
func objHash(object string) string {
	sum := sha256.Sum256([]byte(object))
	return fmt.Sprintf("%x", sum)
}

// objdir はあるキーの全アップロードを収めるバケット相対ディレクトリを返す。
func objdir(object string) string {
	return filepath.Join(metaTmpMultipartDir, objHash(object))
}

// uploadDir は 1 つのアップロードのバケット相対ディレクトリを返す。
func uploadDir(object, uploadID string) string {
	return filepath.Join(objdir(object), uploadID)
}

// partMarker は part に対応するスパースなマーカーのバケット相対パスを返す。
// ペイロード自体はステージングファイル側にあるが、マーカーがあることで part の
// 列挙とサイズ取得を posix バックエンドと同じく readdir と stat のままにできる。
func partMarker(updir string, part int32) string {
	return filepath.Join(updir, strconv.Itoa(int(part)))
}

// stagingPath は全 part の書き込み先となる 1 本のファイルのバケット相対パスを
// 返す。
func stagingPath(updir string) string {
	return filepath.Join(updir, stagingName)
}

// readSlotSize はアップロードが作成された時点の part サイズを返す。記録が無い
// 場合は 0 を返す。
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

// writeSlotSize はアップロード作成時の part サイズを記録する。part の配置は
// このサイズで決まるため、別の値で再起動したゲートウェイが古い値でレイアウト
// されたアップロードに黙って書き足すことがあってはならない。
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

// slotOffset は part n の書き込み位置を返す。
func slotOffset(slot int64, part int32) int64 {
	return int64(part-1) * slot
}
