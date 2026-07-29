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

// migrate-sidecar は sidecar 形式のメタデータストア(属性ごとに個別ファイル)を
// SQLite 形式のメタデータストアへコピーする。
//
// 属性の読み書き自体は backend/meta の SideCar と SQLiteMeta にそのまま
// 委譲するため、このパッケージが新しく持つロジックは「sidecar ディレクトリ
// ツリーを歩いて (bucket, object) の組を列挙する」部分だけである。パスの
// 解決や属性のオンディスク表現を再実装しないことで、既存のテスト済みの
// 挙動をそのまま引き継ぐ。
package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/versity/versitygw/backend/meta"
)

// sidecarMetaDirName は sidecar が属性ファイルを置くディレクトリ名である。
// backend/meta.SideCar の非公開定数 "meta" と同じ値でなければならない。
const sidecarMetaDirName = "meta"

// multipartStagingDir は進行中のマルチパートアップロードのメタデータを
// 保持するディレクトリで、移行対象から除外する。進行中のアップロードは
// 完了済みオブジェクトではなく、新バックエンドのステージング形式も
// 全く異なるため、移行しても意味がない。
const multipartStagingDir = ".sgwtmp"

// entry は移行すべき 1 つの (bucket, object) の組を表す。object が空文字列
// の場合はバケットレベルの属性を指す。
type entry struct {
	bucket string
	object string
}

// discoverEntries は sidecarDir 以下を歩き、属性を持つ (bucket, object) の
// 組をすべて列挙する。".sgwtmp" ディレクトリ配下(進行中のマルチパート
// アップロードのメタデータ)は無視する。結果は bucket, object の順で
// ソートされ、実行のたびに同じ順序になる。
func discoverEntries(sidecarDir string) ([]entry, error) {
	var entries []entry

	err := filepath.WalkDir(sidecarDir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() || d.Name() != sidecarMetaDirName {
			return nil
		}

		rel, err := filepath.Rel(sidecarDir, filepath.Dir(path))
		if err != nil {
			return fmt.Errorf("resolve relative path of %q: %w", path, err)
		}
		rel = filepath.ToSlash(rel)

		if rel == "." {
			// meta が sidecarDir 直下にある。bucket 名を特定できないので
			// 無視する(通常のレイアウトでは起こらない)。
			return filepath.SkipDir
		}

		parts := strings.SplitN(rel, "/", 2)
		bucket := parts[0]
		object := ""
		if len(parts) == 2 {
			object = parts[1]
		}

		if bucket == multipartStagingDir || strings.HasPrefix(object, multipartStagingDir+"/") || object == multipartStagingDir {
			return filepath.SkipDir
		}

		entries = append(entries, entry{bucket: bucket, object: object})

		// meta ディレクトリの下は属性ファイルのみで、これ以上サブ
		// ディレクトリを持たないため潜る必要はない。
		return filepath.SkipDir
	})
	if err != nil {
		return nil, err
	}

	sort.Slice(entries, func(i, j int) bool {
		if entries[i].bucket != entries[j].bucket {
			return entries[i].bucket < entries[j].bucket
		}
		return entries[i].object < entries[j].object
	})

	return entries, nil
}

// migrateEntry は 1 つの (bucket, object) について、src が保持する全属性を
// dst へコピーする。既に dst に存在する属性は上書きする(再実行しても
// 安全なようにするため)。コピーした属性数を返す。
func migrateEntry(src, dst meta.MetadataStorer, e entry) (int, error) {
	attrs, err := src.ListAttributes(e.bucket, e.object)
	if err != nil {
		return 0, fmt.Errorf("list attributes for %s/%s: %w", e.bucket, e.object, err)
	}

	copied := 0
	for _, attr := range attrs {
		value, err := src.RetrieveAttribute(nil, e.bucket, e.object, attr)
		if err != nil {
			return copied, fmt.Errorf("read attribute %s/%s:%s: %w", e.bucket, e.object, attr, err)
		}
		if err := dst.StoreAttribute(nil, e.bucket, e.object, attr, value); err != nil {
			return copied, fmt.Errorf("write attribute %s/%s:%s: %w", e.bucket, e.object, attr, err)
		}
		copied++
	}

	return copied, nil
}

// result は migrate の実行結果を集計したものである。
type result struct {
	entries    int
	attributes int
}

// migrate は sidecarDir 以下のすべての (bucket, object) を src から dst へ
// 移行する。dryRun が true の場合は何も書き込まず、対象を数えるだけに
// とどめる。progress が非 nil なら、エントリごとに呼ばれる(進捗表示用)。
func migrate(src, dst meta.MetadataStorer, sidecarDir string, dryRun bool, progress func(e entry, attrs int)) (result, error) {
	entries, err := discoverEntries(sidecarDir)
	if err != nil {
		return result{}, fmt.Errorf("discover entries: %w", err)
	}

	var res result
	for _, e := range entries {
		var n int
		if dryRun {
			attrs, err := src.ListAttributes(e.bucket, e.object)
			if err != nil {
				return res, fmt.Errorf("list attributes for %s/%s: %w", e.bucket, e.object, err)
			}
			n = len(attrs)
		} else {
			n, err = migrateEntry(src, dst, e)
			if err != nil {
				return res, err
			}
		}

		res.entries++
		res.attributes += n
		if progress != nil {
			progress(e, n)
		}
	}

	return res, nil
}
