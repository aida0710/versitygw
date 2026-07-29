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

package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/urfave/cli/v2"
	"github.com/versity/versitygw/backend/meta"
)

var (
	metadb string
)

// metaStoreFlags はオブジェクトとバケットの属性の保存先を選ぶフラグ群である。
// posix ストレージ層の上に構築されたバックエンドで共有する。
func metaStoreFlags() []cli.Flag {
	return []cli.Flag{
		&cli.StringFlag{
			Name:        "sidecar",
			Usage:       "use provided sidecar directory to store metadata",
			EnvVars:     []string{"VGW_META_SIDECAR"},
			Destination: &sidecar,
		},
		&cli.StringFlag{
			Name:        "metadb",
			Usage:       "use provided directory to store metadata in per-bucket sqlite databases",
			EnvVars:     []string{"VGW_META_DB"},
			Destination: &metadb,
		},
		&cli.BoolFlag{
			Name:        "nometa",
			Usage:       "disable metadata storage",
			EnvVars:     []string{"VGW_META_NONE"},
			Destination: &nometa,
		},
	}
}

// metaStore はバックエンドに対して解決済みのメタデータ設定である。
type metaStore struct {
	storer meta.MetadataStorer

	// sidecarDir は posix のオプションへ渡し、バックエンド側で検証・表示させる。
	// sidecar 以外のモードでは空になる。
	sidecarDir string
}

// newMetaStore はコマンドラインで選択されたメタデータストアを構築する。
// ゲートウェイルートを受け取るのは、その配下にメタデータディレクトリを置くと
// バケットとして公開されてしまうためである。
func newMetaStore(gwroot string) (metaStore, error) {
	var selected []string
	if sidecar != "" {
		selected = append(selected, "sidecar")
	}
	if metadb != "" {
		selected = append(selected, "metadb")
	}
	if nometa {
		selected = append(selected, "nometa")
	}
	if len(selected) > 1 {
		return metaStore{}, fmt.Errorf("only one metadata mode may be set, got %v",
			strings.Join(selected, ", "))
	}

	switch {
	case metadb != "":
		if err := checkOutsideRoot(gwroot, metadb); err != nil {
			return metaStore{}, err
		}
		ms, err := meta.NewSQLite(metadb)
		if err != nil {
			return metaStore{}, fmt.Errorf("failed to init sqlite metadata: %w", err)
		}
		return metaStore{storer: ms}, nil

	case sidecar != "":
		sc, err := meta.NewSideCar(sidecar)
		if err != nil {
			return metaStore{}, fmt.Errorf("failed to init sidecar metadata: %w", err)
		}
		return metaStore{storer: sc, sidecarDir: sidecar}, nil

	case nometa:
		return metaStore{storer: meta.NoMeta{}}, nil

	default:
		if err := (meta.XattrMeta{}).Test(gwroot); err != nil {
			return metaStore{}, fmt.Errorf("xattr check failed: %w", err)
		}
		return metaStore{storer: meta.XattrMeta{}}, nil
	}
}

// checkOutsideRoot はゲートウェイルート配下にあるメタデータディレクトリを
// 拒否する。そこに置くとバケットとして公開されてしまう。
func checkOutsideRoot(gwroot, dir string) error {
	rootAbs, err := filepath.Abs(gwroot)
	if err != nil {
		return fmt.Errorf("get absolute path of %q: %w", gwroot, err)
	}
	dirAbs, err := filepath.Abs(dir)
	if err != nil {
		return fmt.Errorf("get absolute path of %q: %w", dir, err)
	}

	fi, err := os.Stat(dirAbs)
	if err != nil {
		return fmt.Errorf("stat %q: %w", dirAbs, err)
	}
	if !fi.IsDir() {
		return fmt.Errorf("path %q is not a directory", dirAbs)
	}

	rel, err := filepath.Rel(rootAbs, dirAbs)
	if err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return fmt.Errorf("the root directory %v contains the metadata directory %v",
			rootAbs, dirAbs)
	}

	return nil
}
