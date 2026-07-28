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

// metaStoreFlags are the flags selecting where object and bucket attributes
// are kept. They are shared by the backends built on the posix storage layer.
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

// metaStore is the resolved metadata configuration for a backend.
type metaStore struct {
	storer meta.MetadataStorer

	// sidecarDir is passed to the posix options so that the backend can
	// report and validate it. It is empty for every other mode.
	sidecarDir string
}

// newMetaStore builds the metadata storer selected on the command line. The
// gateway root is needed because a metadata directory placed inside it would
// be served as a bucket.
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

// checkOutsideRoot rejects a metadata directory that lives under the gateway
// root, where it would be exposed as a bucket.
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
