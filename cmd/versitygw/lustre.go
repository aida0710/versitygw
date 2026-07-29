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
	"io/fs"
	"math"

	"github.com/urfave/cli/v2"
	"github.com/versity/versitygw/backend/lustre"
	"github.com/versity/versitygw/backend/posix"
)

var (
	mpuPartSize        int64
	disableDirectMpu   bool
	lustreVersioingDir string
)

func lustreCommand() *cli.Command {
	return &cli.Command{
		Name:  "lustre",
		Usage: "lustre filesystem storage backend",
		Description: `Support for Lustre and other parallel filesystems that cannot share blocks
between files. The top level directory for the gateway must be provided. All
sub directories of the top level directory are treated as buckets, and all
files/directories below the "bucket directory" are treated as the objects.
The object name is split on "/" separator to translate to posix storage.
For example:
top level: /mnt/fs/gwroot
bucket: mybucket
object: a/b/c/myobject
will be translated into the file /mnt/fs/gwroot/mybucket/a/b/c/myobject

The posix backend writes every multipart part to its own temporary file and
copies those files into the finished object on completion. That copy is free
on filesystems with reflink support, but Lustre stripes files across object
storage targets on separate servers and has no block sharing for
copy_file_range to use, so each byte of a multipart upload reaches disk twice.

This backend gives an upload one sparse staging file and writes each part
straight into the region it will occupy in the finished object, so completing
the upload is a truncate and a rename. Uploads whose parts are not uniformly
sized fall back to assembling by copy.

Lustre installations frequently have user extended attributes disabled, so
--metadb is the expected way to run this backend. It keeps object and bucket
attributes in per-bucket sqlite databases, which can sit on a different
filesystem from the object data.`,
		Action: runLustre,
		Flags: append([]cli.Flag{
			&cli.BoolFlag{
				Name:        "chuid",
				Usage:       "chown newly created files and directories to client account UID",
				EnvVars:     []string{"VGW_CHOWN_UID"},
				Destination: &chownuid,
			},
			&cli.BoolFlag{
				Name:        "chgid",
				Usage:       "chown newly created files and directories to client account GID",
				EnvVars:     []string{"VGW_CHOWN_GID"},
				Destination: &chowngid,
			},
			&cli.BoolFlag{
				Name:        "bucketlinks",
				Usage:       "allow symlinked directories at bucket level to be treated as buckets",
				EnvVars:     []string{"VGW_BUCKET_LINKS"},
				Destination: &bucketlinks,
			},
			&cli.StringFlag{
				Name:        "versioning-dir",
				Usage:       "the directory path to enable bucket versioning, requires --disable-direct-mpu",
				EnvVars:     []string{"VGW_VERSIONING_DIR"},
				Destination: &lustreVersioingDir,
			},
			&cli.UintFlag{
				Name:        "dir-perms",
				Usage:       "default directory permissions for new directories",
				EnvVars:     []string{"VGW_DIR_PERMS"},
				Destination: &dirPerms,
				DefaultText: "0755",
				Value:       0755,
			},
			&cli.IntFlag{
				Name:        "concurrency",
				Usage:       "maximum concurrent actions allowed",
				EnvVars:     []string{"VGW_POSIX_CONCURRENCY"},
				Value:       5000,
				Destination: &actionsConcurrency,
			},
			&cli.Int64Flag{
				Name:        "mpu-part-size",
				Usage:       "expected multipart part size in bytes, used to place parts in the staging file. Defaults to the size of the first part of each upload to arrive",
				EnvVars:     []string{"VGW_MPU_PART_SIZE"},
				Destination: &mpuPartSize,
			},
			&cli.BoolFlag{
				Name:        "disable-direct-mpu",
				Usage:       "write multipart parts to individual files and copy them on completion, as the posix backend does",
				EnvVars:     []string{"VGW_DISABLE_DIRECT_MPU"},
				Destination: &disableDirectMpu,
			},
			&cli.BoolFlag{
				Name:        "disableotmp",
				Usage:       "disable O_TMPFILE support for new objects",
				EnvVars:     []string{"VGW_DISABLE_OTMP"},
				Destination: &forceNoTmpFile,
			},
			&cli.StringFlag{
				Name:        "default-etag",
				Usage:       "default ETag value returned for objects that do not have a stored etag attribute (e.g. files placed on the filesystem outside of versitygw)",
				EnvVars:     []string{"VGW_DEFAULT_ETAG"},
				Destination: &defaultEtag,
			},
		}, metaStoreFlags()...),
	}
}

func runLustre(ctx *cli.Context) error {
	if ctx.NArg() == 0 {
		return fmt.Errorf("no directory provided for operation")
	}

	gwroot := ctx.Args().Get(0)

	if dirPerms > math.MaxUint32 {
		return fmt.Errorf("invalid directory permissions: %d", dirPerms)
	}

	if actionsConcurrency <= 0 {
		return fmt.Errorf("concurrency must be positive, got %d", actionsConcurrency)
	}

	if mpuPartSize < 0 {
		return fmt.Errorf("mpu part size must not be negative, got %d", mpuPartSize)
	}

	ms, err := newMetaStore(gwroot)
	if err != nil {
		return err
	}

	opts := lustre.Opts{
		Posix: posix.PosixOpts{
			ChownUID:            chownuid,
			ChownGID:            chowngid,
			BucketLinks:         bucketlinks,
			VersioningDir:       lustreVersioingDir,
			NewDirPerm:          fs.FileMode(dirPerms),
			ForceNoTmpFile:      forceNoTmpFile,
			ValidateBucketNames: disableStrictBucketNames,
			Concurrency:         actionsConcurrency,
			CopyObjectThreshold: copyObjectThreshold,
			DefaultEtag:         defaultEtag,
			SideCarDir:          ms.sidecarDir,
		},
		MetaStore:              ms.storer,
		PartSize:               mpuPartSize,
		DisableDirectMultipart: disableDirectMpu,
	}

	be, err := lustre.New(gwroot, ms.storer, opts)
	if err != nil {
		return fmt.Errorf("failed to init lustre backend: %w", err)
	}

	return runGateway(ctx.Context, be)
}
