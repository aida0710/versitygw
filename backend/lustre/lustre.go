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

// Package lustre provides a storage backend for parallel filesystems that
// cannot share blocks between files.
//
// The posix backend assembles a multipart upload by writing every part to its
// own temporary file and then copying those files into the finished object.
// On a filesystem with reflink support that copy is a block reference update
// and costs nothing, which is what makes the design reasonable. Lustre stripes
// a file across object storage targets on separate servers, so there is no
// local block sharing for copy_file_range to exploit and every byte of a
// multipart upload is written to disk twice.
//
// This backend removes the second write. An upload gets one sparse staging
// file and each part is written directly into the region it will occupy in the
// finished object, so completing the upload is a truncate and a rename rather
// than a copy. Everything outside the multipart path is inherited from the
// posix backend unchanged.
package lustre

import (
	"context"
	"fmt"
	"io/fs"
	"os"

	"github.com/versity/versitygw/auth"
	"github.com/versity/versitygw/backend/meta"
	"github.com/versity/versitygw/backend/posix"
	"golang.org/x/sync/semaphore"
)

// Lustre is a posix backend with a multipart implementation that does not
// copy part data on completion.
type Lustre struct {
	*posix.Posix

	// metastore is the same storer handed to the embedded posix backend.
	// The posix fields are unexported, so the multipart implementation here
	// needs its own reference to read and write attributes.
	metastore meta.MetadataStorer

	rootdir string

	// partSize pins the slot stride used to place parts in the staging
	// file. When zero, the stride is taken from the first part of each
	// upload to arrive.
	partSize int64

	// directMultipart enables writing parts straight into the staging file.
	// With it off the backend behaves exactly like posix, which is useful
	// for isolating a problem to this code.
	directMultipart bool

	// The posix equivalents of these are unexported, so the multipart
	// implementation here keeps its own copies.
	newDirPerm fs.FileMode
	chownuid   bool
	chowngid   bool
	euid       int
	egid       int

	// actionLimiter bounds concurrent filesystem work in the methods this
	// backend implements itself. The posix limiter still covers everything
	// inherited from it.
	actionLimiter *semaphore.Weighted
}

// defaultConcurrency matches the posix backend default.
const defaultConcurrency = 5000

// acquireActionSlot blocks until the backend is allowed to start another
// filesystem heavy action.
func (l *Lustre) acquireActionSlot(ctx context.Context) (func(), error) {
	if err := l.actionLimiter.Acquire(ctx, 1); err != nil {
		return func() {}, fmt.Errorf("acquire action slot: %w", err)
	}
	return func() { l.actionLimiter.Release(1) }, nil
}

// getChownIDs returns the uid and gid newly created files should belong to,
// and whether chowning them is needed at all.
func (l *Lustre) getChownIDs(acct auth.Account) (int, int, bool) {
	uid := l.euid
	gid := l.egid
	var needsChown bool
	if l.chownuid && acct.UserID != l.euid {
		uid = acct.UserID
		needsChown = true
	}
	if l.chowngid && acct.GroupID != l.egid {
		gid = acct.GroupID
		needsChown = true
	}

	return uid, gid, needsChown
}

// Opts are the options for the Lustre backend. The posix options are
// forwarded as is.
type Opts struct {
	Posix posix.PosixOpts

	// MetaStore is the metadata storer, which must be the same instance
	// given to the posix backend.
	MetaStore meta.MetadataStorer

	// PartSize pins the multipart slot stride in bytes. Leave it at zero to
	// adopt the size of the first part of each upload.
	PartSize int64

	// DisableDirectMultipart falls back to the posix multipart path, which
	// copies part data into the finished object.
	DisableDirectMultipart bool
}

// New creates a Lustre backend rooted at rootdir.
func New(rootdir string, metastore meta.MetadataStorer, opts Opts) (*Lustre, error) {
	if metastore == nil {
		return nil, fmt.Errorf("a metadata storer is required")
	}

	direct := !opts.DisableDirectMultipart

	// Writing parts into their final position means there is no separate
	// per part file for the posix version machinery to work from, and the
	// helpers that create an object version are internal to that package.
	if direct && opts.Posix.VersioningDir != "" {
		return nil, fmt.Errorf("bucket versioning is not supported with direct multipart writes, pass --disable-direct-mpu to use the copying multipart path")
	}

	if opts.PartSize < 0 {
		return nil, fmt.Errorf("invalid part size %d", opts.PartSize)
	}

	p, err := posix.New(rootdir, metastore, opts.Posix)
	if err != nil {
		return nil, err
	}

	concurrency := opts.Posix.Concurrency
	if concurrency <= 0 {
		concurrency = defaultConcurrency
	}

	return &Lustre{
		Posix:           p,
		metastore:       metastore,
		rootdir:         rootdir,
		partSize:        opts.PartSize,
		directMultipart: direct,
		newDirPerm:      opts.Posix.NewDirPerm,
		chownuid:        opts.Posix.ChownUID,
		chowngid:        opts.Posix.ChownGID,
		euid:            os.Geteuid(),
		egid:            os.Getegid(),
		actionLimiter:   semaphore.NewWeighted(int64(concurrency)),
	}, nil
}

func (*Lustre) String() string {
	return "Lustre Gateway"
}

// Shutdown releases the backend resources, including the metadata storer when
// it holds any.
func (l *Lustre) Shutdown() {
	l.Posix.Shutdown()

	if c, ok := l.metastore.(interface{ Close() error }); ok {
		if err := c.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "close metadata storer: %v\n", err)
		}
	}
}
