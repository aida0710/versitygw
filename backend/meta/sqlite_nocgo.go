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

//go:build !cgo

package meta

import "errors"

// SQLiteMeta is a placeholder for builds without cgo. The SQLite metadata
// storer links against the sqlite3 C library, so it is only available when
// the gateway is built with CGO_ENABLED=1.
type SQLiteMeta struct {
	NoMeta
}

// Close is a no-op for the placeholder.
func (*SQLiteMeta) Close() error { return nil }

// NewSQLite always fails on builds without cgo.
func NewSQLite(_ string) (*SQLiteMeta, error) {
	return nil, errors.New("sqlite metadata requires a build with cgo enabled (CGO_ENABLED=1)")
}
