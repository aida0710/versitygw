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

package meta

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"sync"

	// sqlite3 driver, requires cgo
	_ "github.com/mattn/go-sqlite3"
	"github.com/versity/versitygw/s3err"
)

// SQLiteMeta is a MetadataStorer that keeps all object and bucket attributes
// in per-bucket SQLite databases held in a dedicated directory, typically on
// a filesystem separate from the object data.
//
// It exists for filesystems that cannot store user extended attributes, and
// where the file-per-attribute layout of SideCar generates too many inode and
// directory entry operations. A parallel filesystem such as Lustre serves
// every one of those lookups from its metadata servers, so collapsing the
// attributes of a whole bucket into a single database file replaces a large
// number of namespace operations with a handful of reads against one file.
type SQLiteMeta struct {
	dir string

	mu  sync.Mutex
	dbs map[string]*sql.DB
}

const (
	// sharedDB holds attributes for callers that address a container by
	// path rather than by plain bucket name, such as the object versioning
	// directory. Buckets can never collide with this name because S3
	// bucket names may not begin with a dot.
	sharedDB = ".shared"

	dbSuffix = ".db"

	// maxReaders bounds the connection pool of a single bucket database.
	// SQLite in WAL mode serves concurrent readers alongside one writer,
	// and writers that collide wait out the busy timeout below.
	maxReadersPerCPU = 4

	busyTimeoutMS = 10000
)

const createTableStmt = `
CREATE TABLE IF NOT EXISTS attributes (
	okey  TEXT NOT NULL,
	attr  TEXT NOT NULL,
	value BLOB NOT NULL,
	PRIMARY KEY (okey, attr)
) WITHOUT ROWID;`

// NewSQLite creates a SQLiteMeta storing its databases in dir. The directory
// must already exist. The path is resolved to an absolute one because the
// posix backend changes the process working directory to the gateway root
// after the metadata storer has been constructed.
func NewSQLite(dir string) (*SQLiteMeta, error) {
	absdir, err := filepath.Abs(dir)
	if err != nil {
		return nil, fmt.Errorf("get absolute path of %q: %w", dir, err)
	}

	fi, err := os.Lstat(absdir)
	if err != nil {
		return nil, fmt.Errorf("failed to stat directory: %v", err)
	}
	if !fi.IsDir() {
		return nil, fmt.Errorf("not a directory")
	}

	return &SQLiteMeta{
		dir: absdir,
		dbs: make(map[string]*sql.DB),
	}, nil
}

// Close releases every open database handle. It is safe to call more than
// once.
func (s *SQLiteMeta) Close() error {
	s.mu.Lock()
	dbs := s.dbs
	s.dbs = make(map[string]*sql.DB)
	s.mu.Unlock()

	var errs []error
	for _, db := range dbs {
		if err := db.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// isSimpleName reports whether name can be used directly as a database file
// name. Callers sometimes pass a multi-segment path where a bucket name is
// expected, most notably the object versioning directory, and those are
// folded into the shared database instead.
func isSimpleName(name string) bool {
	if name == "" || name == "." || name == ".." {
		return false
	}
	if strings.HasPrefix(name, ".") {
		return false
	}
	return !strings.ContainsAny(name, `/\`)
}

// normalizeKey turns a path into the canonical slash separated form used for
// the okey column.
func normalizeKey(p string) string {
	p = filepath.ToSlash(p)
	p = path.Clean("/" + p)
	return strings.TrimPrefix(p, "/")
}

// resolve maps a (bucket, object) pair onto the database that holds it and
// the row key within that database.
func resolve(bucket, object string) (string, string) {
	bucket, object = trimVolume(bucket), trimVolume(object)
	if isSimpleName(bucket) {
		return bucket, normalizeKey(object)
	}
	return sharedDB, normalizeKey(filepath.ToSlash(bucket) + "/" + filepath.ToSlash(object))
}

func (s *SQLiteMeta) dbPath(name string) string {
	return filepath.Join(s.dir, name+dbSuffix)
}

// getDB returns the handle for the named database. When create is false and
// the database file does not exist yet, it returns a nil handle rather than
// bringing an empty database into existence for what is only a read.
func (s *SQLiteMeta) getDB(name string, create bool) (*sql.DB, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if db, ok := s.dbs[name]; ok {
		return db, nil
	}

	dbpath := s.dbPath(name)
	if !create {
		_, err := os.Stat(dbpath)
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		if err != nil {
			return nil, fmt.Errorf("stat metadata db: %w", err)
		}
	}

	dsn := fmt.Sprintf("file:%s?_busy_timeout=%d&_journal_mode=WAL&_synchronous=NORMAL&_txlock=immediate",
		dbpath, busyTimeoutMS)
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, fmt.Errorf("open metadata db %q: %w", dbpath, err)
	}

	maxConns := runtime.NumCPU() * maxReadersPerCPU
	db.SetMaxOpenConns(maxConns)
	db.SetMaxIdleConns(maxConns)
	db.SetConnMaxLifetime(0)

	if _, err := db.Exec(createTableStmt); err != nil {
		db.Close()
		return nil, wrapDBErr(fmt.Errorf("init metadata db %q: %w", dbpath, err))
	}

	s.dbs[name] = db
	return db, nil
}

// dropDB closes and removes the named database along with its write ahead
// log sidecars.
func (s *SQLiteMeta) dropDB(name string) error {
	s.mu.Lock()
	db, ok := s.dbs[name]
	delete(s.dbs, name)
	s.mu.Unlock()

	if ok {
		if err := db.Close(); err != nil {
			return fmt.Errorf("close metadata db: %w", err)
		}
	}

	dbpath := s.dbPath(name)
	for _, p := range []string{dbpath, dbpath + "-wal", dbpath + "-shm"} {
		err := os.Remove(p)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("remove %q: %w", p, err)
		}
	}
	return nil
}

// likeEscape quotes the LIKE wildcards in a literal prefix so that attribute
// paths containing them are not treated as patterns.
func likeEscape(s string) string {
	r := strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`)
	return r.Replace(s)
}

// wrapDBErr converts the disk full condition into the S3 error the gateway
// reports for it, and leaves everything else untouched.
func wrapDBErr(err error) error {
	if err == nil {
		return nil
	}
	// The sqlite3 driver reports a full filesystem as a generic disk I/O
	// error, so match on the message rather than an errno.
	if strings.Contains(err.Error(), "disk is full") ||
		strings.Contains(err.Error(), "database or disk is full") {
		return s3err.GetAPIError(s3err.ErrNoSpaceLeftOnDevice)
	}
	return err
}

// RetrieveAttribute retrieves the value of a specific attribute for an object
// or a bucket.
func (s *SQLiteMeta) RetrieveAttribute(_ *os.File, bucket, object, attribute string) ([]byte, error) {
	dbname, okey := resolve(bucket, object)

	db, err := s.getDB(dbname, false)
	if err != nil {
		return nil, err
	}
	if db == nil {
		return nil, ErrNoSuchKey
	}

	var value []byte
	err = db.QueryRow(
		`SELECT value FROM attributes WHERE okey = ? AND attr = ?`,
		okey, attribute).Scan(&value)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNoSuchKey
	}
	if err != nil {
		return nil, fmt.Errorf("read attribute: %w", err)
	}
	if value == nil {
		// database/sql yields a nil slice for a zero length blob, while
		// callers distinguish a stored empty value from a missing one.
		value = []byte{}
	}

	return value, nil
}

// StoreAttribute stores the value of a specific attribute for an object or a
// bucket, replacing any previous value.
func (s *SQLiteMeta) StoreAttribute(_ *os.File, bucket, object, attribute string, value []byte) error {
	dbname, okey := resolve(bucket, object)

	db, err := s.getDB(dbname, true)
	if err != nil {
		return err
	}

	if value == nil {
		value = []byte{}
	}

	_, err = db.Exec(
		`INSERT INTO attributes (okey, attr, value) VALUES (?, ?, ?)
		 ON CONFLICT(okey, attr) DO UPDATE SET value = excluded.value`,
		okey, attribute, value)
	if err != nil {
		return wrapDBErr(fmt.Errorf("write attribute: %w", err))
	}

	return nil
}

// DeleteAttribute removes the value of a specific attribute for an object or
// a bucket.
func (s *SQLiteMeta) DeleteAttribute(bucket, object, attribute string) error {
	dbname, okey := resolve(bucket, object)

	db, err := s.getDB(dbname, false)
	if err != nil {
		return err
	}
	if db == nil {
		return ErrNoSuchKey
	}

	res, err := db.Exec(
		`DELETE FROM attributes WHERE okey = ? AND attr = ?`, okey, attribute)
	if err != nil {
		return wrapDBErr(fmt.Errorf("remove attribute: %w", err))
	}

	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("remove attribute: %w", err)
	}
	if n == 0 {
		return ErrNoSuchKey
	}

	return nil
}

// ListAttributes lists all attributes for an object or a bucket. Attributes
// of nested keys are not included.
func (s *SQLiteMeta) ListAttributes(bucket, object string) ([]string, error) {
	dbname, okey := resolve(bucket, object)

	db, err := s.getDB(dbname, false)
	if err != nil {
		return nil, err
	}
	if db == nil {
		return []string{}, nil
	}

	rows, err := db.Query(`SELECT attr FROM attributes WHERE okey = ?`, okey)
	if err != nil {
		return nil, fmt.Errorf("list attributes: %w", err)
	}
	defer rows.Close()

	var attrs []string
	for rows.Next() {
		var attr string
		if err := rows.Scan(&attr); err != nil {
			return nil, fmt.Errorf("list attributes: %w", err)
		}
		attrs = append(attrs, attr)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list attributes: %w", err)
	}

	return attrs, nil
}

// DeleteAttributes removes all attributes for an object or a bucket, along
// with those of any keys nested below it. Nested keys matter because the
// posix backend stores multipart part attributes underneath the upload
// directory and drops the whole subtree at once.
//
// When object is empty and bucket names a real bucket, the entire database is
// removed so that no orphaned object or multipart metadata survives
// DeleteBucket.
func (s *SQLiteMeta) DeleteAttributes(bucket, object string) error {
	dbname, okey := resolve(bucket, object)

	if dbname != sharedDB && okey == "" {
		return s.dropDB(dbname)
	}

	db, err := s.getDB(dbname, false)
	if err != nil {
		return err
	}
	if db == nil {
		return nil
	}

	_, err = db.Exec(
		`DELETE FROM attributes WHERE okey = ? OR okey LIKE ? ESCAPE '\'`,
		okey, likeEscape(okey)+`/%`)
	if err != nil {
		return wrapDBErr(fmt.Errorf("remove attributes: %w", err))
	}

	return nil
}

// RenameObject moves all attributes stored under oldObject, including those
// of nested keys, to newObject.
func (s *SQLiteMeta) RenameObject(bucket, oldObject, newObject string) error {
	dbname, oldKey := resolve(bucket, oldObject)
	_, newKey := resolve(bucket, newObject)

	if oldKey == newKey {
		return nil
	}

	db, err := s.getDB(dbname, false)
	if err != nil {
		return err
	}
	if db == nil {
		// No metadata stored yet, nothing to rename.
		return nil
	}

	tx, err := db.Begin()
	if err != nil {
		return wrapDBErr(fmt.Errorf("rename metadata: %w", err))
	}
	defer tx.Rollback()

	// Clear the destination first so that a partially populated target does
	// not collide with the rows being moved onto it.
	_, err = tx.Exec(
		`DELETE FROM attributes WHERE okey = ? OR okey LIKE ? ESCAPE '\'`,
		newKey, likeEscape(newKey)+`/%`)
	if err != nil {
		return wrapDBErr(fmt.Errorf("rename metadata: %w", err))
	}

	_, err = tx.Exec(
		`UPDATE attributes SET okey = ? WHERE okey = ?`, newKey, oldKey)
	if err != nil {
		return wrapDBErr(fmt.Errorf("rename metadata: %w", err))
	}

	// length() and substr() both count characters, so deriving the offset
	// from length() keeps multi-byte keys correct.
	_, err = tx.Exec(
		`UPDATE attributes SET okey = ? || substr(okey, length(?) + 1) WHERE okey LIKE ? ESCAPE '\'`,
		newKey, oldKey, likeEscape(oldKey)+`/%`)
	if err != nil {
		return wrapDBErr(fmt.Errorf("rename metadata: %w", err))
	}

	if err := tx.Commit(); err != nil {
		return wrapDBErr(fmt.Errorf("rename metadata: %w", err))
	}

	return nil
}
