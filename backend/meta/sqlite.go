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

	// sqlite3 ドライバ。cgo が必要
	_ "github.com/mattn/go-sqlite3"
	"github.com/versity/versitygw/s3err"
)

// SQLiteMeta はオブジェクトとバケットの全属性を、専用ディレクトリ配下の
// バケット単位 SQLite データベースに保持する MetadataStorer である。この
// ディレクトリは通常オブジェクトデータとは別のファイルシステムに置く。
//
// user 拡張属性を保存できないファイルシステム、および SideCar の属性ごとに
// ファイルを作るレイアウトでは inode とディレクトリエントリの操作が多すぎる
// 環境のために存在する。Lustre のような並列ファイルシステムではそれらの
// 参照がすべてメタデータサーバへの問い合わせになるため、バケット全体の属性を
// 1 つのデータベースファイルにまとめることで、大量の名前空間操作を 1 ファイル
// への数回の読み取りに置き換えられる。
type SQLiteMeta struct {
	dir string

	mu  sync.Mutex
	dbs map[string]*sql.DB
}

const (
	// sharedDB は、オブジェクトバージョニングのディレクトリのように、単純な
	// バケット名ではなくパスでコンテナを指す呼び出し元の属性を保持する。S3 の
	// バケット名はドットで始められないので、バケットと名前が衝突することはない。
	sharedDB = ".shared"

	dbSuffix = ".db"

	// maxReaders は 1 つのバケットデータベースのコネクションプール上限である。
	// WAL モードの SQLite は 1 つのライタと並行して複数のリーダを処理でき、
	// 競合したライタは下の busy timeout の間待機する。
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

// NewSQLite は dir 配下にデータベースを置く SQLiteMeta を生成する。ディレクトリ
// は事前に存在している必要がある。パスを絶対パスに解決するのは、メタデータ
// ストアの生成後に posix バックエンドがプロセスのカレントディレクトリを
// ゲートウェイルートへ変更するためである。
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

// Close は開いている全データベースハンドルを解放する。複数回呼んでも安全。
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

// isSimpleName は name をそのままデータベースファイル名として使えるかを返す。
// バケット名が来るはずの位置に複数階層のパスが渡されることがあり（代表例が
// オブジェクトバージョニングのディレクトリ）、それらは共有データベースへ
// まとめる。
func isSimpleName(name string) bool {
	if name == "" || name == "." || name == ".." {
		return false
	}
	if strings.HasPrefix(name, ".") {
		return false
	}
	return !strings.ContainsAny(name, `/\`)
}

// normalizeKey はパスを okey 列で使うスラッシュ区切りの正規形へ変換する。
func normalizeKey(p string) string {
	p = filepath.ToSlash(p)
	p = path.Clean("/" + p)
	return strings.TrimPrefix(p, "/")
}

// resolve は (bucket, object) の組を、それを保持するデータベースとその中の
// 行キーへ対応付ける。
//
// 2 つの引数の区切り位置そのものには意味が無い。posix バックエンドは同一の属性を
// ある箇所では ("bucket", "some/object")、別の箇所では ("bucket/some/object", "")
// として参照するが、いずれも同じ行に到達しなければならない。そこで先に両者を
// 連結し、その先頭のパス要素からデータベースを決める。
func resolve(bucket, object string) (string, string) {
	b := filepath.ToSlash(trimVolume(bucket))
	full := normalizeKey(b + "/" + filepath.ToSlash(trimVolume(object)))

	// 絶対パスのコンテナはバケットではない。オブジェクトバージョニングの
	// ディレクトリがこの形で渡ってくるが、その先頭要素はファイルシステム上の
	// パス要素であり、バケット用データベースを占有させてはならない。
	if strings.HasPrefix(b, "/") {
		return sharedDB, full
	}

	first, rest, _ := strings.Cut(full, "/")
	if !isSimpleName(first) {
		return sharedDB, full
	}

	return first, rest
}

func (s *SQLiteMeta) dbPath(name string) string {
	return filepath.Join(s.dir, name+dbSuffix)
}

// getDB は指定名のデータベースのハンドルを返す。create が false でデータベース
// ファイルがまだ存在しない場合は、読み取りのためだけに空のデータベースを作る
// ことはせず nil ハンドルを返す。
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

// dropDB は指定名のデータベースを、その WAL 関連ファイルも含めて閉じて削除する。
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

// likeEscape はリテラルの前方一致文字列に含まれる LIKE のワイルドカードを
// エスケープし、そうした文字を含む属性パスがパターンとして扱われないようにする。
func likeEscape(s string) string {
	r := strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`)
	return r.Replace(s)
}

// wrapDBErr はディスク満杯をゲートウェイが返すべき S3 エラーへ変換する。
// それ以外のエラーはそのまま通す。
func wrapDBErr(err error) error {
	if err == nil {
		return nil
	}
	// sqlite3 ドライバはファイルシステム満杯を汎用のディスク I/O エラーとして
	// 報告するので、errno ではなくメッセージで判定する。
	if strings.Contains(err.Error(), "disk is full") ||
		strings.Contains(err.Error(), "database or disk is full") {
		return s3err.GetAPIError(s3err.ErrNoSpaceLeftOnDevice)
	}
	return err
}

// RetrieveAttribute はオブジェクトまたはバケットの指定属性の値を取得する。
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
		// database/sql は長さ 0 の blob に対して nil スライスを返すが、呼び出し
		// 元は「空の値が保存されている」と「値が無い」を区別する。
		value = []byte{}
	}

	return value, nil
}

// StoreAttribute はオブジェクトまたはバケットの指定属性の値を保存する。既存の
// 値があれば置き換える。
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

// DeleteAttribute はオブジェクトまたはバケットの指定属性を削除する。
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

// ListAttributes はオブジェクトまたはバケットの全属性を列挙する。配下のキーの
// 属性は含まない。
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

// DeleteAttributes はオブジェクトまたはバケットの全属性を、配下のキーのものも
// 含めて削除する。配下も対象にするのは、posix バックエンドがマルチパートの part
// 属性をアップロードディレクトリの下に置き、サブツリーごとまとめて削除するため
// である。
//
// object が空でかつ bucket が実在のバケットを指す場合はデータベース全体を削除
// する。DeleteBucket 後に孤立したオブジェクトやマルチパートのメタデータが残ら
// ないようにするため。
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

// RenameObject は oldObject 配下に保存された全属性を、配下のキーのものも含めて
// newObject へ移す。
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
		// メタデータがまだ無いので、リネームするものは無い。
		return nil
	}

	tx, err := db.Begin()
	if err != nil {
		return wrapDBErr(fmt.Errorf("rename metadata: %w", err))
	}
	defer tx.Rollback()

	// 先に移動先を空にする。中途半端に埋まった移動先が、移してくる行と衝突
	// しないようにするため。
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

	// length() と substr() はいずれも文字数で数えるため、オフセットを length()
	// から導けばマルチバイトのキーでも正しく動く。
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
