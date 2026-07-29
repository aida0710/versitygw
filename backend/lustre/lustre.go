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

// Package lustre はファイル間でブロックを共有できない並列ファイルシステム向けの
// ストレージバックエンドを提供する。
//
// posix バックエンドはマルチパートアップロードを、各 part を専用の一時ファイルへ
// 書いてから最終オブジェクトへコピーして組み立てる。reflink 対応のファイルシステム
// ならこのコピーはブロック参照の更新で済みコストが無いため、この設計は妥当である。
// 一方 Lustre はファイルを別サーバ上の OST へストライプするため copy_file_range が
// 利用できるローカルなブロック共有が存在せず、マルチパートアップロードの全バイトが
// ディスクへ 2 回書かれる。
//
// このバックエンドは 2 回目の書き込みを無くす。アップロードごとに 1 本のスパースな
// ステージングファイルを用意し、各 part を最終オブジェクト内で占める領域へ直接
// 書き込むため、完了処理はコピーではなく truncate と rename で済む。マルチパート
// 以外はすべて posix バックエンドからそのまま継承している。
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

// Lustre は完了時に part のデータをコピーしないマルチパート実装を持つ posix
// バックエンドである。
type Lustre struct {
	*posix.Posix

	// metastore は埋め込んだ posix バックエンドに渡したものと同じストアである。
	// posix のフィールドは非公開なので、ここのマルチパート実装が属性を読み書き
	// するには自前の参照が要る。
	metastore meta.MetadataStorer

	rootdir string

	// partSize はステージングファイル内で part を配置する間隔である。この
	// サイズに従わない part は拒否されるため、直書きモードでは必須となる。
	partSize int64

	// directMultipart は part をステージングファイルへ直接書く動作を有効にする。
	// 無効にすると posix と完全に同じ挙動になるので、問題がこのコードに起因する
	// のかを切り分けるのに使える。
	directMultipart bool

	// posix 側の同等のフィールドは非公開なので、ここのマルチパート実装は自前の
	// コピーを持つ。
	newDirPerm fs.FileMode
	chownuid   bool
	chowngid   bool
	euid       int
	egid       int

	// actionLimiter はこのバックエンドが自前で実装したメソッドにおける同時
	// ファイルシステム操作を制限する。posix から継承したメソッドは引き続き
	// posix 側のリミッタが担当する。
	actionLimiter *semaphore.Weighted
}

// defaultConcurrency は posix バックエンドの既定値に合わせてある。
const defaultConcurrency = 5000

// acquireActionSlot はファイルシステム負荷の高い操作を新たに開始してよくなる
// までブロックする。
func (l *Lustre) acquireActionSlot(ctx context.Context) (func(), error) {
	if err := l.actionLimiter.Acquire(ctx, 1); err != nil {
		return func() {}, fmt.Errorf("acquire action slot: %w", err)
	}
	return func() { l.actionLimiter.Release(1) }, nil
}

// getChownIDs は新規作成したファイルが属するべき uid と gid、および chown が
// そもそも必要かどうかを返す。
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

// Opts は Lustre バックエンドのオプションである。posix のオプションはそのまま
// 引き渡される。
type Opts struct {
	Posix posix.PosixOpts

	// MetaStore はメタデータストアである。posix バックエンドへ渡すものと同一の
	// インスタンスでなければならない。
	MetaStore meta.MetadataStorer

	// PartSize はマルチパートの part サイズをバイト単位で指定する。直書きモード
	// では必須で、このサイズに従わない part は拒否される。
	PartSize int64

	// DisableDirectMultipart は posix のマルチパート経路に戻す。この経路は part
	// のデータを最終オブジェクトへコピーする。
	DisableDirectMultipart bool
}

// New は rootdir を起点とする Lustre バックエンドを生成する。
func New(rootdir string, metastore meta.MetadataStorer, opts Opts) (*Lustre, error) {
	if metastore == nil {
		return nil, fmt.Errorf("a metadata storer is required")
	}

	direct := !opts.DisableDirectMultipart

	// part を最終位置へ直接書くと posix のバージョン管理機構が参照する part 単位
	// のファイルが残らず、またオブジェクトバージョンを作るヘルパーは posix
	// パッケージ内部にあり外から呼べない。
	if direct && opts.Posix.VersioningDir != "" {
		return nil, fmt.Errorf("bucket versioning is not supported with direct multipart writes, pass --disable-direct-mpu to use the copying multipart path")
	}

	// part の配置はこのサイズで決まり、異なるサイズの part は黙ってコピー結合に
	// 退避させず拒否する。よって既定値として妥当な値は存在しない。
	if direct && opts.PartSize <= 0 {
		return nil, fmt.Errorf("a multipart part size is required, pass --mpu-part-size with the part size the clients use")
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

// Shutdown はバックエンドが保持する資源を解放する。メタデータストアが解放を
// 要するものであればそれも含む。
func (l *Lustre) Shutdown() {
	l.Posix.Shutdown()

	if c, ok := l.metastore.(interface{ Close() error }); ok {
		if err := c.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "close metadata storer: %v\n", err)
		}
	}
}
