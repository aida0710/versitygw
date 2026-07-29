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
	"context"
	"crypto/md5"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/versity/versitygw/auth"
	"github.com/versity/versitygw/backend"
	"github.com/versity/versitygw/backend/meta"
	"github.com/versity/versitygw/debuglogger"
	"github.com/versity/versitygw/s3api/middlewares"
	"github.com/versity/versitygw/s3api/utils"
	"github.com/versity/versitygw/s3err"
	"github.com/versity/versitygw/s3response"
)

// CreateMultipartUpload は posix バックエンドと同じ手順でアップロードを準備し、
// さらに part の書き込み先となるステージングファイルを作る。
func (l *Lustre) CreateMultipartUpload(ctx context.Context, mpu s3response.CreateMultipartUploadInput) (s3response.InitiateMultipartUploadResult, error) {
	res, err := l.Posix.CreateMultipartUpload(ctx, mpu)
	if err != nil || !l.directMultipart {
		return res, err
	}

	updir := filepath.Join(*mpu.Bucket, uploadDir(*mpu.Key, res.UploadId))
	if err := l.initStaging(updir); err != nil {
		// part を書き込めないアップロードを残さない。
		_ = l.Posix.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
			Bucket:   mpu.Bucket,
			Key:      mpu.Key,
			UploadId: &res.UploadId,
		})
		return s3response.InitiateMultipartUploadResult{}, err
	}

	return res, nil
}

// initStaging は全 part のペイロードを保持するスパースファイルを作り、この
// アップロードがどの part サイズでレイアウトされるかを記録する。
func (l *Lustre) initStaging(updir string) error {
	f, err := os.OpenFile(stagingPath(updir), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		return fmt.Errorf("create staging file: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("create staging file: %w", err)
	}

	return writeSlotSize(updir, l.partSize)
}

// uploadSlotSize は進行中のアップロードがレイアウトされた part サイズを返し、
// それが現在の設定値と一致しない場合は操作を拒否する。part の配置はこの値で
// 決まるため、別の値のまま続行すると part が誤ったオフセットに置かれる。
func (l *Lustre) uploadSlotSize(updir string) (int64, error) {
	slot, err := readSlotSize(updir)
	if err != nil {
		return 0, err
	}

	if slot != l.partSize {
		return 0, s3err.APIError{
			Code: "InvalidRequest",
			Description: fmt.Sprintf("This upload was created for a part size of %d bytes, but the gateway is configured for %d. Abort the upload and start it again.",
				slot, l.partSize),
			HTTPStatusCode: http.StatusBadRequest,
		}
	}

	return slot, nil
}

// partWriter は part のボディをステージングファイル内の該当領域へ流し込む。
// posix の一時ファイルと同様に、宣言された Content-Length を超える書き込みを
// 拒否する。
type partWriter struct {
	f      *os.File
	off    int64
	remain int64
}

func (w *partWriter) Write(b []byte) (int, error) {
	if int64(len(b)) > w.remain {
		return 0, fmt.Errorf("write exceeds content length %v", w.remain)
	}

	n, err := w.f.WriteAt(b, w.off)
	w.off += int64(n)
	w.remain -= int64(n)
	return n, err
}

func (w *partWriter) Close() error { return w.f.Close() }

// readFromBufSize は ReadFrom が使う内部バッファのサイズである。
// io.Copy の既定バッファ (32KiB) のままだと、大きな part の転送が
// 何百回もの小さな WriteAt(pwrite) syscall に分解されてしまう。CPU
// プロファイルで確認したところ、これがサーバ側 CPU 時間の過半数を
// 占めていた。生の dd/nc が使う 1MiB ブロックに合わせておく。
const readFromBufSize = 1024 * 1024

// ReadFrom は io.ReaderFrom を実装する。io.Copy は宛先がこのインター
// フェースを実装していれば、既定の 32KiB 固定バッファのコピーループに
// フォールバックせず、こちらを呼ぶ。内部で大きなバッファを使うことで
// WriteAt の呼び出し回数(ひいては pwrite syscall の回数)を大幅に
// 減らせる。remain の超過チェックは既存の Write に委譲する。
func (w *partWriter) ReadFrom(r io.Reader) (int64, error) {
	buf := make([]byte, readFromBufSize)
	var written int64
	for {
		nr, er := r.Read(buf)
		if nr > 0 {
			nw, ew := w.Write(buf[:nr])
			written += int64(nw)
			if ew != nil {
				return written, ew
			}
		}
		if er != nil {
			if er == io.EOF {
				return written, nil
			}
			return written, er
		}
	}
}

// openPartWriter は part をステージングファイル内の所定のオフセットへ配置し、
// そのペイロード用の Writer を返す。
//
// part N は (N-1) * 設定 part サイズ から始まる領域を占めるので、設定値より
// 大きい part は隣の領域を侵すため許可しない。逆に小さい part はここでは許可
// する。最終 part は短くて当然であり、この時点ではどれが最終 part かを判断
// できないためで、その判定は完了処理で行う。
func (l *Lustre) openPartWriter(updir string, part int32, length int64) (*partWriter, error) {
	slot, err := l.uploadSlotSize(updir)
	if err != nil {
		return nil, err
	}

	if length > slot {
		debuglogger.Logf("lustre: rejecting part %v of %v: %v bytes exceeds the configured part size of %v",
			part, updir, length, slot)
		return nil, s3err.GetEntityTooLargeErr(length, slot)
	}

	f, err := os.OpenFile(stagingPath(updir), os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("open staging file: %w", err)
	}

	return &partWriter{f: f, off: slotOffset(slot, part), remain: length}, nil
}

// markPart は part を可視化する。列挙時にサイズと更新時刻を与えるスパースな
// マーカーを作る処理で、part アップロードの最終ステップに置いてある。これに
// より、マーカーはペイロードがディスクに載った後にのみ現れる。
func markPart(updir string, part int32, length int64) error {
	name := partMarker(updir, part)

	f, err := os.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("create part marker: %w", err)
	}
	defer f.Close()

	if err := f.Truncate(length); err != nil {
		return fmt.Errorf("size part marker: %w", err)
	}

	return nil
}

// UploadPart は part を、最終オブジェクト内で占めることになるステージング
// ファイル上の領域へ直接書き込む。
func (l *Lustre) UploadPart(ctx context.Context, input *s3.UploadPartInput) (*s3.UploadPartOutput, error) {
	if !l.directMultipart {
		return l.Posix.UploadPart(ctx, input)
	}

	release, err := l.acquireActionSlot(ctx)
	if err != nil {
		return nil, err
	}
	defer release()

	if input.Key == nil {
		return nil, s3err.GetAPIError(s3err.ErrNoSuchKey)
	}

	bucket := *input.Bucket
	object := *input.Key
	uploadID := *input.UploadId
	part := *input.PartNumber

	length := int64(0)
	if input.ContentLength != nil {
		length = *input.ContentLength
	}
	r := input.Body

	if _, err := os.Stat(bucket); errors.Is(err, fs.ErrNotExist) {
		return nil, s3err.GetBucketErr(s3err.ErrNoSuchBucket, bucket)
	} else if err != nil {
		return nil, fmt.Errorf("stat bucket: %w", err)
	}

	mpPath := uploadDir(object, uploadID)
	updir := filepath.Join(bucket, mpPath)

	if _, err := os.Stat(updir); errors.Is(err, fs.ErrNotExist) {
		return nil, s3err.GetNoSuchUploadErr(uploadID)
	} else if err != nil {
		return nil, fmt.Errorf("stat uploadid: %w", err)
	}

	partPath := filepath.Join(mpPath, fmt.Sprint(part))

	w, err := l.openPartWriter(updir, part, length)
	if err != nil {
		if errors.Is(err, syscall.EDQUOT) {
			drainBody(r)
			return nil, s3err.GetAPIError(s3err.ErrQuotaExceeded)
		}
		if errors.Is(err, syscall.ENOSPC) {
			drainBody(r)
			return nil, s3err.GetAPIError(s3err.ErrNoSpaceLeftOnDevice)
		}
		return nil, err
	}
	defer w.Close()

	hash := md5.New()
	tr := io.TeeReader(r, hash)

	chRdr, chunkUpload := input.Body.(middlewares.ChecksumReader)
	isTrailingChecksum := chunkUpload && chRdr.Algorithm() != ""

	// クライアント指定のチェックサムアルゴリズム。chunk アップロードまたは
	// リクエストヘッダのいずれかで渡される
	var inputChAlgo utils.HashType
	// リクエストヘッダで指定されたクライアント側のチェックサム値
	var inputSum string

	if !isTrailingChecksum {
		for _, config := range []hashConfig{
			{input.ChecksumCRC32, utils.HashTypeCRC32},
			{input.ChecksumCRC32C, utils.HashTypeCRC32C},
			{input.ChecksumSHA1, utils.HashTypeSha1},
			{input.ChecksumSHA256, utils.HashTypeSha256},
			{input.ChecksumCRC64NVME, utils.HashTypeCRC64NVME},
			{input.ChecksumSHA512, utils.HashTypeSha512},
			{input.ChecksumMD5, utils.HashTypeMd5},
			{input.ChecksumXXHASH64, utils.HashTypeXXHASH64},
			{input.ChecksumXXHASH3, utils.HashTypeXXHASH3},
			{input.ChecksumXXHASH128, utils.HashTypeXXHASH128},
		} {
			if config.value != nil {
				inputChAlgo = config.hashType
				inputSum = *config.value
				break
			}
		}
	} else {
		inputChAlgo = utils.HashType(chRdr.Algorithm())
	}

	exposeChecksum := inputChAlgo != ""

	checksums, err := l.retrieveChecksums(nil, bucket, mpPath)
	if err != nil && !errors.Is(err, meta.ErrNoSuchKey) {
		return nil, fmt.Errorf("retrieve mp checksum: %w", err)
	}

	// part にチェックサムが無いが、マルチパート開始時には指定されており、かつ
	// チェックサムタイプが 'COMPOSITE' の場合は不一致エラーを返す
	if inputChAlgo == "" && checksums.Type == types.ChecksumTypeComposite {
		return nil, s3err.GetChecksumTypeMismatchErr(checksums.Algorithm, "null")
	}

	// 渡されたチェックサムアルゴリズムがマルチパート開始時のものと一致するか
	// を確認する
	if inputChAlgo != "" && checksums.Type != "" {
		algo := types.ChecksumAlgorithm(strings.ToUpper(string(inputChAlgo)))
		if checksums.Algorithm != algo {
			return nil, s3err.GetChecksumTypeMismatchErr(checksums.Algorithm, algo)
		}
	}

	if inputChAlgo == "" {
		// 既定は crc64nvme
		inputChAlgo = utils.HashTypeCRC64NVME
	}

	// hashRdr はクライアント指定のチェックサムを計算・検証する
	var hashRdr *utils.HashReader
	// crc64nvmeRdr は内部利用のみの part crc64nvme を計算する
	var crc64nvmeRdr *utils.HashReader

	if checksums.Type == "" {
		if inputChAlgo != utils.HashTypeCRC64NVME {
			crc64nvmeRdr, err = utils.NewHashReader(tr, "", utils.HashTypeCRC64NVME)
			if err != nil {
				return nil, fmt.Errorf("initialize crc64nvme hash reader: %w", err)
			}
			tr = crc64nvmeRdr
		}

		if !isTrailingChecksum {
			hashRdr, err = utils.NewHashReader(tr, inputSum, inputChAlgo)
			if err != nil {
				return nil, fmt.Errorf("initialize hash reader: %w", err)
			}
			tr = hashRdr
		}
	} else if !isTrailingChecksum {
		chAlgo := utils.HashType(strings.ToLower(string(checksums.Algorithm)))
		hashRdr, err = utils.NewHashReader(tr, inputSum, chAlgo)
		if err != nil {
			return nil, fmt.Errorf("initialize hash reader: %w", err)
		}
		tr = hashRdr
	}

	if _, err := io.Copy(w, tr); err != nil {
		if errors.Is(err, syscall.EDQUOT) {
			drainBody(tr)
			return nil, s3err.GetAPIError(s3err.ErrQuotaExceeded)
		}
		if errors.Is(err, syscall.ENOSPC) {
			drainBody(tr)
			return nil, s3err.GetAPIError(s3err.ErrNoSpaceLeftOnDevice)
		}
		if _, ok := err.(s3err.S3Error); ok {
			return nil, err
		}
		return nil, fmt.Errorf("write part data: %w", err)
	}

	if err := w.Close(); err != nil {
		return nil, fmt.Errorf("write part data: %w", err)
	}

	// 属性より先に part を可視化する。メタデータストアが属性を inode に置く
	// 実装の場合、属性の書き込み先となるファイルが先に存在している必要がある
	// ため。
	if err := markPart(updir, part, length); err != nil {
		return nil, err
	}

	etag := backend.GenerateEtag(hash)
	if err := l.metastore.StoreAttribute(nil, bucket, partPath, etagkey, []byte(etag)); err != nil {
		return nil, fmt.Errorf("set etag attr: %w", err)
	}

	res := &s3.UploadPartOutput{ETag: &etag}

	// マルチパート開始時にチェックサムアルゴリズムが指定されていれば保存し、
	// そうでなければ保存せずレスポンスに返すだけにする
	if checksums.Type != "" {
		checksum := s3response.Checksum{Algorithm: checksums.Algorithm}

		var sum string
		if isTrailingChecksum {
			sum = chRdr.Checksum()
		}
		if hashRdr != nil {
			sum = hashRdr.Sum()
		}

		setStoredChecksum(&checksum, checksums.Algorithm, &sum)
		setUploadPartChecksum(res, checksums.Algorithm, &sum)

		if err := l.storeChecksums(nil, bucket, partPath, checksum); err != nil {
			return nil, fmt.Errorf("store checksum: %w", err)
		}

		return res, nil
	}

	var internalCrc64NvmeSum string
	if inputChAlgo == utils.HashTypeCRC64NVME {
		if isTrailingChecksum {
			internalCrc64NvmeSum = chRdr.Checksum()
		} else {
			internalCrc64NvmeSum = hashRdr.Sum()
		}
	} else {
		internalCrc64NvmeSum = crc64nvmeRdr.Sum()
	}

	err = l.metastore.StoreAttribute(nil, bucket, partPath, partCrc64nvme,
		[]byte(internalCrc64NvmeSum))
	if err != nil {
		return nil, fmt.Errorf("store part internal crc64nvme: %w", err)
	}

	if exposeChecksum {
		var sumToReturn string
		if isTrailingChecksum {
			sumToReturn = chRdr.Checksum()
		} else {
			sumToReturn = hashRdr.Sum()
		}

		setUploadPartChecksum(res,
			types.ChecksumAlgorithm(strings.ToUpper(string(inputChAlgo))), &sumToReturn)
	}

	return res, nil
}

// UploadPartCopy は既存オブジェクトのバイト範囲を part としてコピーする。
//
// コピー自体は posix バックエンドへ委譲する。posix はペイロードを独立した part
// ファイルとして書き、etag とチェックサムを算出する。その後ペイロードを
// ステージングファイル内の所定スロットへ移し、このバックエンドが前提とする
// レイアウトを保つ。スロットへ直接流し込む UploadPart と違い、ここには配置
// すべきリクエストボディがそもそも無いため、コピー範囲を 1 回余分に読み書き
// することになる。バルク投入の経路ではないので許容している。
func (l *Lustre) UploadPartCopy(ctx context.Context, upi *s3.UploadPartCopyInput) (s3response.CopyPartResult, error) {
	if !l.directMultipart {
		return l.Posix.UploadPartCopy(ctx, upi)
	}

	bucket := *upi.Bucket
	part := *upi.PartNumber
	mpPath := uploadDir(*upi.Key, *upi.UploadId)
	updir := filepath.Join(bucket, mpPath)
	partPath := filepath.Join(mpPath, fmt.Sprint(part))

	// part サイズを見る前に、存在しないアップロードはそのように報告する。順序を
	// 逆にすると不正な uploadId がすべてレイアウトの不一致として報告される。
	if _, err := os.Stat(updir); errors.Is(err, fs.ErrNotExist) {
		return s3response.CopyPartResult{}, s3err.GetNoSuchUploadErr(*upi.UploadId)
	} else if err != nil {
		return s3response.CopyPartResult{}, fmt.Errorf("stat uploadid: %w", err)
	}

	slot, err := l.uploadSlotSize(updir)
	if err != nil {
		return s3response.CopyPartResult{}, err
	}

	res, err := l.Posix.UploadPartCopy(ctx, upi)
	if err != nil {
		return res, err
	}

	partFile := partMarker(updir, part)
	fi, err := os.Stat(partFile)
	if err != nil {
		return res, fmt.Errorf("stat copied part: %w", err)
	}

	if fi.Size() > slot {
		os.Remove(partFile)
		_ = l.metastore.DeleteAttributes(bucket, partPath)
		return s3response.CopyPartResult{}, s3err.GetEntityTooLargeErr(fi.Size(), slot)
	}

	// 属性がまだペイロードファイルに紐づいているうちに読み出す。属性を inode に
	// 置くストアでも失われないようにするため。
	saved := make(map[string][]byte)
	for _, attr := range []string{etagkey, checksumsKey, partCrc64nvme} {
		v, err := l.metastore.RetrieveAttribute(nil, bucket, partPath, attr)
		if errors.Is(err, meta.ErrNoSuchKey) {
			continue
		}
		if err != nil {
			return res, fmt.Errorf("get copied part %v attr: %w", attr, err)
		}
		saved[attr] = v
	}

	if err := relocateIntoSlot(updir, part, fi.Size(), slotOffset(slot, part)); err != nil {
		return res, err
	}

	if err := markPart(updir, part, fi.Size()); err != nil {
		return res, err
	}

	for attr, v := range saved {
		if err := l.metastore.StoreAttribute(nil, bucket, partPath, attr, v); err != nil {
			return res, fmt.Errorf("set copied part %v attr: %w", attr, err)
		}
	}

	return res, nil
}

// relocateIntoSlot は独立した part ペイロードをステージングファイル内の該当
// 領域へ移し、独立していた方を削除する。
func relocateIntoSlot(updir string, part int32, size, off int64) error {
	src, err := os.Open(partMarker(updir, part))
	if err != nil {
		return fmt.Errorf("open copied part: %w", err)
	}
	defer src.Close()

	dst, err := os.OpenFile(stagingPath(updir), os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("open staging file: %w", err)
	}
	defer dst.Close()

	w := &partWriter{f: dst, off: off, remain: size}
	if _, err := io.Copy(w, io.LimitReader(src, size)); err != nil {
		return fmt.Errorf("stage copied part: %w", err)
	}
	if err := dst.Close(); err != nil {
		return fmt.Errorf("stage copied part: %w", err)
	}

	return nil
}

// completedPart は完了処理中のアップロードに含まれる part を、実際のディスク上
// の状態と突き合わせて解決したものである。
type completedPart struct {
	number int32
	size   int64
	// wantOffset は最終オブジェクト内でこの part が位置すべきオフセット。
	wantOffset int64
	// slotOffset はステージングファイル内でペイロードが実際にある位置。
	slotOffset int64
}

// CompleteMultipartUpload は最終オブジェクトを組み立てる。全 part が占めるべき
// オフセットに収まっていればステージングファイルがそのままオブジェクトなので、
// 完了処理は truncate と rename だけで済む。収まっていない場合はレイアウトが
// 成立しないため、コピーへ退避させずエラーを返す。
func (l *Lustre) CompleteMultipartUpload(ctx context.Context, input *s3.CompleteMultipartUploadInput) (s3response.CompleteMultipartUploadResult, string, error) {
	if !l.directMultipart {
		return l.Posix.CompleteMultipartUpload(ctx, input)
	}

	var res s3response.CompleteMultipartUploadResult

	release, err := l.acquireActionSlot(ctx)
	if err != nil {
		return res, "", err
	}
	defer release()

	acct, ok := ctx.Value("account").(auth.Account)
	if !ok {
		acct = auth.Account{}
	}

	if input.Key == nil {
		return res, "", s3err.GetAPIError(s3err.ErrNoSuchKey)
	}
	if input.UploadId == nil {
		return res, "", s3err.GetAPIError(s3err.ErrNoSuchUpload)
	}
	if input.MultipartUpload == nil {
		return res, "", s3err.GetAPIError(s3err.ErrMalformedXML)
	}

	bucket := *input.Bucket
	object := *input.Key
	uploadID := *input.UploadId
	parts := input.MultipartUpload.Parts

	if _, err := os.Stat(bucket); errors.Is(err, fs.ErrNotExist) {
		return res, "", s3err.GetBucketErr(s3err.ErrNoSuchBucket, bucket)
	} else if err != nil {
		return res, "", fmt.Errorf("stat bucket: %w", err)
	}

	// そのまま返す。不正な part リストは対応する S3 エラーとして表面化するので、
	// ラップすると 400 が 500 になってしまう。
	s3MD5, err := backend.GetMultipartMD5(parts)
	if err != nil {
		return res, "", err
	}

	objRelDir := objdir(object)
	uploadRel := filepath.Join(objRelDir, uploadID)
	activeName := fmt.Sprintf("%s.%s%s", uploadID, strings.Trim(s3MD5, "\""), inProgressSuffix)
	activeRel := filepath.Join(objRelDir, activeName)

	// 同一アップロードに対する並行 complete を直列化しているのがこのディレクトリ
	// rename である。rename に成功するのは 1 つだけになる。
	err = os.Rename(filepath.Join(bucket, uploadRel), filepath.Join(bucket, activeRel))
	if errors.Is(err, fs.ErrNotExist) {
		return l.completeIdempotent(bucket, object, uploadID, activeRel, s3MD5)
	}
	if err != nil {
		return res, "", fmt.Errorf("mark upload in progress: %w", err)
	}

	if err := l.metastore.RenameObject(bucket, uploadRel, activeRel); err != nil {
		os.Rename(filepath.Join(bucket, activeRel), filepath.Join(bucket, uploadRel))
		return res, "", fmt.Errorf("rename upload metadata: %w", err)
	}

	// 以降で失敗した場合はアップロードを元に戻し、クライアントが再試行できる
	// ようにする。いずれも best effort で、成功パスでアップロードが片付いた後は
	// 何もしない。
	completed := false
	defer func() {
		if completed {
			return
		}
		if err := os.Rename(filepath.Join(bucket, activeRel), filepath.Join(bucket, uploadRel)); err != nil {
			return
		}
		_ = l.metastore.RenameObject(bucket, activeRel, uploadRel)
	}()

	b, err := l.metastore.RetrieveAttribute(nil, bucket, object, etagkey)
	if err != nil && !errors.Is(err, meta.ErrNoSuchKey) {
		return res, "", fmt.Errorf("get object etag: %w", err)
	}
	objExists := err == nil
	if err := backend.EvaluateObjectPutPreconditions(string(b),
		input.IfMatch, input.IfNoneMatch, objExists); err != nil {
		return res, "", err
	}

	checksums, err := l.retrieveChecksums(nil, bucket, activeRel)
	if err != nil && !errors.Is(err, meta.ErrNoSuchKey) {
		return res, "", fmt.Errorf("get mp checksums: %w", err)
	}

	// ChecksumType は CreateMultipartUpload で指定されたものと一致する必要がある
	if input.ChecksumType != "" && checksums.Type != input.ChecksumType {
		checksumType := checksums.Type
		if checksumType == "" {
			checksumType = types.ChecksumType("null")
		}
		return res, "", s3err.GetChecksumTypeMismatchOnMpErr(checksumType)
	}

	// mpChecksumType はマルチパートアップロードのチェックサムタイプを保持する
	mpChecksumType := checksums.Type

	// チェックサムのタイプ/アルゴリズムの既定値は FULL_OBJECT(crc64nvme)
	if checksums.Type == "" {
		checksums.Type = types.ChecksumTypeFullObject
		checksums.Algorithm = types.ChecksumAlgorithmCrc64nvme
	}

	var compositeChecksumRdr *utils.CompositeChecksumReader
	if checksums.Type == types.ChecksumTypeComposite {
		compositeChecksumRdr, err = utils.NewCompositeChecksumReader(
			utils.HashType(strings.ToLower(string(checksums.Algorithm))))
		if err != nil {
			return res, "", fmt.Errorf("initialize composite checksum reader: %w", err)
		}
	}

	slot, err := readSlotSize(filepath.Join(bucket, activeRel))
	if err != nil {
		return res, "", err
	}

	resolved, totalsize, partSizes, composableCsum, err := l.resolveParts(
		bucket, activeRel, uploadID, slot, parts, checksums, mpChecksumType,
		compositeChecksumRdr)
	if err != nil {
		return res, "", err
	}

	if input.MpuObjectSize != nil && totalsize != *input.MpuObjectSize {
		return res, "", s3err.GetIncorrectMpObjectSizeErr(totalsize, *input.MpuObjectSize)
	}

	// 最終的なチェックサム値を算出する。
	var value string
	switch checksums.Type {
	case types.ChecksumTypeComposite:
		value = fmt.Sprintf("%s-%v", compositeChecksumRdr.Sum(), len(parts))
	case types.ChecksumTypeFullObject:
		value = composableCsum
	}

	gotSum := completionChecksum(input, &checksums, &res, value)

	// クライアントが提示したチェックサムと算出値が一致するか確認する。
	if mpChecksumType != "" && gotSum != nil {
		s := *gotSum
		if checksums.Type == types.ChecksumTypeComposite && !strings.Contains(s, "-") {
			s = fmt.Sprintf("%s-%v", s, len(parts))
		}
		if s != value {
			return res, "", s3err.GetChecksumBadDigestErr(checksums.Algorithm)
		}
	}

	assembledRel, err := l.assemble(bucket, activeRel, object, resolved, totalsize)
	if err != nil {
		return res, "", err
	}

	if err := l.finishObject(bucket, object, activeRel, assembledRel, acct,
		checksums, s3MD5, uploadID, partSizes); err != nil {
		return res, "", err
	}

	completed = true

	// アップロードの残骸を片付ける。ステージングファイルは rename で移動済み
	// なので、残っているのは管理用のファイルだけである。
	_ = os.RemoveAll(filepath.Join(bucket, activeRel))
	_ = os.Remove(filepath.Join(bucket, objRelDir))
	_ = l.metastore.DeleteAttributes(bucket, activeRel)

	res.Bucket = &bucket
	res.Key = &object
	res.ETag = &s3MD5
	if mpChecksumType != "" {
		res.ChecksumType = &checksums.Type
	}

	return res, "", nil
}

// completeIdempotent はアップロードディレクトリの獲得競争に敗れた complete
// 要求をどう扱うかを決める。並行する別要求に負けた場合と、同じクライアントが
// レスポンスを受け取れなかった以前の試行に負けた場合がある。
func (l *Lustre) completeIdempotent(bucket, object, uploadID, activeRel, s3MD5 string) (s3response.CompleteMultipartUploadResult, string, error) {
	var none s3response.CompleteMultipartUploadResult

	// このアップロードを完了させる要求はいずれも同じ etag を算出するので、実際
	// の作業をしていなくても成功を返して差し支えない。
	success := s3response.CompleteMultipartUploadResult{
		Bucket: &bucket,
		Key:    &object,
		ETag:   &s3MD5,
	}

	// 別の要求がアップロードを獲得し、まだ組み立て中である。
	if _, err := os.Stat(filepath.Join(bucket, activeRel)); err == nil {
		return success, "", nil
	}

	// 並行する要求が既に完了させたため、アップロードは存在しない。
	if _, err := os.Stat(filepath.Join(bucket, object)); err == nil {
		return success, "", nil
	}

	// 上の stat は、勝った要求がオブジェクトを配置する処理と競合して外すことが
	// あるので、オブジェクトに記録された uploadId を見て判断し直す。
	b, err := l.metastore.RetrieveAttribute(nil, bucket, object, mpMetaKey)
	if err != nil {
		return none, "", s3err.GetAPIError(s3err.ErrNoSuchUpload)
	}

	mpMeta, err := backend.UnmarshalMpUploadMetadata(b, false)
	if err != nil {
		return none, "", fmt.Errorf("parse object multipart metadata: %w", err)
	}

	// そのオブジェクトは別のアップロードが上書きした結果かもしれない。
	if mpMeta.UploadID != uploadID {
		return none, "", s3err.GetAPIError(s3err.ErrNoSuchUpload)
	}

	return success, "", nil
}

// resolveParts は要求された part をディスク上の状態と突き合わせて検証し、
// それぞれが現在どこにあるかを求める。
func (l *Lustre) resolveParts(bucket, activeRel, uploadID string, slot int64,
	parts []types.CompletedPart, checksums s3response.Checksum,
	mpChecksumType types.ChecksumType, compositeChecksumRdr *utils.CompositeChecksumReader,
) (resolved []completedPart, totalsize int64, partSizes []int64, composableCsum string, err error) {
	last := len(parts) - 1

	// 初期値は partNumber の下限である 0
	var partNumber int32
	for i, part := range parts {
		if part.PartNumber == nil {
			return nil, 0, nil, "", s3err.GetAPIError(s3err.ErrMalformedXML)
		}
		if *part.PartNumber < 1 {
			return nil, 0, nil, "", s3err.GetInvalidArgumentErr(
				s3err.InvalidArgCompleteMpPartNumber, fmt.Sprint(*part.PartNumber))
		}
		if *part.PartNumber <= partNumber {
			return nil, 0, nil, "", s3err.GetAPIError(s3err.ErrInvalidPartOrder)
		}
		partNumber = *part.PartNumber

		partObjPath := filepath.Join(activeRel, fmt.Sprint(partNumber))
		fi, err := os.Lstat(filepath.Join(bucket, partObjPath))
		if err != nil {
			return nil, 0, nil, "", s3err.GetInvalidPartErr(uploadID, partNumber,
				backend.GetStringFromPtr(part.ETag))
		}

		resolved = append(resolved, completedPart{
			number:     partNumber,
			size:       fi.Size(),
			wantOffset: totalsize,
			slotOffset: slotOffset(slot, partNumber),
		})

		totalsize += fi.Size()
		partSizes = append(partSizes, totalsize)

		// 最終 part を除く全 part は許容最小サイズ (5 MiB) 以上である必要が
		// ある
		if i < last && fi.Size() < backend.MinPartSize {
			return nil, 0, nil, "", s3err.GetEntityTooSmallErr(fi.Size(), backend.MinPartSize)
		}

		b, err := l.metastore.RetrieveAttribute(nil, bucket, partObjPath, etagkey)
		etag := string(b)
		if err != nil {
			etag = ""
		}
		if part.ETag == nil {
			return nil, 0, nil, "", s3err.GetAPIError(s3err.ErrMalformedXML)
		}
		if !backend.AreEtagsSame(etag, *part.ETag) {
			return nil, 0, nil, "", s3err.GetInvalidPartErr(uploadID, partNumber, etag)
		}

		partChecksum, err := l.retrieveChecksums(nil, bucket, partObjPath)
		if err != nil && !errors.Is(err, meta.ErrNoSuchKey) {
			return nil, 0, nil, "", fmt.Errorf("get part checksum: %w", err)
		}

		if err := validatePartChecksum(partChecksum, part, uploadID); err != nil {
			return nil, 0, nil, "", err
		}

		switch checksums.Type {
		case types.ChecksumTypeFullObject:
			var pcs string
			if mpChecksumType != "" {
				pcs = getPartChecksum(checksums.Algorithm, part)
			} else {
				crc64nvme, err := l.metastore.RetrieveAttribute(nil, bucket, partObjPath, partCrc64nvme)
				if err != nil {
					return nil, 0, nil, "", fmt.Errorf("retrieve part internal crc64nvme: %w", err)
				}
				pcs = string(crc64nvme)
			}
			if i == 0 {
				composableCsum = pcs
			} else {
				composableCsum, err = utils.AddCRCChecksum(checksums.Algorithm,
					composableCsum, pcs, fi.Size())
				if err != nil {
					return nil, 0, nil, "", fmt.Errorf("add part %v checksum: %w", partNumber, err)
				}
			}
		case types.ChecksumTypeComposite:
			if err := compositeChecksumRdr.Process(getPartChecksum(checksums.Algorithm, part)); err != nil {
				return nil, 0, nil, "", fmt.Errorf("process %v part checksum: %w", partNumber, err)
			}
		}
	}

	return resolved, totalsize, partSizes, composableCsum, nil
}

// misplacedPart は最終オブジェクト内で占めるべきオフセットに収まっていない
// 最初の part と、そのような part が存在したかどうかを返す。
//
// 最終 part を除く全 part が設定 part サイズちょうどであり、かつ part 番号が
// 1 から歯抜けなく並んでいる必要がある。それ以外の場合、part はステージング
// ファイル上の誤った位置にあることになる。
func misplacedPart(parts []completedPart) (completedPart, bool) {
	for _, p := range parts {
		if p.slotOffset != p.wantOffset {
			return p, true
		}
	}
	return completedPart{}, false
}

// assemble はステージングファイルを最終オブジェクトに仕立て、そのバケット相対
// パスを返す。part は既にあるべき位置に収まっているので、処理は truncate だけ
// である。
func (l *Lustre) assemble(bucket, activeRel, object string, parts []completedPart, totalsize int64) (string, error) {
	if p, bad := misplacedPart(parts); bad {
		debuglogger.Logf("lustre: rejecting complete of %v/%v: part %v is %v bytes at offset %v but the layout needs it at %v",
			bucket, object, p.number, p.size, p.slotOffset, p.wantOffset)

		return "", s3err.APIError{
			Code: "InvalidRequest",
			Description: fmt.Sprintf("This gateway is configured for a fixed multipart part size of %d bytes. Every part except the last must be exactly that size, and the part numbers must run from 1 without gaps. Part %d is %d bytes and does not fit that layout.",
				l.partSize, p.number, p.size),
			HTTPStatusCode: http.StatusBadRequest,
		}
	}

	staging := stagingPath(activeRel)
	if err := os.Truncate(filepath.Join(bucket, staging), totalsize); err != nil {
		return "", fmt.Errorf("truncate staging file: %w", err)
	}

	return staging, nil
}

// finishObject は組み立て済みファイルへアップロードの属性を写し、名前空間へ
// 移動させ、メタデータも併せて移す。
func (l *Lustre) finishObject(bucket, object, activeRel, assembledRel string,
	acct auth.Account, checksums s3response.Checksum, s3MD5, uploadID string,
	partSizes []int64,
) error {
	// 属性は組み立て済みファイルに対して書き、その後オブジェクトへ移す。これに
	// より inode に属性を置くストアとパスをキーにするストアの双方で正しく動作し、
	// かつ属性の無いオブジェクトが名前空間に現れることもない。
	for _, attr := range []string{
		metadataHdr, contentTypeHdr, contentEncHdr, contentDispHdr,
		contentLangHdr, cacheCtrlHdr, expiresHdr, websiteRedirectHdr,
		tagHdr, objectLegalHoldKey, objectRetentionKey,
	} {
		v, err := l.metastore.RetrieveAttribute(nil, bucket, activeRel, attr)
		if errors.Is(err, meta.ErrNoSuchKey) {
			continue
		}
		if err != nil {
			return fmt.Errorf("get upload %v attr: %w", attr, err)
		}
		if err := l.metastore.StoreAttribute(nil, bucket, assembledRel, attr, v); err != nil {
			return fmt.Errorf("set object %v attr: %w", attr, err)
		}
	}

	if err := l.storeChecksums(nil, bucket, assembledRel, checksums); err != nil {
		return fmt.Errorf("store checksums: %w", err)
	}

	if err := l.metastore.StoreAttribute(nil, bucket, assembledRel, etagkey, []byte(s3MD5)); err != nil {
		return fmt.Errorf("set etag attr: %w", err)
	}

	mpMetaBytes, err := backend.MarshalMpUploadMetadata(backend.MpUploadMetadata{
		UploadID: uploadID,
		Parts:    partSizes,
	}, false)
	if err != nil {
		return fmt.Errorf("marshal multipart metadata: %w", err)
	}
	if err := l.metastore.StoreAttribute(nil, bucket, assembledRel, mpMetaKey, mpMetaBytes); err != nil {
		return fmt.Errorf("set multipart metadata attr: %w", err)
	}

	objPath := filepath.Join(bucket, object)
	uid, gid, doChown := l.getChownIDs(acct)
	if err := backend.MkdirAll(filepath.Dir(objPath), uid, gid, doChown, l.newDirPerm); err != nil {
		return fmt.Errorf("create object parent dir: %w", err)
	}

	src := filepath.Join(bucket, assembledRel)
	if err := os.Chmod(src, 0644); err != nil {
		return fmt.Errorf("set object permissions: %w", err)
	}
	if doChown {
		if err := os.Chown(src, uid, gid); err != nil {
			return fmt.Errorf("chown object: %w", err)
		}
	}

	if err := os.Rename(src, objPath); err != nil {
		return fmt.Errorf("move object into namespace: %w", err)
	}

	if err := l.metastore.RenameObject(bucket, assembledRel, object); err != nil {
		return fmt.Errorf("move object metadata: %w", err)
	}

	return nil
}

// completionChecksum は算出値を保存用チェックサムとレスポンスの両方へ記録し、
// クライアントが提示していた値があればそれを返す。
func completionChecksum(input *s3.CompleteMultipartUploadInput,
	checksums *s3response.Checksum, res *s3response.CompleteMultipartUploadResult,
	value string,
) *string {
	var gotSum *string

	switch checksums.Algorithm {
	case types.ChecksumAlgorithmCrc32:
		gotSum = input.ChecksumCRC32
		checksums.CRC32 = &value
		res.ChecksumCRC32 = &value
	case types.ChecksumAlgorithmCrc32c:
		gotSum = input.ChecksumCRC32C
		checksums.CRC32C = &value
		res.ChecksumCRC32C = &value
	case types.ChecksumAlgorithmSha1:
		gotSum = input.ChecksumSHA1
		checksums.SHA1 = &value
		res.ChecksumSHA1 = &value
	case types.ChecksumAlgorithmSha256:
		gotSum = input.ChecksumSHA256
		checksums.SHA256 = &value
		res.ChecksumSHA256 = &value
	case types.ChecksumAlgorithmCrc64nvme:
		gotSum = input.ChecksumCRC64NVME
		checksums.CRC64NVME = &value
		res.ChecksumCRC64NVME = &value
	case types.ChecksumAlgorithmSha512:
		gotSum = input.ChecksumSHA512
		checksums.SHA512 = &value
		res.ChecksumSHA512 = &value
	case types.ChecksumAlgorithmMd5:
		gotSum = input.ChecksumMD5
		checksums.MD5 = &value
		res.ChecksumMD5 = &value
	case types.ChecksumAlgorithmXxhash64:
		gotSum = input.ChecksumXXHASH64
		checksums.XXHASH64 = &value
		res.ChecksumXXHASH64 = &value
	case types.ChecksumAlgorithmXxhash3:
		gotSum = input.ChecksumXXHASH3
		checksums.XXHASH3 = &value
		res.ChecksumXXHASH3 = &value
	case types.ChecksumAlgorithmXxhash128:
		gotSum = input.ChecksumXXHASH128
		checksums.XXHASH128 = &value
		res.ChecksumXXHASH128 = &value
	}

	return gotSum
}

// drainBody は r の残りバイトを読み捨てる。接続が閉じられる前にクライアントが
// エラーレスポンスを読めるようにするため。
func drainBody(r io.Reader) {
	if r == nil {
		return
	}
	_, _ = io.Copy(io.Discard, r)
}
