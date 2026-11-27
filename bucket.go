package kodo

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"

	"github.com/qiniu/go-sdk/v7/storage"
)

// Check 探活：查询桶信息（GetBucketInfo）。
func (b *Bucket) Check(ctx context.Context) (err error) {
	defer recoverToError(&err, "Check")
	if err := b.readyManager(); err != nil {
		return err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	// 七牛 v1 SDK 的 GetBucketInfo 不接受 ctx，这里仅做 ctx 预检查
	if err := ctx.Err(); err != nil {
		return err
	}
	_, err = b.bucketManager.GetBucketInfo(b.name)
	return wrapOp("Check", b.name, "", err)
}

// Put 是 Upload 的别名：上传字节切片。
func (b *Bucket) Put(ctx context.Context, key string, data []byte, contentType string) (err error) {
	defer recoverToError(&err, "Put")
	_, err = b.upload(ctx, "Put", key, bytes.NewReader(data), contentType, false, 0)
	return err
}

// UploadBytes 便捷方法：直接上传字节切片。
func (b *Bucket) UploadBytes(ctx context.Context, key string, data []byte, contentType string) (err error) {
	defer recoverToError(&err, "UploadBytes")
	_, err = b.upload(ctx, "UploadBytes", key, bytes.NewReader(data), contentType, false, 0)
	return err
}

// Upload 上传一个 Reader 内容到指定 key。
//
// contentType 为空时使用桶的默认类型，再为空时按扩展名推断，仍为空则 application/octet-stream。
//
// 上传通道选择：
//   - 已知长度且未超过 resumeThreshold：表单上传（FormUploader）；
//   - 已知长度且超过阈值：分片断点续传上传（ResumeUploader）；
//   - 未知长度：PutWithoutSize 流式分片上传（**不把数据读进内存**）；
//   - 配置 bufferUnknownSize=true 时，未知长度流会先缓冲到 maxUploadMemory 再走表单上传。
func (b *Bucket) Upload(ctx context.Context, key string, body io.Reader, contentType string) (err error) {
	defer recoverToError(&err, "Upload")
	_, err = b.upload(ctx, "Upload", key, body, contentType, false, 0)
	return err
}

// UploadFile 从本地文件上传（推荐用于大文件：按需 seek 取长度，流式分片，不占内存）。
func (b *Bucket) UploadFile(ctx context.Context, key, localPath, contentType string) (err error) {
	defer recoverToError(&err, "UploadFile")
	if localPath == "" {
		return wrapErrWith(ErrInvalidKey, "localPath 不能为空")
	}
	// 先做无副作用的前置校验，避免在插件不可用 / key 非法时仍然打开磁盘文件。
	if err := b.readyUploader(); err != nil {
		return err
	}
	if err := ValidateKey(key); err != nil {
		return err
	}
	f, openErr := os.Open(localPath)
	if openErr != nil {
		return wrapOp("UploadFile", b.Name(), key, fmt.Errorf("打开本地文件失败: %w", openErr))
	}
	defer func() {
		if closeErr := f.Close(); closeErr != nil {
			logWarn("[kodo] 关闭本地文件失败: " + closeErr.Error())
		}
	}()
	info, statErr := f.Stat()
	if statErr != nil {
		return wrapOp("UploadFile", b.Name(), key, fmt.Errorf("读取本地文件信息失败: %w", statErr))
	}
	if info.IsDir() {
		return wrapOp("UploadFile", b.Name(), key, fmt.Errorf("%s 是目录，不能上传", localPath))
	}
	_, err = b.upload(ctx, "UploadFile", key, f, contentType, false, 0)
	return err
}

// upload 是 Upload/UploadMultipart/UploadFile 的共同实现。
// forceResume=true 时强制走分片（断点续传）通道。
func (b *Bucket) upload(ctx context.Context, op, key string, body io.Reader, contentType string, forceResume bool, partSize int64) (*storage.PutRet, error) {
	if err := b.readyUploader(); err != nil {
		return nil, err
	}
	if err := ValidateKey(key); err != nil {
		return nil, err
	}
	if body == nil || isNilValue(body) {
		// isNilValue 覆盖「接口非 nil 但内部指针为 nil」（如 (*bytes.Reader)(nil)），
		// 这类值直接使用会在 SDK 内部解引用 nil 指针 panic。
		return nil, ErrNilReader
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if contentType == "" {
		contentType = b.resolveContentType(key)
	}
	upToken, err := b.uploadToken(key)
	if err != nil {
		return nil, err
	}
	src, err := uploadSource(body)
	if err != nil {
		return nil, err
	}
	// 上传 body 会被 net/http 的 writeLoop 协程（分片上传还有 SDK 的 worker 池）读取，
	// 那些协程里的 panic 无法被本函数的 defer recover 兜住，会直接终止进程。
	// 因此交给 SDK 的读取入口一律换成 panic 安全适配器。
	if !src.Sized && b.cfg != nil && b.cfg.BufferUnknownSize {
		data, readErr := readAllBounded(&safeReader{r: src.Reader}, b.cfg.MaxUploadMemory)
		if readErr != nil {
			return nil, readErr
		}
		src = uploadInput{Reader: bytes.NewReader(data), Size: int64(len(data)), Sized: true, FromStart: true}
	}
	// 必须在 src 被替换之后再包装，否则会用到已消费完的旧 reader
	stream := &safeReader{r: src.Reader}
	chunkSize := b.resolveChunkSize(partSize)
	ret := &storage.PutRet{}
	switch {
	case src.Sized && !forceResume && !b.shouldResume(src.Size):
		putExtra := &storage.PutExtra{MimeType: contentType}
		err = b.formUploader.Put(ctx, ret, upToken, key, stream, src.Size, putExtra)
	case src.Sized && src.FromStart:
		rputExtra := &storage.RputExtra{MimeType: contentType, ChunkSize: chunkSize}
		if readerAt, ok := src.Reader.(io.ReaderAt); ok {
			err = b.resumeUploader.Put(ctx, ret, upToken, key, &safeReaderAt{r: readerAt}, src.Size, rputExtra)
		} else {
			// 知道长度但没有 ReaderAt：仍可流式上传，SDK 会边读边分片
			err = b.resumeUploader.PutWithoutSize(ctx, ret, upToken, key, stream, rputExtra)
		}
	default:
		// 长度未知，或已知长度但读取位置不在流起点：
		// 一律流式分片上传，避免 io.ReadAll 造成 OOM，也避免按长度截取导致内容错位
		rputExtra := &storage.RputExtra{MimeType: contentType, ChunkSize: chunkSize}
		err = b.resumeUploader.PutWithoutSize(ctx, ret, upToken, key, stream, rputExtra)
	}
	if err != nil {
		return nil, wrapOp(op, b.name, key, err)
	}
	return ret, nil
}

// UploadMultipart 分片（断点续传）上传，返回七牛返回的文件 Hash（类 ETag）。
//
// partSize 为分片大小（字节）：>0 时生效；<=0 时使用配置 chunkSize；仍为 0 时由 SDK
// 使用默认值（4MB）。七牛服务端对分片偏移有对齐要求，建议使用 4MB 的倍数。
func (b *Bucket) UploadMultipart(ctx context.Context, key, contentType string, body io.Reader, partSize int64) (hash string, err error) {
	defer recoverToError(&err, "UploadMultipart")
	ret, uploadErr := b.upload(ctx, "UploadMultipart", key, body, contentType, true, partSize)
	if uploadErr != nil {
		return "", uploadErr
	}
	if ret == nil {
		return "", nil
	}
	return ret.Hash, nil
}

// Download 将指定 key 的内容写入 w（io.WriterAt）。
//
// 通过该桶绑定的访问域名发起 HTTP 下载：私有空间使用带下载凭证的 URL，公有空间直接访问。
// 不设 HTTP 总超时（旧实现的 5 分钟总超时会使大文件下载必然失败），总时长由 ctx 控制；
// 等待响应头的时长由 responseHeaderTimeout 约束。下载失败会重试（幂等：从偏移 0 重写）。
//
// 体积上限为 maxDownloadSize（0 表示仅受 8GiB 硬上限约束），超限返回 ErrDownloadTooLarge。
//
// 重试与目标清理：每次尝试都从偏移 0 重写。若上一次尝试已写入部分数据，重试前会对
// 实现了 Reset() 的缓冲（writeAtBuf）或 Truncate(int64) error 的写入端（如 *os.File）
// 做清理，避免对象在两次尝试之间被覆盖变短后留下旧尾部数据。
//
// **注意**：Truncate 作用于文件本身。若多个 goroutine 用同一个 *os.File 并发下载不同
// 对象（各自 WriteAt 不同区间），其中一路触发重试时会清空整份文件，破坏其它写入结果。
// 因此共享文件句柄并发下载时，请为每个下载使用独立文件（或自行包装出不含 Truncate
// 的 WriterAt）。单文件单下载（最常见用法）不受影响。
func (b *Bucket) Download(ctx context.Context, key string, w io.WriterAt) (err error) {
	defer recoverToError(&err, "Download")
	// limit=0 表示使用流式下载上限（8GiB），不约束调用方磁盘空间
	return b.download(ctx, "Download", key, w, 0)
}

// download 是 Download / DownloadBytes 的共用实现。
// limit<=0 时使用配置的流式下载上限。
func (b *Bucket) download(ctx context.Context, op, key string, w io.WriterAt, limit int64) error {
	if err := b.ready(); err != nil {
		return err
	}
	if err := ValidateKey(key); err != nil {
		return err
	}
	if w == nil || isNilValue(w) {
		return ErrNilWriter
	}
	if ctx == nil {
		ctx = context.Background()
	}
	url, err := b.accessURL(key)
	if err != nil {
		return err
	}
	if limit <= 0 {
		limit = b.cfg.downloadLimit()
	}
	attempt := 0
	var lastWritten int64
	err = retryDo(ctx, b.cfg.retryPolicy(), func() error {
		if attempt > 0 && lastWritten > 0 {
			// 重试时从偏移 0 重新写：先清掉上一次的残留，避免对象变短后留下旧尾部数据
			if resetter, ok := w.(interface{ Reset() }); ok {
				resetter.Reset()
			} else if truncater, ok := w.(interface{ Truncate(int64) error }); ok {
				if truncateErr := truncater.Truncate(0); truncateErr != nil {
					return fmt.Errorf("kodo: 重试前清理下载目标失败: %w", truncateErr)
				}
			}
		}
		attempt++
		written, downloadErr := b.downloadOnce(ctx, key, url, w, limit)
		lastWritten = written
		return downloadErr
	})
	return wrapOp(op, b.name, key, err)
}

// sizeLimitError 生成「下载体积超限」错误，并同时给出两条路径的解决办法。
// 只报「超限」的文案会让调用方继续用内存下载拉大文件，最终只能靠 OOM 才发现问题。
func sizeLimitError(size, limit int64) error {
	return fmt.Errorf("%w: content-length %d > 上限 %d 字节；"+
		"内存下载请改用 Download(io.WriterAt) 或 DownloadFile 流式落盘（或调大 maxDownloadBytes，硬上限 %d 字节）；"+
		"流式下载请调大 maxDownloadSize",
		ErrDownloadTooLarge, size, limit, HardMaxDownloadBytes)
}

// downloadOnce 执行一次下载尝试，返回本次实际写入的字节数。
// 每次尝试都从偏移 0 开始写，因此可安全重试。
func (b *Bucket) downloadOnce(ctx context.Context, key, url string, w io.WriterAt, limit int64) (written int64, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, fmt.Errorf("kodo: 构造下载请求失败: %w", err)
	}
	resp, err := b.httpClient.Do(req)
	if err != nil {
		return 0, err
	}
	if resp == nil || resp.Body == nil {
		// http.Client 正常路径保证二者非 nil；这里兜住自定义 Transport 的异常实现，
		// 避免 defer 里直接 resp.Body.Close() 造成 nil 解引用。
		return 0, fmt.Errorf("kodo: 下载 %q 失败: 空响应", key)
	}
	// bodyReadToEOF 表示 body 是否已被读到 EOF。只有这种情况才做「排空以复用连接」：
	//   - 成功读完：body 已在 EOF，排空立即返回，纯收益；
	//   - 4xx/5xx、超限、读失败：body 可能「已回头部却不再发数据」，而下载用的
	//     http.Client 刻意不设总超时（避免大文件被截断），此时排空会永久阻塞，
	//     把一次本该立即返回的错误变成一个卡死的 goroutine。直接 Close 更安全
	//     （net/http 的 earlyClose 会关闭连接，不会等待 body 读完），代价只是
	//     放弃这条连接的复用。
	bodyReadToEOF := false
	defer func() {
		if bodyReadToEOF {
			drainAndClose(resp.Body)
			return
		}
		safeCloseBody(resp.Body)
	}()
	if resp.StatusCode == http.StatusNotFound {
		return 0, fmt.Errorf("kodo: 下载 %q: %w", key, ErrObjectNotFound)
	}
	// 只有 2xx 才算「拿到了对象内容」。
	// 这里不能用「>= 400 才报错」：3xx（例如没有 Location 的重定向、304）响应体并不是
	// 对象内容，按旧判定会把重定向页/空页当作下载成功写进目标，属于静默数据错误。
	// 3xx 也不可重试（statusError.retryable 只放行 429 与 5xx）。
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return 0, &statusError{Op: "download", StatusCode: resp.StatusCode, Status: resp.Status}
	}
	if resp.ContentLength > 0 && resp.ContentLength > limit {
		return 0, sizeLimitError(resp.ContentLength, limit)
	}
	// 已知长度且目标支持预分配（writeAtBuf）时，一次性按精确长度分配。
	// 否则边读边扩容会让「旧缓冲 + 新缓冲」同时驻留，峰值可达最终大小的 2~3 倍。
	if resp.ContentLength > 0 {
		if p, ok := w.(interface{ PresizeDownload(int64) error }); ok {
			if preErr := p.PresizeDownload(resp.ContentLength); preErr != nil {
				return 0, preErr
			}
		}
	}
	writer := &writeAtWriter{w: w}
	if _, copyErr := io.Copy(writer, &limitedReader{r: resp.Body, limit: limit}); copyErr != nil {
		return writer.off, copyErr
	}
	bodyReadToEOF = true
	// 截断检测：连接中途断开时 io.Copy 可能以 EOF 正常结束，业务会拿到半个文件却以为成功。
	// 因此校验 Content-Length 与实际写入字节数（gzip 自动解压时 ContentLength=-1，跳过）。
	if resp.ContentLength >= 0 && writer.off != resp.ContentLength {
		return writer.off, fmt.Errorf("kodo: 下载内容不完整，期望 %d 字节，实际 %d 字节: %w",
			resp.ContentLength, writer.off, io.ErrUnexpectedEOF)
	}
	return writer.off, nil
}

// DownloadBytes 下载指定 key 的完整内容并返回字节切片。
//
// **这是唯一会把整个对象放进内存的下载方式**，因此上限与流式下载分离：
// 默认 64MiB（maxDownloadBytes），配置最多可抬到 1GiB；超出返回 ErrDownloadTooLarge。
// 大对象请用 Download(io.WriterAt) 或 DownloadFile 流式写入，不占内存。
func (b *Bucket) DownloadBytes(ctx context.Context, key string) (data []byte, err error) {
	defer recoverToError(&err, "DownloadBytes")
	return b.downloadBytes(ctx, key, 0, nil)
}

// DownloadBytesLimit 与 DownloadBytes 相同，但可对本次调用单独指定内存上限
// （便于「这一个对象有 200MB，我知道」的场景，而不必改全局配置）。
// maxBytes<=0 表示使用配置值；即使传入更大值也不会突破 HardMaxDownloadBytes（1GiB）。
func (b *Bucket) DownloadBytesLimit(ctx context.Context, key string, maxBytes int64) (data []byte, err error) {
	defer recoverToError(&err, "DownloadBytesLimit")
	return b.downloadBytes(ctx, key, maxBytes, nil)
}

// downloadBytes 是内存下载的统一实现。maxBytes<=0 用配置值；budget 可为 nil。
func (b *Bucket) downloadBytes(ctx context.Context, key string, maxBytes int64, budget *byteBudget) (data []byte, err error) {
	if err := b.ready(); err != nil {
		return nil, err
	}
	limit := b.cfg.memoryDownloadLimit()
	if maxBytes > 0 {
		// 显式指定优先（可上调，但不能突破硬上限）：调用方明确表示知道这个对象的大小
		limit = maxBytes
	}
	if limit > HardMaxDownloadBytes {
		limit = HardMaxDownloadBytes
	}
	buf := newWriteAtBufWithBudget(limit, budget)
	// 失败（含 panic）时把已占用的批量预算归还，否则一个失败的 key 会永久吃掉额度，
	// 后续 key 被误判为「批量总量不足」。成功时保留占用，因为返回的 []byte 仍驻留内存。
	defer func() {
		if r := recover(); r != nil {
			budget.release(buf.Cap())
			panic(r)
		}
		if err != nil {
			budget.release(buf.Cap())
		}
	}()
	if err = b.download(ctx, "DownloadBytes", key, buf, limit); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// DownloadFile 把对象流式下载到本地文件，不占用内存（分片/大文件推荐方式）。
//
// 文件已存在会被覆盖；下载失败或重试时由 *os.File 的 Truncate 清理残留。
// 注意：不要多个 goroutine 共享同一路径/文件句柄并发调用（重试会 Truncate(0)）。
func (b *Bucket) DownloadFile(ctx context.Context, key, localPath string) (err error) {
	defer recoverToError(&err, "DownloadFile")
	if err := b.ready(); err != nil {
		return err
	}
	if err := ValidateKey(key); err != nil {
		return err
	}
	if localPath == "" {
		return wrapErrWith(ErrInvalidKey, "localPath 不能为空")
	}
	f, openErr := os.OpenFile(localPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if openErr != nil {
		return fmt.Errorf("kodo: 打开本地文件失败: %w", openErr)
	}
	// 必须用 defer 关闭：download 内部可能 panic（由入口 recover 兜住），
	// 若只在成功/普通失败路径手工 Close，panic 时文件描述符会泄漏。
	// closed=false 意味着「不是正常结束」——此时把文件截断为 0，不留半截内容
	// （半截文件的大小/内容看起来像成功，比空文件更危险）。
	closed := false
	defer func() {
		if closed {
			return
		}
		if truncErr := f.Truncate(0); truncErr != nil {
			logWarn("[kodo] 下载未完成，清理半截文件失败: " + truncErr.Error())
		}
		if closeErr := f.Close(); closeErr != nil {
			logWarn("[kodo] 关闭本地文件失败: " + closeErr.Error())
		}
	}()
	if dlErr := b.download(ctx, "DownloadFile", key, f, 0); dlErr != nil {
		return dlErr
	}
	if closeErr := f.Close(); closeErr != nil {
		closed = true
		return fmt.Errorf("kodo: 关闭本地文件失败: %w", closeErr)
	}
	closed = true
	return nil
}

// Delete 删除指定 key，不存在不报错（幂等）。失败会按配置重试。
func (b *Bucket) Delete(ctx context.Context, key string) (err error) {
	defer recoverToError(&err, "Delete")
	if err := b.readyManager(); err != nil {
		return err
	}
	if err := ValidateKey(key); err != nil {
		return err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	err = retryDo(ctx, b.cfg.retryPolicy(), func() error {
		return b.bucketManager.Delete(b.name, key)
	})
	if err != nil && IsNotFound(err) {
		return nil
	}
	return wrapOp("Delete", b.name, key, err)
}

// Exists 判断 key 是否存在。失败会按配置重试；不存在返回 (false, nil)。
func (b *Bucket) Exists(ctx context.Context, key string) (exists bool, err error) {
	defer recoverToError(&err, "Exists")
	if err := b.readyManager(); err != nil {
		return false, err
	}
	if err := ValidateKey(key); err != nil {
		return false, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	err = retryDo(ctx, b.cfg.retryPolicy(), func() error {
		_, statErr := b.bucketManager.Stat(b.name, key)
		return statErr
	})
	if err != nil {
		if IsNotFound(err) {
			return false, nil
		}
		return false, wrapOp("Exists", b.name, key, err)
	}
	return true, nil
}

// Stat 返回对象的元信息（新增 API）。不存在返回 ErrObjectNotFound。
func (b *Bucket) Stat(ctx context.Context, key string) (info ObjectInfo, err error) {
	defer recoverToError(&err, "Stat")
	if err := b.readyManager(); err != nil {
		return ObjectInfo{}, err
	}
	if err := ValidateKey(key); err != nil {
		return ObjectInfo{}, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	var fileInfo storage.FileInfo
	err = retryDo(ctx, b.cfg.retryPolicy(), func() error {
		fi, statErr := b.bucketManager.Stat(b.name, key)
		if statErr != nil {
			return statErr
		}
		fileInfo = fi
		return nil
	})
	if err != nil {
		if IsNotFound(err) {
			return ObjectInfo{}, wrapOp("Stat", b.name, key, fmt.Errorf("object not found: %w", ErrObjectNotFound))
		}
		return ObjectInfo{}, wrapOp("Stat", b.name, key, err)
	}
	info = ObjectInfo{
		Key:         key,
		Size:        fileInfo.Fsize,
		ETag:        fileInfo.Hash,
		ContentType: fileInfo.MimeType,
	}
	if fileInfo.PutTime > 0 {
		info.LastModified = putTimeToTime(fileInfo.PutTime)
	}
	return info, nil
}

// List 列举桶内对象，prefix 为前缀过滤，max 为最大返回数量（<=0 取 1000，上限 1000）。
//
// 注意：这是**单页**语义，七牛单次最多返回 1000 条。需要完整列表请用 ListAll，
// 否则调用方会拿到被静默截断的结果而毫无感知。
func (b *Bucket) List(ctx context.Context, prefix string, max int32) (list []ObjectInfo, err error) {
	defer recoverToError(&err, "List")
	if err := b.readyManager(); err != nil {
		return nil, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	limit := int(max)
	if limit <= 0 {
		limit = DefaultListLimit
	}
	if limit > DefaultListLimit {
		limit = DefaultListLimit
	}
	var ret *storage.ListFilesRet
	err = retryDo(ctx, b.cfg.retryPolicy(), func() error {
		page, _, listErr := b.bucketManager.ListFilesWithContext(ctx, b.name,
			storage.ListInputOptionsPrefix(prefix),
			storage.ListInputOptionsLimit(limit),
		)
		if listErr != nil {
			return listErr
		}
		ret = page
		return nil
	})
	if err != nil {
		return nil, wrapOp("List", b.name, prefix, err)
	}
	return toObjectInfos(ret), nil
}

// pageFetcher 拉取一页对象：返回本页条目、下一页 marker、是否还有下一页。
// 抽成函数类型是为了让分页循环（上限、ctx、marker 不推进）可以脱离网络单测。
type pageFetcher func(ctx context.Context, marker string) (items []ObjectInfo, next string, hasNext bool, err error)

// listPages 是所有分页列举的唯一实现。
//
// maxObjects>0 时，累计条数一旦超限立即返回 ErrListTooLarge（绝不静默截断）；
// maxObjects<=0 表示不限制（仅用于流式遍历，此时内存不随条数增长）。
// fn 为 nil 时视为不消费（仅计数）。
func listPages(ctx context.Context, op, prefix string, maxObjects int, fetch pageFetcher, fn func([]ObjectInfo) error) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if fetch == nil {
		return wrapErrWith(ErrNilCallback, op+" 缺少分页取数函数")
	}
	marker := ""
	fetched := 0
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		items, next, hasNext, err := fetch(ctx, marker)
		if err != nil {
			return err
		}
		if maxObjects > 0 && fetched+len(items) > maxObjects {
			return wrapErrWith(ErrListTooLarge, fmt.Sprintf(
				"%s 已列举 %d 个对象，超过上限 %d（配置 maxListObjects，硬上限 %d）；遍历超大桶请改用 ListEach/ListEachPage 流式接口",
				op, fetched+len(items), maxObjects, HardMaxListObjects))
		}
		fetched += len(items)
		if fn != nil && len(items) > 0 {
			if err := fn(items); err != nil {
				return err
			}
		}
		if !hasNext || next == "" || next == marker {
			if hasNext && next == marker {
				logWarn("[kodo] " + op + " 检测到 marker 未推进，提前结束以避免死循环")
			}
			return nil
		}
		marker = next
	}
}

// fetchPage 用 SDK 拉取一页（带幂等重试）。marker 为空表示首页。
func (b *Bucket) fetchPage(ctx context.Context, prefix, marker string, limit int) ([]ObjectInfo, string, bool, error) {
	var page *storage.ListFilesRet
	var hasNext bool
	err := retryDo(ctx, b.cfg.retryPolicy(), func() error {
		ret, more, listErr := b.bucketManager.ListFilesWithContext(ctx, b.name,
			storage.ListInputOptionsPrefix(prefix),
			storage.ListInputOptionsMarker(marker),
			storage.ListInputOptionsLimit(limit),
		)
		if listErr != nil {
			return listErr
		}
		page, hasNext = ret, more
		return nil
	})
	if err != nil {
		return nil, "", false, err
	}
	if page == nil {
		return nil, "", false, nil
	}
	return toObjectInfos(page), page.Marker, hasNext, nil
}

// ListAll 列举桶内全部对象（按 marker 分页循环，自动跟随下一页）。
//
// 结果整体驻留内存，因此受 maxListObjects（默认 10 万条，硬上限 100 万）约束：
// 超限返回 ErrListTooLarge，而不是返回一个被静默截断的列表。
// 遍历超大桶请用 ListEach / ListEachPage（流式，内存与对象数无关）。
//
// 防死循环：若服务端返回的 marker 未推进，则立即退出。ctx 取消会中止循环。
func (b *Bucket) ListAll(ctx context.Context, prefix string) (list []ObjectInfo, err error) {
	return b.ListAllLimit(ctx, prefix, 0)
}

// ListAllLimit 与 ListAll 相同，但可对本次调用单独指定条数上限。
// maxObjects<=0 表示使用配置值；超过 HardMaxListObjects 会被钳制（配置无法突破硬上限）。
//
// 注意：这是「全量上限」而不是「取前 N 条」——单页最小 1000 条，若只要少量对象
// 请用 List(ctx, prefix, n)，否则 maxObjects 小于一页时会直接报 ErrListTooLarge。
func (b *Bucket) ListAllLimit(ctx context.Context, prefix string, maxObjects int) (list []ObjectInfo, err error) {
	defer recoverToError(&err, "ListAll")
	if err := b.readyManager(); err != nil {
		return nil, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	limit := maxObjects
	if limit <= 0 {
		limit = b.cfg.listObjectLimit()
	}
	if limit > HardMaxListObjects {
		limit = HardMaxListObjects
	}
	result := make([]ObjectInfo, 0, 256)
	err = listPages(ctx, "ListAll", prefix, limit,
		func(ctx context.Context, marker string) ([]ObjectInfo, string, bool, error) {
			return b.fetchPage(ctx, prefix, marker, DefaultListLimit)
		},
		func(items []ObjectInfo) error {
			result = append(result, items...)
			return nil
		})
	if err != nil {
		// 不返回部分结果：调用方拿到半份列表却以为成功，比直接失败更危险
		return nil, wrapOp("ListAll", b.name, prefix, err)
	}
	return result, nil
}

// ListEach 流式遍历桶内全部对象，每读到一个对象就回调一次。
//
// 与 ListAll 的关键区别：不累积结果，内存占用只与单页大小（1000 条）相关，
// 因此不受 maxListObjects 限制，可用于百万级对象的桶。
// 回调返回 error 会立即中止遍历并原样返回（可用于「找到即停」）。
func (b *Bucket) ListEach(ctx context.Context, prefix string, fn func(ObjectInfo) error) (err error) {
	defer recoverToError(&err, "ListEach")
	if fn == nil {
		return ErrNilCallback
	}
	return b.ListEachPage(ctx, prefix, func(page []ObjectInfo) error {
		for _, info := range page {
			if err := fn(info); err != nil {
				return err
			}
		}
		return nil
	})
}

// ListEachPage 流式遍历桶内全部对象，按页回调（每页最多 1000 条）。
//
// 需要批量写库/批量删除时用这个，比 ListEach 少了逐条回调开销。
// 每页切片都是新分配的，回调内可安全持有。回调返回 error 会立即中止遍历。
func (b *Bucket) ListEachPage(ctx context.Context, prefix string, fn func([]ObjectInfo) error) (err error) {
	defer recoverToError(&err, "ListEachPage")
	if fn == nil {
		return ErrNilCallback
	}
	if err := b.readyManager(); err != nil {
		return err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	err = listPages(ctx, "ListEach", prefix, 0,
		func(ctx context.Context, marker string) ([]ObjectInfo, string, bool, error) {
			return b.fetchPage(ctx, prefix, marker, DefaultListLimit)
		}, fn)
	if err != nil {
		return wrapOp("ListEach", b.name, prefix, err)
	}
	return nil
}

// Presign 生成下载预签名 URL，有效期使用配置的 presignExpiry。
// 私有空间返回带下载凭证的 URL；公有空间返回永久可访问的公开 URL。
// key 会被正确 escape（含空格、'?'、'#' 等字符）。
func (b *Bucket) Presign(ctx context.Context, key string) (url string, err error) {
	defer recoverToError(&err, "Presign")
	if err := b.ready(); err != nil {
		return "", err
	}
	if err := ValidateKey(key); err != nil {
		return "", err
	}
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return "", err
		}
	}
	return b.accessURL(key)
}

// PresignUpload 生成上传预签名凭证（upToken），客户端可凭此凭证直传到该桶。
//
// 安全约束：**key 不能为空**。空 key 会让 scope 退化为 bucket 级，等于允许上传到桶内
// 任意 key（越权）。空 key 返回 ErrInvalidKey。
func (b *Bucket) PresignUpload(ctx context.Context, key string) (token string, err error) {
	defer recoverToError(&err, "PresignUpload")
	if err := b.readyUploader(); err != nil {
		return "", err
	}
	if err := ValidateKey(key); err != nil {
		return "", err
	}
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return "", err
		}
	}
	return b.uploadToken(key)
}
