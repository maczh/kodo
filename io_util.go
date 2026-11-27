package kodo

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"runtime/debug"
	"strings"
	"sync"
)

// uploadInput 是规整后的上传输入。
type uploadInput struct {
	// Reader 实际用于上传的 reader
	Reader io.Reader
	// Size 已知长度（Sized=false 时无意义）
	Size int64
	// Sized 是否已知长度
	Sized bool
	// FromStart 当前读取位置是否为流起点。仅当 FromStart=true 才能用 io.ReaderAt
	// 配合 Size 做「按长度分片」（ReaderAt 的偏移总是从 0 开始算）。
	FromStart bool
}

// maxTrustedUploadSize 是「可信长度」的上限（5TiB）。
//
// 为什么需要：已知长度会走 ResumeUploader.Put(fsize)，SDK 的分片循环是
// `for off < totalSize { ... off += blockSize }`，且每个分片都会 new 一个
// SectionReader 并派发任务。若 io.Seeker 上报的长度是谎言（自定义 Seeker 返回
// MaxInt64、或 end-cur 计算被异常实现污染），循环会在「读到 EOF 但 off 远未
// 达到 totalSize」之间空转数十亿次 —— 表现为 CPU 打满的假死，比 panic 更难发现。
// 超过该上限的长度一律不信任，退回流式上传（PutWithoutSize，边读边分片，
// 读到 EOF 即结束，结果依然正确，只是少了 ReaderAt 的并发度）。
const maxTrustedUploadSize int64 = 5 << 40 // 5TiB

// errSeekPositionLost 表示「已经 seek 到末尾，但无法复位到原位置」。
//
// 这种情况必须直接失败而不是退回流式上传：此时 reader 停在流末尾，继续上传
// 会把「空内容」当作成功结果写进对象存储（静默数据损坏），比报错危险得多。
var errSeekPositionLost = errors.New("kodo: seek position lost")

// errNotSeekable 表示 reader 根本不支持 seek（管道、网络流等）。
//
// 这是完全正常的输入，走流式上传即可，因此不打印警告 —— 否则凡是上传管道/流的
// 业务调用都会刷一条 warn 日志。
var errNotSeekable = errors.New("kodo: reader is not seekable")

// uploadSource 把任意 io.Reader 规整为「可用于流式上传的 reader」及其长度。
//
// 旧实现对所有非 bytes.Reader/Buffer 的输入直接 io.ReadAll，大文件会全部进内存，
// 而且 100MB 的分片阈值要等读完之后才判断，等于失效。现在的策略：
//  1. 已知长度的类型（bytes.Reader/bytes.Buffer/strings.Reader）直接取长度；
//  2. 实现 io.Seeker 的（如 *os.File）seek 到末尾测得长度后 seek 回来，不占内存；
//     测得的长度还需落在 [0, maxTrustedUploadSize] 内才被信任；
//  3. 其余情况返回 Sized=false，由调用方走 PutWithoutSize 流式分片上传（不占内存）。
func uploadSource(r io.Reader) (uploadInput, error) {
	input := uploadInput{Reader: r}
	// isNilValue 同时拦住 typed-nil（如 (*bytes.Reader)(nil)）：
	// 这类值 `r != nil` 成立，但调用其方法会解引用 nil 指针 panic。
	if isNilValue(r) {
		return input, ErrNilReader
	}
	switch v := r.(type) {
	case *bytes.Reader:
		size := v.Size()
		input.Size = int64(v.Len())
		input.Sized = true
		input.FromStart = int64(v.Len()) == size
		return input, nil
	case *bytes.Buffer:
		data := v.Bytes()
		input.Reader = bytes.NewReader(data)
		input.Size = int64(len(data))
		input.Sized = true
		input.FromStart = true
		return input, nil
	case *strings.Reader:
		input.Size = int64(v.Len())
		input.Sized = true
		input.FromStart = int64(v.Len()) == v.Size()
		return input, nil
	}
	if seeker, ok := r.(io.Seeker); ok {
		if isNilValue(seeker) {
			return input, ErrNilReader
		}
		size, fromStart, seekErr := sizeOfSeeker(seeker)
		switch {
		case errors.Is(seekErr, errSeekPositionLost):
			// 无法复位：继续上传会写入空内容，必须失败而不是「猜」
			return input, seekErr
		case errors.Is(seekErr, errNotSeekable):
			// 管道/网络流：正常输入，静默降级为流式上传
		case seekErr != nil:
			// 可 seek 但测量失败：不常见，记一条日志便于排查
			logWarn("[kodo] 无法通过 Seek 测量上传流长度，改为流式分片上传: " + errorText(seekErr))
		case size < 0 || size > maxTrustedUploadSize:
			// 长度不可信：不按它切分片，否则 SDK 会空转（见 maxTrustedUploadSize 注释）
			logWarn(fmt.Sprintf("[kodo] 上传流声明的长度 %d 不可信（上限 %d 字节），改为流式分片上传", size, maxTrustedUploadSize))
		default:
			input.Size = size
			input.Sized = true
			input.FromStart = fromStart
		}
	}
	return input, nil
}

// sizeOfSeeker 测出从当前位置到末尾的剩余长度与「当前位置是否为起点」，并保证位置不变。
//
// 错误语义：
//   - errNotSeekable：第一次 Seek(Current) 就失败，说明完全不支持 seek（管道等），
//     位置未变，调用方静默降级即可；
//   - errSeekPositionLost：已经 Seek 到末尾但无法复位，调用方必须当作致命错误，
//     因为此时 reader 停在末尾，任何后续读取都会立即 EOF（上传会写出空对象）；
//   - 其它错误：seek 可用但测量异常，位置已被复位，可降级。
func sizeOfSeeker(s io.Seeker) (size int64, fromStart bool, err error) {
	cur, err := s.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, false, fmt.Errorf("%w: %v", errNotSeekable, err)
	}
	end, err := s.Seek(0, io.SeekEnd)
	if err != nil {
		// 部分实现的 Seek 会先移动位置再返回错误，因此这里也要尝试复位
		if _, backErr := s.Seek(cur, io.SeekStart); backErr != nil {
			return 0, false, fmt.Errorf("%w: %v (复位失败: %v)", errSeekPositionLost, err, backErr)
		}
		return 0, false, err
	}
	if _, err := s.Seek(cur, io.SeekStart); err != nil {
		return 0, false, fmt.Errorf("%w: %v", errSeekPositionLost, err)
	}
	if end < cur {
		return 0, false, fmt.Errorf("kodo: invalid reader position: end=%d cur=%d", end, cur)
	}
	return end - cur, cur == 0, nil
}

// readAllBounded 读取至多 limit 字节，超限返回 ErrUploadTooLarge。
func readAllBounded(r io.Reader, limit int64) ([]byte, error) {
	if isNilValue(r) {
		return nil, ErrNilReader
	}
	if limit <= 0 {
		limit = DefaultMaxUploadMemory
	}
	// LimitReader(r, limit+1) 中的 +1 在 limit==MaxInt64 时会溢出为负数，
	// 退化成立即 EOF（静默返回空内容）。这里先把上限压到安全区间。
	if limit >= math.MaxInt64-1 {
		limit = math.MaxInt64 - 1
	}
	buf := &bytes.Buffer{}
	n, err := io.Copy(buf, io.LimitReader(r, limit+1))
	if err != nil {
		return nil, err
	}
	if n > limit {
		return nil, fmt.Errorf("%w: 已读取 %d 字节 > 上限 %d 字节，大文件请使用 UploadFile 或保持流式上传",
			ErrUploadTooLarge, n, limit)
	}
	return buf.Bytes(), nil
}

// writeAtWriter 将 io.WriterAt 适配为 io.Writer（顺序写入，不占额外内存）。
type writeAtWriter struct {
	w   io.WriterAt
	off int64
}

// Write 从当前偏移顺序写入。
func (a *writeAtWriter) Write(p []byte) (int, error) {
	if a == nil || isNilValue(a.w) {
		return 0, ErrNilWriter
	}
	n, err := a.w.WriteAt(p, a.off)
	// io.WriterAt 契约要求 0<=n<=len(p)。第三方实现违规时，若按原始 n 推进偏移，
	// 会让后续写入错位甚至让 off 变负，这里先钳制再推进。
	if n < 0 {
		n = 0
	}
	if n > len(p) {
		n = len(p)
	}
	a.off += int64(n)
	return n, err
}

// byteBudget 是并发安全的内存配额计数器，按「容量」计费。
//
// 用途：DownloadMany 会把所有对象同时保留在内存里返回，单对象上限乘以 key 数量
// 依然可以把进程打爆。这里给整批下载一个共享总量预算，任何一个对象扩容前都要
// 先从预算里占到容量，占不到就以 ErrDownloadTooLarge 失败。
type byteBudget struct {
	mu   sync.Mutex
	used int64
	max  int64
}

// newByteBudget 创建总量为 max 字节的预算；max<=0 时退化为默认总量。
func newByteBudget(max int64) *byteBudget {
	if max <= 0 {
		max = DefaultMaxDownloadManyBytes
	}
	return &byteBudget{max: max}
}

// charge 尝试占用 n 字节；成功返回 true。
func (b *byteBudget) charge(n int64) bool {
	if b == nil || n <= 0 {
		return true
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.max > 0 && b.used+n > b.max {
		return false
	}
	b.used += n
	return true
}

// release 归还 n 字节。
func (b *byteBudget) release(n int64) {
	if b == nil || n <= 0 {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.used -= n
	if b.used < 0 {
		b.used = 0
	}
}

// usedBytes 返回已占用字节数（测试与诊断用）。
func (b *byteBudget) usedBytes() int64 {
	if b == nil {
		return 0
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.used
}

// writeAtBuf 可增长的 WriteAt 缓冲，用于把下载内容累积在内存里。带显式大小上限。
//
// 内存安全设计（OOM 是 runtime throw，recover 兜不住，只能事前拦住）：
//  1. 上限与流式下载分离，默认 64MiB，配置最多 1GiB；
//  2. 已知 Content-Length 时按精确长度一次性分配（Presize），避免边读边扩容
//     造成「旧缓冲 + 新缓冲」的 2~3 倍内存峰值；
//  3. 未知长度时才按 1.5 倍增长（不是 2 倍），且每次扩容前先向预算申请容量。
type writeAtBuf struct {
	buf    []byte
	limit  int64
	budget *byteBudget
}

// newWriteAtBuf 创建带上限的缓冲。limit <= 0 时使用内存下载硬上限。
func newWriteAtBuf(limit int64) *writeAtBuf {
	return newWriteAtBufWithBudget(limit, nil)
}

// newWriteAtBufWithBudget 创建带共享内存预算的缓冲。
func newWriteAtBufWithBudget(limit int64, budget *byteBudget) *writeAtBuf {
	if limit <= 0 || limit > HardMaxDownloadBytes {
		limit = HardMaxDownloadBytes
	}
	return &writeAtBuf{limit: limit, budget: budget}
}

// effectiveLimit 返回生效上限（任何情况下不超过 HardMaxDownloadBytes）。
func (b *writeAtBuf) effectiveLimit() int64 {
	limit := HardMaxDownloadBytes
	if b != nil && b.limit > 0 && b.limit < limit {
		limit = b.limit
	}
	return limit
}

// tooLarge 生成带使用建议的超限错误。文案里必须给出逃生路径，
// 否则业务只会看到「下载失败」而继续用 DownloadBytes 拉大文件。
func (b *writeAtBuf) tooLarge(size int64) error {
	return fmt.Errorf("%w: 至少 %d 字节 > 内存下载上限 %d 字节；"+
		"大对象请改用 Download(io.WriterAt) 或 DownloadFile 流式落盘，或调大 maxDownloadBytes（硬上限 %d 字节）",
		ErrDownloadTooLarge, size, b.effectiveLimit(), HardMaxDownloadBytes)
}

// reserve 确保容量至少为 need；exact=true 时按 need 精确分配（不做超额增长）。
func (b *writeAtBuf) reserve(need int64, exact bool) error {
	if need <= int64(cap(b.buf)) {
		return nil
	}
	if need > b.effectiveLimit() {
		return b.tooLarge(need)
	}
	newCap := need
	if !exact {
		cur := int64(cap(b.buf))
		// 1.5 倍增长：比 2 倍少一次「旧缓冲 + 新缓冲」的峰值
		if grown := cur + cur/2; grown > newCap {
			newCap = grown
		}
		// 小缓冲时保证最小步进，避免小对象触发频繁扩容；
		// 步进取 64KiB（而不是 1MiB），否则 DownloadMany 里成千上万个小文件
		// 会各占 1MiB 容量，把总量预算瞬间吃光。
		step := int64(64 << 10)
		if cur+step > newCap {
			newCap = cur + step
		}
		if newCap > b.effectiveLimit() {
			newCap = b.effectiveLimit()
		}
		if newCap < need {
			newCap = need
		}
	}
	delta := newCap - int64(cap(b.buf))
	if delta > 0 && b.budget != nil && !b.budget.charge(delta) {
		return fmt.Errorf("%w: 批量下载总内存预算不足（本批上限 %d 字节）",
			ErrDownloadTooLarge, b.budget.max)
	}
	grown := make([]byte, len(b.buf), newCap)
	copy(grown, b.buf)
	b.buf = grown
	return nil
}

// PresizeDownload 按 Content-Length 预先精确分配容量，避免边读边扩容。
// 供 downloadOnce 在读取 body 前调用；重复调用与重试均安全（已有容量时不重新分配）。
func (b *writeAtBuf) PresizeDownload(n int64) error {
	if b == nil {
		return ErrNilWriter
	}
	if n <= 0 {
		return nil
	}
	if n > b.effectiveLimit() {
		return b.tooLarge(n)
	}
	return b.reserve(n, true)
}

// WriteAt 实现 io.WriterAt，超过上限返回 ErrDownloadTooLarge。
func (b *writeAtBuf) WriteAt(p []byte, off int64) (int, error) {
	if b == nil {
		return 0, ErrNilWriter
	}
	if off < 0 {
		return 0, fmt.Errorf("kodo: negative write offset %d", off)
	}
	if len(p) == 0 {
		return 0, nil
	}
	end := off + int64(len(p))
	if end < off {
		return 0, fmt.Errorf("%w: offset overflow", ErrDownloadTooLarge)
	}
	limit := b.effectiveLimit()
	if end > limit {
		return 0, b.tooLarge(end)
	}
	if end > int64(cap(b.buf)) {
		if err := b.reserve(end, false); err != nil {
			return 0, err
		}
	}
	if end > int64(len(b.buf)) {
		b.buf = b.buf[:end]
	}
	copy(b.buf[off:], p)
	return len(p), nil
}

// Reset 清空缓冲（供下载重试时截断上次残留数据）
func (b *writeAtBuf) Reset() {
	if b == nil {
		return
	}
	b.buf = b.buf[:0]
}

// Bytes 返回已写入的字节切片。
func (b *writeAtBuf) Bytes() []byte {
	if b == nil {
		return nil
	}
	return b.buf
}

// Cap 返回当前容量（用于失败时把已占用的预算精确归还）。
func (b *writeAtBuf) Cap() int64 {
	if b == nil {
		return 0
	}
	return int64(cap(b.buf))
}

// limitedReader 限制最多读取 limit+1 字节，超出即返回 ErrDownloadTooLarge。
type limitedReader struct {
	r     io.Reader
	limit int64
	read  int64
}

// Read 实现 io.Reader。
func (l *limitedReader) Read(p []byte) (int, error) {
	if l == nil || isNilValue(l.r) {
		return 0, ErrNilReader
	}
	remaining := l.limit - l.read
	if remaining < 0 {
		return 0, fmt.Errorf("%w: 已超过 %d 字节上限；大对象请改用 DownloadFile 流式落盘", ErrDownloadTooLarge, l.limit)
	}
	// 多读 1 字节用于判断是否超限。remaining 取 math.MaxInt64 时 +1 会溢出成负数，
	// 随后 p[:负数] 直接 panic（虽然生产路径的 limit ≤ 8GiB，但这是通用读取器，
	// 测试与未来调用方都可能传入极端值）。
	if remaining < math.MaxInt64 && int64(len(p)) > remaining+1 {
		p = p[:remaining+1]
	}
	n, err := l.r.Read(p)
	l.read += int64(n)
	if l.read > l.limit {
		return n, fmt.Errorf("%w: 已超过 %d 字节上限；大对象请改用 DownloadFile 流式落盘", ErrDownloadTooLarge, l.limit)
	}
	return n, err
}

// safeReader 在上传边界兜住业务 io.Reader 的 panic。
//
// 为什么必须做（这是本插件唯一无法用 defer recover 覆盖的路径）：
// 上传请求的 body 由 net/http 在**独立的 writeLoop 协程**读取；分片上传还会经过
// SDK 自己的 worker 池（storage.uploadByWorkers）。业务传入的 Reader 若在 Read 里
// panic，panic 发生在那些协程里，调用方的 defer recover 完全兜不住，会直接终止进程。
// 把 Read 的 panic 转成 error，SDK 就会按「上传失败」正常返回。
type safeReader struct {
	r io.Reader
}

// Read 实现 io.Reader，把底层 panic 与非法计数转成错误。
func (s *safeReader) Read(p []byte) (n int, err error) {
	defer func() {
		if r := recover(); r != nil {
			detail := panicText(r)
			logError(fmt.Sprintf("[kodo] panic while reading upload body: %s\n%s", detail, debug.Stack()))
			n, err = 0, fmt.Errorf("kodo: panic while reading upload body: %s", detail)
		}
	}()
	n, err = s.r.Read(p)
	if n < 0 || n > len(p) {
		return 0, fmt.Errorf("kodo: invalid read count %d (len=%d) from upload body", n, len(p))
	}
	return n, err
}

// safeReaderAt 与 safeReader 同理，用于分片上传的 io.ReaderAt 入口
// （SDK 会把 SectionReader 交给 worker 池并发读取）。
type safeReaderAt struct {
	r io.ReaderAt
}

// ReadAt 实现 io.ReaderAt，把底层 panic 与非法计数转成错误。
func (s *safeReaderAt) ReadAt(p []byte, off int64) (n int, err error) {
	defer func() {
		if r := recover(); r != nil {
			detail := panicText(r)
			logError(fmt.Sprintf("[kodo] panic while reading upload body at %d: %s\n%s", off, detail, debug.Stack()))
			n, err = 0, fmt.Errorf("kodo: panic while reading upload body at %d: %s", off, detail)
		}
	}()
	n, err = s.r.ReadAt(p, off)
	if n < 0 || n > len(p) {
		return 0, fmt.Errorf("kodo: invalid read count %d (len=%d) from upload body at %d", n, len(p), off)
	}
	return n, err
}

// drainAndClose 尽量读完 body 剩余少量数据以复用连接，然后关闭。
//
// 为什么要单独兜 panic：body 可能来自自定义 Transport（或三方拦截器），其
// Read/Close 抛 panic 时，若这个 defer 在「下载已成功」之后 panic，会把一次
// 成功下载变成失败（更糟的是连接得不到关闭）。这里保证：
//   - 读残留数据 panic 被吞掉；
//   - 无论读是否 panic，Close 都会执行，且 Close 自身的 panic 也被吞掉。
func drainAndClose(body io.ReadCloser) {
	if body == nil {
		return
	}
	defer func() {
		if recover() != nil {
			safeCloseBody(body)
		}
	}()
	_, _ = io.Copy(io.Discard, io.LimitReader(body, 4*1024))
	safeCloseBody(body)
}

// safeCloseBody 关闭 body 并吞掉其 panic（旁路清理不允许影响主流程）。
func safeCloseBody(body io.ReadCloser) {
	if body == nil {
		return
	}
	defer func() { _ = recover() }()
	_ = body.Close()
}

// recoverToError 把 panic 转成 error 并打印堆栈。
//
// 这是插件边界：SDK 内部 panic 不能打穿业务的 HTTP handler。
//
// 必须写成 `defer recoverToError(&err, "X")` 的直接形式：recover() 只有在
// 「defer 注册的函数体内直接调用」时才有效，若再包一层匿名闭包会静默失效
// （io_util_test.go 的 TestNoClosureWrappedRecoverToError 会拦住这种回退）。
func recoverToError(errp *error, op string) {
	r := recover()
	if r == nil {
		return
	}
	// 已经处于 recover 路径：这里的任何二次 panic 都会让进程直接退出。
	// 因此 panic 值格式化与日志输出都必须保证不 panic。
	detail := panicText(r)
	logError(fmt.Sprintf("[kodo] panic in %s: %s\n%s", op, detail, debug.Stack()))
	if errp != nil {
		*errp = fmt.Errorf("kodo: panic in %s: %s", op, detail)
	}
}

// panicText 安全地格式化 panic 值。
//
// panic 值可能是「String()/Error() 自身会 panic」的类型，直接 %v 会在 recover
// 路径里再抛 panic（进程终止），因此单独兜一层。
func panicText(r any) (text string) {
	defer func() {
		if recover() != nil {
			text = fmt.Sprintf("<unprintable panic value: %T>", r)
		}
	}()
	return fmt.Sprintf("%v", r)
}
