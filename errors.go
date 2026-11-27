package kodo

import (
	"errors"
	"fmt"
	"strings"
	"unicode"

	qiniu "github.com/qiniu/go-sdk/v7"
	qclient "github.com/qiniu/go-sdk/v7/client"
	"github.com/qiniu/go-sdk/v7/storage"
)

// 插件哨兵错误。调用方应使用 errors.Is 判断，而不是匹配错误文案。
var (
	// ErrNotInitialized 插件未初始化（或未成功初始化）即被调用
	ErrNotInitialized = errors.New("kodo: plugin not initialized")
	// ErrBucketNotFound 指定的桶不存在于配置中
	ErrBucketNotFound = errors.New("kodo: bucket not found")
	// ErrClosed 插件已 Close，拒绝继续服务（再次 Init 可恢复）
	ErrClosed = errors.New("kodo: plugin closed")
	// ErrDisabled 配置中 enabled: false 显式禁用，插件拒绝提供任何存储操作
	ErrDisabled = errors.New("kodo: plugin disabled by config")
	// ErrInvalidConfig 配置解析或校验失败
	ErrInvalidConfig = errors.New("kodo: invalid config")
	// ErrInvalidKey 对象 key 非法（空、超长、含控制字符）
	ErrInvalidKey = errors.New("kodo: invalid object key")
	// ErrNilReader 上传时传入 nil reader
	ErrNilReader = errors.New("kodo: nil reader")
	// ErrNilWriter 下载时传入 nil writer
	ErrNilWriter = errors.New("kodo: nil writer")
	// ErrObjectNotFound 对象不存在（下载 404 被映射为该错误）
	ErrObjectNotFound = errors.New("kodo: object not found")
	// ErrDownloadTooLarge 下载体积超过 maxDownloadSize 或硬上限
	ErrDownloadTooLarge = errors.New("kodo: download exceeds max size")
	// ErrUploadTooLarge 未知长度上传流缓冲超过 maxUploadMemory
	ErrUploadTooLarge = errors.New("kodo: upload exceeds max upload memory")
	// ErrListTooLarge ListAll 累积的对象条数超过 maxListObjects（改用 ListEach 流式遍历）
	ErrListTooLarge = errors.New("kodo: list result exceeds max objects")
	// ErrNilCallback 流式遍历时传入 nil 回调
	ErrNilCallback = errors.New("kodo: nil callback")
)

// statusError 表示一次非 2xx 的 HTTP 响应。
type statusError struct {
	Op         string
	StatusCode int
	Status     string
}

// Error 实现 error 接口（nil 接收者返回固定文案，避免 typed-nil 在错误链里被格式化时 panic）。
func (e *statusError) Error() string {
	if e == nil {
		return "kodo: nil status error"
	}
	return fmt.Sprintf("kodo: %s failed: %s", e.Op, e.Status)
}

// retryable 5xx 与 429 属于可重试的服务端/限流错误，4xx 一律不重试。
func (e *statusError) retryable() bool {
	if e == nil {
		return false
	}
	return e.StatusCode == 429 || e.StatusCode >= 500
}

// KeyError 为批量操作中单个 key 的失败包装。
type KeyError struct {
	Key string
	Err error
}

// Error 实现 error 接口。
//
// Err 可能为 nil 或 typed-nil（调用方自行构造 KeyError 时），直接 %v 在
// 「底层错误的 Error() 自身会 panic」时会让日志/聚合路径反向崩溃。
func (e *KeyError) Error() string {
	if e == nil {
		return ""
	}
	msg := errorText(e.Err)
	if msg == "" {
		msg = "<nil>"
	}
	return fmt.Sprintf("%q: %s", e.Key, msg)
}

// Unwrap 支持 errors.Is/As 穿透到原始错误。
func (e *KeyError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// MultiError 聚合批量操作中的多个失败项，返回全部失败而不是只返回第一个。
// 调用方可用 errors.As 取出 *MultiError 逐个检查，也可用 errors.Is 判断是否包含某类错误。
type MultiError struct {
	Op     string
	Errors []error
}

// Error 实现 error 接口。
//
// 注意：Errors 是对外暴露的切片，调用方可能塞入 nil，此时直接调用 err.Error()
// 会 panic，因此这里跳过 nil 项并在摘要里体现数量。
func (m *MultiError) Error() string {
	if m == nil || len(m.Errors) == 0 {
		return ""
	}
	const maxDetail = 5
	details := make([]string, 0, maxDetail)
	nilCount := 0
	for i, err := range m.Errors {
		if i >= maxDetail {
			details = append(details, fmt.Sprintf("... and %d more", len(m.Errors)-maxDetail))
			break
		}
		if err == nil || isNilValue(err) {
			nilCount++
			continue
		}
		details = append(details, errorText(err))
	}
	if nilCount > 0 {
		details = append(details, fmt.Sprintf("%d item(s) with nil error", nilCount))
	}
	if len(details) == 0 {
		return fmt.Sprintf("kodo: %s failed for %d item(s)", m.Op, len(m.Errors))
	}
	return fmt.Sprintf("kodo: %s failed for %d item(s): %s", m.Op, len(m.Errors), strings.Join(details, "; "))
}

// Unwrap 返回全部子错误，配合 errors.Is/As 使用。
func (m *MultiError) Unwrap() []error {
	if m == nil {
		return nil
	}
	return m.Errors
}

// Failed 返回失败项数量。
func (m *MultiError) Failed() int {
	if m == nil {
		return 0
	}
	return len(m.Errors)
}

// IsNotFound 判断错误是否表示「文件/资源不存在」。
//
// 判定顺序（严格，避免误判）：
//  1. errors.Is(err, storage.ErrNoSuchFile) / errors.Is(err, ErrObjectNotFound)
//  2. errors.As(*qiniu.QError) 且 Code == "612"
//  3. errors.As(*client.ErrorInfo) 且 Code == 612
//  4. 文案兜底：仅匹配 "no such file" / "no such object"（不再匹配裸 "612"，
//     否则 key 里带 612、文件大小 612 等都会被误判为不存在）
func IsNotFound(err error) (notFound bool) {
	// 纯判定函数，输入是任意第三方错误（可能带 typed-nil 节点或会 panic 的
	// Unwrap/Is 实现）。判定失败退化为 false，绝不允许把调用方打崩。
	defer func() {
		if recover() != nil {
			notFound = false
		}
	}()
	// typed-nil（指针为 nil 但接口非 nil）既不能 errors.Is（可能触发方法调用）也不能取 Error()
	if err == nil || isNilValue(err) {
		return false
	}
	if errors.Is(err, storage.ErrNoSuchFile) || errors.Is(err, ErrObjectNotFound) {
		return true
	}
	var qe *qiniu.QError
	if errors.As(err, &qe) && qe != nil && qe.Code == "612" {
		return true
	}
	var ei *qclient.ErrorInfo
	if errors.As(err, &ei) && ei != nil && ei.Code == 612 {
		return true
	}
	msg := strings.ToLower(errorText(err))
	return strings.Contains(msg, "no such file") || strings.Contains(msg, "no such object")
}

// wrapOp 为底层错误附加操作上下文（op / bucket / key），并用 %w 保留错误链。
//
// 为什么不能简单 return err：同一次请求失败时无法区分是哪个桶、哪个 key、哪个操作。
// 上下文一律通过 %w 包装，保证 errors.Is/IsNotFound/isRetryable 的判定链不中断。
// key 参数对列举类操作传入 prefix。
//
// 安全约束：op/bucket/key 均由调用方提供，不得包含 SecretKey、upToken 或预签名 token。
func wrapOp(op, bucket, key string, err error) error {
	// typed-nil 包装后会变成「非 nil 但无内容」的错误，调用方 err != nil 判定为失败，
	// 既难排查也会让错误链里出现空指针节点，这里统一按「无错误」处理。
	if err == nil || isNilValue(err) {
		return nil
	}
	context := "kodo " + op
	if bucket != "" {
		context += " bucket=" + bucket
	}
	if key != "" {
		context += " key=" + key
	}
	return fmt.Errorf("%s: %w", context, err)
}

// wrapErrWith 为哨兵错误附加上下文信息，同时保持 errors.Is 可判定。
func wrapErrWith(sentinel error, msg string) error {
	if sentinel == nil {
		return errors.New(msg)
	}
	if msg == "" {
		return sentinel
	}
	return fmt.Errorf("%w: %s", sentinel, msg)
}

// ValidateKey 校验七牛对象 key：非空、长度 <= MaxKeyLength 字节、不含控制字符/NUL。
//
// 为什么必须校验：
//   - 空 key 在 upToken 中会退化成 scope=bucket，等于允许上传到桶内任意 key（越权）；
//   - 控制字符与 NUL 会破坏 URL / 签名；
//   - 七牛 scope 有长度上限，超长 key 会导致签名失败且难以定位。
//
// 控制字符用 unicode.IsControl 判定，覆盖 C0（0x00-0x1F）、DEL（0x7F）与
// C1（U+0080-U+009F）：只查 ASCII 会放过 C1，它们同样是合法的 UTF-8 但会
// 污染 URL / HTTP 头。
func ValidateKey(key string) error {
	if key == "" {
		return wrapErrWith(ErrInvalidKey, "key 不能为空")
	}
	if len(key) > MaxKeyLength {
		return wrapErrWith(ErrInvalidKey, fmt.Sprintf("key 长度 %d 超过上限 %d 字节", len(key), MaxKeyLength))
	}
	for _, r := range key {
		if r == 0 || unicode.IsControl(r) {
			return wrapErrWith(ErrInvalidKey, "key 含控制字符或 NUL")
		}
	}
	return nil
}
