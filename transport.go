package kodo

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"strings"
	"syscall"
	"time"

	qclient "github.com/qiniu/go-sdk/v7/client"
	"github.com/qiniu/go-sdk/v7/storage"
)

// newHTTPClients 构建插件共用的两个 HTTP 客户端，二者**共享同一个 Transport**
// （同一 host 只保留一个连接池）：
//
//   - data：上传/下载使用，**不设置总超时**。旧实现用 Timeout=5min，该超时覆盖
//     body 读取，导致任何超过 5 分钟的大文件下载必然失败；现在 body 读取总时长
//     交给调用方 ctx，ResponseHeaderTimeout 只约束「等响应头」。
//   - manager：管理类 API（Stat/Delete/List/Check）使用，带总超时。七牛 v1 管理
//     API 不接受 ctx，只能靠 http.Client.Timeout 兜住「响应头已到、body 卡住」
//     导致的永久挂起。
func newHTTPClients(cfg *Config) (data, manager *http.Client) {
	transport := newHTTPTransport(cfg)
	data = &http.Client{Transport: transport}
	manager = &http.Client{Transport: transport, Timeout: cfg.manageTimeout()}
	return data, manager
}

// newHTTPTransport 构建连接池参数。上传/下载与管理类 API 共用同一份实现。
func newHTTPTransport(cfg *Config) *http.Transport {
	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   cfg.dialTimeout(),
			KeepAlive: DefaultKeepAlive,
		}).DialContext,
		ForceAttemptHTTP2:     cfg.enableHTTP2(),
		MaxIdleConns:          cfg.maxIdleConns(),
		MaxIdleConnsPerHost:   cfg.maxIdleConnsPerHost(),
		IdleConnTimeout:       cfg.idleConnTimeout(),
		TLSHandshakeTimeout:   DefaultTLSHandshakeTimeout,
		ExpectContinueTimeout: DefaultExpectContinueTimeout,
		ResponseHeaderTimeout: cfg.responseHeaderTimeout(),
	}
	if !cfg.enableHTTP2() {
		// 显式关闭 HTTP/2：自定义 Transport 默认不会协商 h2，但显式置空
		// TLSNextProto 可确保不会被自动升级（规避 SDK uplog 的 HTTP/2 数据竞争）。
		transport.TLSNextProto = make(map[string]func(string, *tls.Conn) http.RoundTripper)
	}
	return transport
}

// retryPolicy 描述幂等操作的重试策略。
type retryPolicy struct {
	// max 最大重试次数（不含首次尝试）
	max int
	// base 退避基数
	base time.Duration
}

// retryPolicy 由配置推导出重试策略。
func (c *Config) retryPolicy() retryPolicy {
	maxRetry := DefaultRetryMax
	base := DefaultRetryBaseDelay
	if c != nil {
		maxRetry = c.RetryMax
		base = c.retryBaseDelay()
	}
	if maxRetry < 0 {
		maxRetry = 0
	}
	if maxRetry > MaxRetryCount {
		// 配置误写（如 retryMax: 1e9）会让幂等操作陷入长时间重试风暴
		maxRetry = MaxRetryCount
	}
	return retryPolicy{max: maxRetry, base: base}
}

// retryDo 执行幂等操作，遇可重试错误时按指数退避 + 抖动重试。
//
// 仅用于 Stat/Delete/List/Check/Download 这类幂等操作；上传的 body 已被消费，
// 重试会写入残缺内容，因此上传路径不调用该函数。
func retryDo(ctx context.Context, policy retryPolicy, fn func() error) error {
	if fn == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	maxRetry := policy.max
	if maxRetry < 0 {
		maxRetry = 0
	}
	if maxRetry > MaxRetryCount {
		maxRetry = MaxRetryCount
	}
	var lastErr error
	for attempt := 0; attempt <= maxRetry; attempt++ {
		if err := ctx.Err(); err != nil {
			if lastErr != nil {
				return lastErr
			}
			return err
		}
		err := fn()
		if err == nil {
			return nil
		}
		lastErr = err
		if attempt == maxRetry || !isRetryable(err) {
			return err
		}
		if !sleepBackoff(ctx, policy.base, attempt+1) {
			return lastErr
		}
	}
	return lastErr
}

// sleepBackoff 退避等待，返回 false 表示 ctx 已结束、应停止重试。
func sleepBackoff(ctx context.Context, base time.Duration, attempt int) bool {
	if base <= 0 {
		base = DefaultRetryBaseDelay
	}
	delay := base
	if delay > MaxRetryDelay {
		delay = MaxRetryDelay
	}
	for i := 1; i < attempt; i++ {
		delay *= 2
		if delay >= MaxRetryDelay {
			delay = MaxRetryDelay
			break
		}
	}
	if delay > MaxRetryDelay {
		delay = MaxRetryDelay
	}
	// 0.5x ~ 1.0x 抖动，避免重试风暴同步
	half := delay / 2
	if half <= 0 {
		half = delay
	}
	jitter := time.Duration(rand.Int64N(int64(half) + 1))
	timer := time.NewTimer(half + jitter)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// isRetryable 判断错误是否值得重试。
//
// 可重试：网络超时/临时错误、连接被重置、io.ErrUnexpectedEOF、HTTP 5xx、429。
// 不可重试：ctx 取消、HTTP 4xx（含 400/401/403/404/612 等业务错误）。
func isRetryable(err error) (retryable bool) {
	// 纯判定函数，输入是任意第三方错误，判定失败退化为「不重试」，
	// 不允许因 Unwrap/Error 的异常实现把调用方打崩。
	defer func() {
		if recover() != nil {
			retryable = false
		}
	}()
	// typed-nil 不能进 errors.Is/Error()：其 Unwrap/Error 方法可能在 nil 接收者上 panic
	if err == nil || isNilValue(err) {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	if IsNotFound(err) || errors.Is(err, storage.ErrBucketNotExist) {
		return false
	}
	// 明确的 HTTP 状态错误
	var se *statusError
	if errors.As(err, &se) && se != nil {
		return se.retryable()
	}
	// 七牛 SDK 的 HTTP 错误（Code 即 HTTP 状态码）
	var ei *qclient.ErrorInfo
	if errors.As(err, &ei) && ei != nil {
		switch {
		case ei.Code == 429 || ei.Code >= 500:
			return true
		case ei.Code >= 400:
			return false
		}
	}
	if errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	if errors.Is(err, net.ErrClosed) {
		return true
	}
	if errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.ECONNREFUSED) ||
		errors.Is(err, syscall.EPIPE) || errors.Is(err, syscall.ETIMEDOUT) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr != nil {
		if netErr.Timeout() {
			return true
		}
		//nolint:staticcheck // Temporary 已废弃但对 net 包实现仍有效
		if netErr.Temporary() {
			return true
		}
	}
	msg := strings.ToLower(errorText(err))
	for _, fragment := range []string{
		"connection reset",
		"broken pipe",
		"unexpected eof",
		"i/o timeout",
		"temporarily unavailable",
		"server closed idle connection",
		"connection refused",
	} {
		if strings.Contains(msg, fragment) {
			return true
		}
	}
	return false
}
