package kodo

import (
	"fmt"
	"math"
	"net/url"
	"runtime/debug"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// 插件默认值。所有默认值均可通过配置覆盖（除硬上限外）。
const (
	// DefaultResumeThreshold 未配置 resumeThreshold 时，超过该字节数自动改用分片（断点续传）上传
	DefaultResumeThreshold int64 = 100 * 1024 * 1024 // 100MB

	// DefaultMaxUploadMemory 未知长度上传流在内存中允许缓冲的最大字节数
	DefaultMaxUploadMemory int64 = 64 * 1024 * 1024 // 64MB
	// HardMaxUploadMemory 上传内存缓冲的绝对硬上限（配置无法突破）。
	// bufferUnknownSize=true 时整个流会先读进内存，配置误写（如 1TB）会直接 OOM；
	// 超过该上限的流应当走默认的流式分片上传（PutWithoutSize）。
	HardMaxUploadMemory int64 = 256 * 1024 * 1024 // 256MiB
	// HardMaxChunkSize 分片上传单个分片大小的硬上限。
	// SDK 会为每个并发分片分配 make([]byte, 0, ChunkSize)，配置成 1GB 时
	// 并发数 × 1GB 直接 OOM；七牛实际推荐 4MB，这里给到 8MB 已足够。
	HardMaxChunkSize int64 = 8 * 1024 * 1024 // 8MiB

	// HardMaxDownloadSize 单次「流式下载」允许写入的最大字节数（硬上限，任何配置都无法突破）。
	// 该上限用于 Download(io.WriterAt) / DownloadFile 这类不占内存的路径。
	HardMaxDownloadSize int64 = 8 * 1024 * 1024 * 1024 // 8GiB

	// DefaultMaxDownloadBytes DownloadBytes / DownloadMany 单个对象允许在内存中
	// 累积的默认字节数。内存下载的成本是「对象大小 × 并发数」，默认必须远小于
	// 流式上限，否则一个 5GiB 对象就能把进程打爆（Go 的 OOM 是 runtime throw，
	// recover 兜不住）。
	DefaultMaxDownloadBytes int64 = 64 * 1024 * 1024 // 64MiB
	// HardMaxDownloadBytes 内存下载单对象上限的绝对硬上限（配置无法突破）
	HardMaxDownloadBytes int64 = 1 << 30 // 1GiB
	// DefaultMaxDownloadManyBytes DownloadMany 全部结果在内存中的默认总上限。
	// DownloadMany 会把所有对象同时保留在内存里返回，因此必须限制「总和」而不只是单对象。
	DefaultMaxDownloadManyBytes int64 = 256 * 1024 * 1024 // 256MiB
	// HardMaxDownloadManyBytes DownloadMany 总上限的绝对硬上限
	HardMaxDownloadManyBytes int64 = 2 << 30 // 2GiB

	// DefaultPresignExpiry 预签名 URL / 上传凭证的默认有效期（秒）
	DefaultPresignExpiry int = 3600

	// DefaultDialTimeout TCP 建连超时
	DefaultDialTimeout = 10 * time.Second
	// DefaultResponseHeaderTimeout 等待 HTTP 响应头的超时（不限制 body 读取总时长，避免大文件下载被截断）
	DefaultResponseHeaderTimeout = 30 * time.Second
	// DefaultIdleConnTimeout 空闲连接回收时间
	DefaultIdleConnTimeout = 90 * time.Second
	// DefaultTLSHandshakeTimeout TLS 握手超时
	DefaultTLSHandshakeTimeout = 10 * time.Second
	// DefaultManageTimeout 管理类 API（Stat/Delete/List/Check）的单次请求总超时。
	// 与上传/下载不同：管理类响应很小，若服务端「响应头已下发、body 卡住」，
	// 没有总超时会永久挂住（七牛 v1 SDK 不接受 ctx，无法靠调用方取消）。
	DefaultManageTimeout = 60 * time.Second
	// DefaultExpectContinueTimeout Expect: 100-continue 等待时间
	DefaultExpectContinueTimeout = 1 * time.Second
	// DefaultKeepAlive TCP keepalive 周期
	DefaultKeepAlive = 30 * time.Second

	// DefaultMaxIdleConns 连接池最大空闲连接数
	DefaultMaxIdleConns = 100
	// DefaultMaxIdleConnsPerHost 每个 host 最大空闲连接数
	DefaultMaxIdleConnsPerHost = 16

	// DefaultRetryMax 幂等操作默认最大重试次数（即最多 3 次尝试）
	DefaultRetryMax = 2
	// DefaultRetryBaseDelay 退避基数
	DefaultRetryBaseDelay = 200 * time.Millisecond
	// MaxRetryDelay 单次退避上限
	MaxRetryDelay = 2 * time.Second

	// DefaultConcurrency 批量操作默认并发度
	DefaultConcurrency = 8
	// MaxConcurrency 批量操作并发硬上限
	MaxConcurrency = 64

	// MaxKeyLength 七牛对象 key 的最大字节数（受 putPolicy scope 长度限制）
	MaxKeyLength = 750

	// DefaultCheckTimeout Check 探活的默认超时
	DefaultCheckTimeout = 15 * time.Second

	// DefaultListLimit List 单页默认/最大条数（七牛 ListFiles 上限为 1000）
	DefaultListLimit = 1000

	// DefaultMaxListObjects ListAll 默认允许累积的对象条数。
	// ListAll 把全部结果驻留内存，百万级对象按每项约 100~300 字节估算就是数百 MB，
	// append 扩容期间还会短暂翻倍；因此必须有默认上限。
	// 需要遍历超大桶请用 ListEach / ListEachPage（流式，不累积）。
	DefaultMaxListObjects = 100_000
	// HardMaxListObjects ListAll 条数的绝对硬上限（配置无法突破）。
	// 超过该量级时即使配置允许，也建议改用流式遍历。
	HardMaxListObjects = 1_000_000

	// MaxRetryCount 重试次数硬上限。
	// 防止配置误写（如 retryMax: 1000000000）把幂等操作变成长时间重试风暴。
	MaxRetryCount = 10

	// MaxConfigSeconds 秒级配置的取值上限（约 68 年）。
	// 这些值最终会乘以 time.Second 转成 Duration，超过 math.MaxInt32 秒会 int64 溢出，
	// 溢出后可能变成负数（被当作「无超时」）或极小的错误值，属于静默配置失效。
	MaxConfigSeconds = math.MaxInt32
)

// Config 是七牛 Kodo 存储插件的完整配置，对应 application.yml 中 go.kodo 节点。
//
// 多 bucket：buckets 为空时自动以顶层 domain/singleBucket 工作（单桶模式）；
// buckets 非空时通过 Get(name) 区分不同用途的桶（如公有资源桶 / 私有文件桶）。
//
// 与 S3 的核心差异：七牛的每个桶必须绑定一个访问域名（domain），且区分公有/私有空间。
//
// 手工构造配置请使用 DefaultConfig()，否则 useHTTPS 等默认值不会自动生效：
//
//	cfg := kodo.DefaultConfig()
//	cfg.AccessKey = "..."
//	cfg.SingleBucket = "mybucket"
//	cfg.Domain = "https://cdn.example.com"
//	cfg.Normalize()
type Config struct {
	// Enabled 是否启用。nil（键缺省）= 启用；显式 enabled: false 表示禁用，
	// Init 会跳过所有初始化，插件对外统一返回 ErrDisabled（Check/Ready 亦然）。
	// 注意：字段为 *bool 且属于破坏性类型变更（历史版本为 bool，且从未被读取过）。
	Enabled *bool `yaml:"enabled"`
	// AccessKey 七牛账号的 AccessKey（密钥管理页面获取）
	AccessKey string `yaml:"accessKey"`
	// SecretKey 七牛账号的 SecretKey
	SecretKey string `yaml:"secretKey"`
	// Zone 存储区域，支持以下写法（不区分大小写）：
	//   z0 / huadong          华东
	//   cn-east-2 / huadong2  华东-浙江2
	//   z1 / huabei           华北
	//   z2 / huanan           华南
	//   na0 / beimei          北美
	//   as0 / xinjiapo        东南亚（新加坡）
	// 留空默认 huadong。
	Zone string `yaml:"zone"`
	// UseHTTPS 是否使用 HTTPS 访问，默认 true。
	// 注意：该字段不会被 normalize 改写，显式配置 false 即生效。
	UseHTTPS bool `yaml:"useHTTPS"`
	// UseCdnDomains 上传是否使用 CDN 加速域名，默认 false
	UseCdnDomains bool `yaml:"useCdnDomains"`
	// PresignExpiry 预签名 URL（私有下载/上传凭证）默认有效期（秒），默认 3600
	PresignExpiry int `yaml:"presignExpiry"`
	// ResumeThreshold 超过该字节数启用分片（断点续传）上传，默认 100MB
	ResumeThreshold int64 `yaml:"resumeThreshold"`
	// ChunkSize 分片上传的单个分片大小（字节），<=0 时使用 SDK 默认值（4MB）。
	// 七牛服务端对分片偏移有对齐要求，实际使用建议为 4MB 的倍数。
	ChunkSize int64 `yaml:"chunkSize"`
	// MaxUploadMemory 未知长度上传流允许在内存中缓冲的最大字节数，默认 64MB。
	// 仅在 bufferUnknownSize=true 时生效（默认走流式分片上传，不占用额外内存）。
	MaxUploadMemory int64 `yaml:"maxUploadMemory"`
	// BufferUnknownSize 是否把未知长度的上传流先读入内存再走表单上传，默认 false。
	// 开启后超过 maxUploadMemory 会返回 ErrUploadTooLarge（大文件请改用 UploadFile）。
	BufferUnknownSize bool `yaml:"bufferUnknownSize"`
	// MaxDownloadSize 单次下载允许的最大字节数，0 表示仅受硬上限约束。
	// 对 Download/DownloadFile 为 8GiB 硬上限；对 DownloadBytes/DownloadMany
	// 会与 maxDownloadBytes 取较小值。
	MaxDownloadSize int64 `yaml:"maxDownloadSize"`
	// MaxDownloadBytes DownloadBytes 允许在内存中累积的单个对象最大字节数，默认 64MiB。
	// 0 表示使用默认值；超过硬上限 1GiB 会被钳制。大对象请改用
	// Download(io.WriterAt) 或 DownloadFile（流式落盘，不占内存）。
	MaxDownloadBytes int64 `yaml:"maxDownloadBytes"`
	// MaxDownloadManyBytes DownloadMany 返回的全部内容在内存中的总上限，默认 256MiB，
	// 硬上限 2GiB。DownloadMany 会把所有对象同时驻留内存，因此需要约束总和。
	MaxDownloadManyBytes int64 `yaml:"maxDownloadManyBytes"`
	// MaxListObjects ListAll 允许累积的对象条数，默认 100000，硬上限 1000000。
	// 超限返回 ErrListTooLarge；遍历超大桶请改用 ListEach / ListEachPage（流式）。
	MaxListObjects int `yaml:"maxListObjects"`
	// DialTimeout TCP 建连超时（秒），默认 10
	DialTimeout int `yaml:"dialTimeout"`
	// ResponseHeaderTimeout 等待响应头的超时（秒），默认 30。不限制 body 读取总时长。
	ResponseHeaderTimeout int `yaml:"responseHeaderTimeout"`
	// IdleConnTimeout 空闲连接回收时间（秒），默认 90
	IdleConnTimeout int `yaml:"idleConnTimeout"`
	// MaxIdleConns 连接池最大空闲连接数，默认 100
	MaxIdleConns int `yaml:"maxIdleConns"`
	// MaxIdleConnsPerHost 每个 host 最大空闲连接数，默认 16
	MaxIdleConnsPerHost int `yaml:"maxIdleConnsPerHost"`
	// ManageTimeout 管理类 API（Stat/Delete/List/Check）的单次请求总超时（秒），默认 60。
	// 上传/下载不受该值影响（大文件总时长交给调用方 ctx）。
	ManageTimeout int `yaml:"manageTimeout"`
	// EnableHTTP2 是否为 SDK 注入的 http.Client 启用 HTTP/2，默认 false。
	// 关闭原因：qiniu go-sdk v7.27.0 的 internal/uplog 在 HTTP/2 下由 httptrace 回调
	// 跨 goroutine 读写同一变量（SDK 内部数据竞争），Go 的 -race 会直接报错。
	// 对象存储场景使用 HTTP/1.1 + 连接池即可；如需 HTTP/2 可显式开启。
	EnableHTTP2 bool `yaml:"enableHTTP2"`
	// RetryMax 幂等操作（Stat/Delete/List/Check/Download）最大重试次数。
	// 默认 2（最多 3 次尝试），0 表示禁用重试。上传因 body 已消费不做自动重试。
	RetryMax int `yaml:"retryMax"`
	// RetryBaseDelayMs 重试退避基数（毫秒），默认 200
	RetryBaseDelayMs int `yaml:"retryBaseDelayMs"`
	// MaxConcurrency 批量操作（DeleteMany/DownloadMany）默认并发度，默认 8，硬上限 64
	MaxConcurrency int `yaml:"maxConcurrency"`
	// SingleBucket 单桶模式桶名
	SingleBucket string `yaml:"singleBucket"`
	// Domain 单桶模式的访问域名（如 http://example.com 或 https://cdn.example.com）
	Domain string `yaml:"domain"`
	// Private 单桶模式是否私有空间，默认 false（公有空间）
	Private bool `yaml:"private"`
	// DefaultContentType 单桶模式上传时未指定 ContentType 的默认值
	DefaultContentType string `yaml:"defaultContentType"`
	// Buckets 多桶配置
	Buckets []BucketConfig `yaml:"buckets"`
}

// BucketConfig 单个桶的配置
type BucketConfig struct {
	// Name 桶名（必填）
	Name string `yaml:"name"`
	// Domain 该桶的访问域名（必填，如 https://cdn.example.com）
	Domain string `yaml:"domain"`
	// Private 是否为私有空间（影响下载是否需要下载凭证），默认 false
	Private bool `yaml:"private"`
	// DefaultContentType 该桶上传时未指定 ContentType 时的默认值
	DefaultContentType string `yaml:"defaultContentType"`
}

// DefaultConfig 返回带默认值的配置。手动构造配置时应以此为起点。
//
// 之所以单独提供该函数：解析 yaml 时以 DefaultConfig() 为目标对象，才能让
// 「未出现的 useHTTPS 键」解析为 true，而「显式写 useHTTPS: false」保持 false。
func DefaultConfig() *Config {
	cfg := &Config{
		UseHTTPS:              true,
		Zone:                  "huadong",
		PresignExpiry:         DefaultPresignExpiry,
		ResumeThreshold:       0,
		ChunkSize:             0,
		MaxUploadMemory:       DefaultMaxUploadMemory,
		MaxDownloadSize:       0,
		MaxDownloadBytes:      DefaultMaxDownloadBytes,
		MaxDownloadManyBytes:  DefaultMaxDownloadManyBytes,
		MaxListObjects:        DefaultMaxListObjects,
		DialTimeout:           int(DefaultDialTimeout / time.Second),
		ResponseHeaderTimeout: int(DefaultResponseHeaderTimeout / time.Second),
		IdleConnTimeout:       int(DefaultIdleConnTimeout / time.Second),
		MaxIdleConns:          DefaultMaxIdleConns,
		MaxIdleConnsPerHost:   DefaultMaxIdleConnsPerHost,
		ManageTimeout:         int(DefaultManageTimeout / time.Second),
		RetryMax:              DefaultRetryMax,
		RetryBaseDelayMs:      int(DefaultRetryBaseDelay / time.Millisecond),
		MaxConcurrency:        DefaultConcurrency,
	}
	cfg.normalize()
	return cfg
}

// ParseConfig 解析 yaml 配置字节并校验必填项。
// 解析目标为 DefaultConfig()，因此未出现的键使用默认值。
//
// 该函数是导出 API，可能被业务直接调用，因此也做了 panic 兜底：
// yaml 库面对畸形输入理论上返回 error，但插件边界不允许「理论上」。
func ParseConfig(configData []byte) (cfg *Config, err error) {
	defer func() {
		if r := recover(); r != nil {
			logError(fmt.Sprintf("[kodo] panic in ParseConfig: %s\n%s", panicText(r), debug.Stack()))
			cfg, err = nil, fmt.Errorf("kodo: panic in ParseConfig: %s", panicText(r))
		}
	}()
	cfg = DefaultConfig()
	if len(configData) > 0 {
		if err := yaml.Unmarshal(configData, cfg); err != nil {
			return nil, wrapErrWith(ErrInvalidConfig, "解析 yaml 失败: "+err.Error())
		}
	}
	cfg.normalize()
	// 显式 enabled: false 时不再要求账号与桶配置（禁用状态下不会发起任何请求）
	if !cfg.Disabled() {
		if cfg.AccessKey == "" || cfg.SecretKey == "" {
			return nil, wrapErrWith(ErrInvalidConfig, "accessKey/secretKey 未配置")
		}
		if len(cfg.bucketDefs()) == 0 {
			return nil, wrapErrWith(ErrInvalidConfig, "未配置 buckets 也未配置 singleBucket")
		}
	}
	return cfg, nil
}

// Disabled 判断配置是否显式禁用（enabled: false）。键缺省（nil）视为启用。
func (c *Config) Disabled() bool {
	return c != nil && c.Enabled != nil && !*c.Enabled
}

// Normalize 填充默认值。该方法不会改写 UseHTTPS（显式配置 false 会被尊重）。
func (c *Config) Normalize() { c.normalize() }

// normalize 填充默认值，供内部调用。
func (c *Config) normalize() {
	if c == nil {
		return
	}
	if c.Zone == "" {
		c.Zone = "huadong"
	}
	if c.PresignExpiry <= 0 {
		c.PresignExpiry = DefaultPresignExpiry
	}
	if c.PresignExpiry > MaxConfigSeconds {
		c.PresignExpiry = MaxConfigSeconds
	}
	if c.ResumeThreshold < 0 {
		c.ResumeThreshold = 0
	}
	if c.ChunkSize < 0 {
		c.ChunkSize = 0
	}
	if c.MaxUploadMemory <= 0 {
		c.MaxUploadMemory = DefaultMaxUploadMemory
	}
	if c.MaxUploadMemory > HardMaxUploadMemory {
		// 与下载不同，这里是「缓冲未知长度流」，配置误写会直接把进程撑爆
		c.MaxUploadMemory = HardMaxUploadMemory
	}
	if c.MaxDownloadSize < 0 {
		c.MaxDownloadSize = 0
	}
	if c.MaxDownloadBytes <= 0 {
		c.MaxDownloadBytes = DefaultMaxDownloadBytes
	}
	if c.MaxDownloadBytes > HardMaxDownloadBytes {
		// 配置无法突破硬上限：内存下载一旦真的按配置分配，就是不可 recover 的 OOM
		c.MaxDownloadBytes = HardMaxDownloadBytes
	}
	if c.MaxDownloadManyBytes <= 0 {
		c.MaxDownloadManyBytes = DefaultMaxDownloadManyBytes
	}
	if c.MaxDownloadManyBytes > HardMaxDownloadManyBytes {
		c.MaxDownloadManyBytes = HardMaxDownloadManyBytes
	}
	if c.MaxDownloadManyBytes < c.MaxDownloadBytes {
		// 总预算不能小于单对象预算，否则连一个对象都下不下来
		c.MaxDownloadManyBytes = c.MaxDownloadBytes
	}
	if c.MaxListObjects <= 0 {
		c.MaxListObjects = DefaultMaxListObjects
	}
	if c.MaxListObjects > HardMaxListObjects {
		// 条数上限同样是内存保护：ListAll 的返回切片会整体驻留内存
		c.MaxListObjects = HardMaxListObjects
	}
	if c.DialTimeout <= 0 {
		c.DialTimeout = int(DefaultDialTimeout / time.Second)
	}
	if c.DialTimeout > MaxConfigSeconds {
		c.DialTimeout = MaxConfigSeconds
	}
	if c.ResponseHeaderTimeout <= 0 {
		c.ResponseHeaderTimeout = int(DefaultResponseHeaderTimeout / time.Second)
	}
	if c.ResponseHeaderTimeout > MaxConfigSeconds {
		c.ResponseHeaderTimeout = MaxConfigSeconds
	}
	if c.IdleConnTimeout <= 0 {
		c.IdleConnTimeout = int(DefaultIdleConnTimeout / time.Second)
	}
	if c.IdleConnTimeout > MaxConfigSeconds {
		c.IdleConnTimeout = MaxConfigSeconds
	}
	if c.MaxIdleConns <= 0 {
		c.MaxIdleConns = DefaultMaxIdleConns
	}
	if c.MaxIdleConnsPerHost <= 0 {
		c.MaxIdleConnsPerHost = DefaultMaxIdleConnsPerHost
	}
	if c.ManageTimeout <= 0 {
		c.ManageTimeout = int(DefaultManageTimeout / time.Second)
	}
	if c.ManageTimeout > MaxConfigSeconds {
		c.ManageTimeout = MaxConfigSeconds
	}
	if c.RetryMax < 0 {
		c.RetryMax = 0
	}
	if c.RetryMax > MaxRetryCount {
		c.RetryMax = MaxRetryCount
	}
	if c.RetryBaseDelayMs <= 0 {
		c.RetryBaseDelayMs = int(DefaultRetryBaseDelay / time.Millisecond)
	}
	if c.MaxConcurrency <= 0 {
		c.MaxConcurrency = DefaultConcurrency
	}
	if c.MaxConcurrency > MaxConcurrency {
		c.MaxConcurrency = MaxConcurrency
	}
}

// presignDuration 返回预签名有效期。
// 先钳制到 MaxConfigSeconds 再乘 time.Second，避免 int64 溢出（溢出后会变成
// 负数或极小值，导致私有 URL 一生成就过期）。
func (c *Config) presignDuration() time.Duration {
	if c == nil || c.PresignExpiry <= 0 {
		return time.Duration(DefaultPresignExpiry) * time.Second
	}
	sec := c.PresignExpiry
	if sec > MaxConfigSeconds {
		sec = MaxConfigSeconds
	}
	return time.Duration(sec) * time.Second
}

// presignExpirySeconds 返回上传凭证有效期（秒），至少 1 秒
func (c *Config) presignExpirySeconds() uint64 {
	sec := int64(c.presignDuration() / time.Second)
	if sec <= 0 {
		sec = int64(DefaultPresignExpiry)
	}
	return uint64(sec)
}

// dialTimeout 返回 TCP 建连超时
func (c *Config) dialTimeout() time.Duration {
	if c == nil || c.DialTimeout <= 0 {
		return DefaultDialTimeout
	}
	return time.Duration(clampSeconds(c.DialTimeout)) * time.Second
}

// responseHeaderTimeout 返回响应头超时
func (c *Config) responseHeaderTimeout() time.Duration {
	if c == nil || c.ResponseHeaderTimeout <= 0 {
		return DefaultResponseHeaderTimeout
	}
	return time.Duration(clampSeconds(c.ResponseHeaderTimeout)) * time.Second
}

// idleConnTimeout 返回空闲连接回收时间
func (c *Config) idleConnTimeout() time.Duration {
	if c == nil || c.IdleConnTimeout <= 0 {
		return DefaultIdleConnTimeout
	}
	return time.Duration(clampSeconds(c.IdleConnTimeout)) * time.Second
}

// manageTimeout 返回管理类 API 的单次请求总超时。
func (c *Config) manageTimeout() time.Duration {
	if c == nil || c.ManageTimeout <= 0 {
		return DefaultManageTimeout
	}
	return time.Duration(clampSeconds(c.ManageTimeout)) * time.Second
}

// clampSeconds 把秒级配置钳制到不会让 time.Duration 溢出的区间。
func clampSeconds(v int) int {
	if v > MaxConfigSeconds {
		return MaxConfigSeconds
	}
	return v
}

// enableHTTP2 是否启用 HTTP/2（默认关闭，见 EnableHTTP2 字段说明）
func (c *Config) enableHTTP2() bool {
	return c != nil && c.EnableHTTP2
}

// maxIdleConns 返回连接池最大空闲连接数
func (c *Config) maxIdleConns() int {
	if c == nil || c.MaxIdleConns <= 0 {
		return DefaultMaxIdleConns
	}
	return c.MaxIdleConns
}

// maxIdleConnsPerHost 返回每 host 最大空闲连接数
func (c *Config) maxIdleConnsPerHost() int {
	if c == nil || c.MaxIdleConnsPerHost <= 0 {
		return DefaultMaxIdleConnsPerHost
	}
	return c.MaxIdleConnsPerHost
}

// retryBaseDelay 返回重试退避基数（单次退避本身受 MaxRetryDelay 约束，这里同步钳制基数）
func (c *Config) retryBaseDelay() time.Duration {
	if c == nil || c.RetryBaseDelayMs <= 0 {
		return DefaultRetryBaseDelay
	}
	ms := c.RetryBaseDelayMs
	if maxMs := int(MaxRetryDelay / time.Millisecond); ms > maxMs {
		ms = maxMs
	}
	return time.Duration(ms) * time.Millisecond
}

// downloadLimit 返回单次「流式下载」允许的最大字节数（不会超过硬上限 8GiB）。
// 该上限只约束写入量，不约束内存（目标通常是文件或流）。
func (c *Config) downloadLimit() int64 {
	limit := HardMaxDownloadSize
	if c != nil && c.MaxDownloadSize > 0 && c.MaxDownloadSize < limit {
		limit = c.MaxDownloadSize
	}
	return limit
}

// memoryDownloadLimit 返回「在内存中下载单个对象」允许的最大字节数。
//
// 与 downloadLimit 分开的原因：DownloadBytes 把整个对象放在 []byte 里，
// 8GiB 的流式上限放在这里等于允许进程直接 OOM（runtime throw，recover 兜不住）。
// 因此内存路径默认只给 64MiB，并且配置最多只能抬到 1GiB。
// maxDownloadSize 仍然生效（取两者较小值），保持历史语义兼容。
func (c *Config) memoryDownloadLimit() int64 {
	limit := DefaultMaxDownloadBytes
	if c != nil && c.MaxDownloadBytes > 0 {
		limit = c.MaxDownloadBytes
	}
	if limit > HardMaxDownloadBytes {
		limit = HardMaxDownloadBytes
	}
	if c != nil && c.MaxDownloadSize > 0 && c.MaxDownloadSize < limit {
		limit = c.MaxDownloadSize
	}
	return limit
}

// downloadManyLimit 返回 DownloadMany 全部结果允许驻留内存的总字节数。
func (c *Config) downloadManyLimit() int64 {
	limit := DefaultMaxDownloadManyBytes
	if c != nil && c.MaxDownloadManyBytes > 0 {
		limit = c.MaxDownloadManyBytes
	}
	if limit > HardMaxDownloadManyBytes {
		limit = HardMaxDownloadManyBytes
	}
	if per := c.memoryDownloadLimit(); limit < per {
		limit = per
	}
	return limit
}

// listObjectLimit 返回 ListAll 允许累积的对象条数（不会超过硬上限）。
func (c *Config) listObjectLimit() int {
	limit := DefaultMaxListObjects
	if c != nil && c.MaxListObjects > 0 {
		limit = c.MaxListObjects
	}
	if limit > HardMaxListObjects {
		limit = HardMaxListObjects
	}
	return limit
}

// bucketDef 是解析后的桶定义（单桶模式会被规整为一条）
type bucketDef struct {
	name               string
	domain             string
	private            bool
	defaultContentType string
}

// bucketDefs 返回全部已配置的桶定义。名称为空的桶会被忽略。
func (c *Config) bucketDefs() []bucketDef {
	if c == nil {
		return nil
	}
	defs := make([]bucketDef, 0, len(c.Buckets)+1)
	if len(c.Buckets) == 0 {
		if c.SingleBucket != "" {
			defs = append(defs, bucketDef{
				name:               c.SingleBucket,
				domain:             c.Domain,
				private:            c.Private,
				defaultContentType: c.DefaultContentType,
			})
		}
		return defs
	}
	for _, b := range c.Buckets {
		if b.Name == "" {
			continue
		}
		defs = append(defs, bucketDef{
			name:               b.Name,
			domain:             b.Domain,
			private:            b.Private,
			defaultContentType: b.DefaultContentType,
		})
	}
	return defs
}

// hasScheme 判断字符串是否带 http:// / https:// 协议前缀（不区分大小写）
func hasScheme(s string) bool {
	lower := strings.ToLower(s)
	return strings.HasPrefix(lower, "http://") || strings.HasPrefix(lower, "https://")
}

// resolveDomain 校验并规范化桶的访问域名。
//
// 规则：
//   - 只允许 http / https 两种 scheme，其它（file、gopher、ftp 等）一律判为非法配置；
//   - 禁止 userinfo（user:pass@host）、query、fragment —— 既会破坏 key 拼接，也会泄漏凭据；
//   - 已显式带 scheme 时原样保留（不做静默改写）；若 useHTTPS=true 但显式写了 http://，
//     属于降级，打 warn 但按用户显式配置使用；
//   - 无 scheme 时按 useHTTPS 补全。
//
// 返回的错误信息不会回显原始 domain（可能含 userinfo），避免把凭据写进日志。
func resolveDomain(domain string, useHTTPS bool) (string, error) {
	trimmed := strings.TrimSpace(domain)
	if trimmed == "" {
		return "", wrapErrWith(ErrInvalidConfig, "domain 为空")
	}
	// 只有显式出现 "://" 才认为带了 scheme；否则 host:port 形式会被误判
	if !strings.Contains(trimmed, "://") {
		trimmed = ensureScheme(trimmed, useHTTPS)
	}
	parsed, err := url.Parse(trimmed)
	if err != nil {
		// 不回显原始 domain：可能含 userinfo 凭据
		return "", wrapErrWith(ErrInvalidConfig, "domain 无法解析")
	}
	scheme := strings.ToLower(parsed.Scheme)
	if scheme == "" {
		return "", wrapErrWith(ErrInvalidConfig, "domain 缺少协议，仅支持 http/https")
	}
	if scheme != "http" && scheme != "https" {
		return "", wrapErrWith(ErrInvalidConfig, "domain 协议只允许 http/https，实际为 "+parsed.Scheme)
	}
	if parsed.User != nil {
		return "", wrapErrWith(ErrInvalidConfig, "domain 不允许包含 userinfo（账号密码）")
	}
	if parsed.Host == "" {
		return "", wrapErrWith(ErrInvalidConfig, "domain 缺少主机名")
	}
	if parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", wrapErrWith(ErrInvalidConfig, "domain 不允许包含 query 或 fragment")
	}
	if scheme == "http" && useHTTPS {
		logWarn("[kodo] useHTTPS=true 但 domain 显式配置为 http，按显式配置使用（明文传输）")
	}
	return strings.TrimRight(trimmed, "/"), nil
}

// ensureScheme 给域名补全协议前缀（按 UseHTTPS 决定 http/https）；已带协议则原样返回
func ensureScheme(domain string, useHTTPS bool) string {
	domain = strings.TrimRight(strings.TrimSpace(domain), "/")
	if domain == "" {
		return ""
	}
	if hasScheme(domain) {
		return domain
	}
	if useHTTPS {
		return "https://" + domain
	}
	return "http://" + domain
}
