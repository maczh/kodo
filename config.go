package kodo

import "time"

// Config 是七牛 Kodo 存储插件的完整配置，对应 application.yml 中 go.kodo 节点。
//
// 多 bucket：buckets 为空时自动以顶层 domain/singleBucket 工作（单桶模式）；
// buckets 非空时通过 Get(name) 区分不同用途的桶（如公有资源桶 / 私有文件桶）。
//
// 与 S3 的核心差异：七牛的每个桶必须绑定一个访问域名（domain），且区分公有/私有空间。
type Config struct {
	// Enabled 是否启用（go.config.used 已包含 kodo 时仍可用此开关二次控制）
	Enabled bool `yaml:"enabled"`
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
	// UseHTTPS 是否使用 HTTPS 访问，默认 true
	UseHTTPS bool `yaml:"useHTTPS"`
	// UseCdnDomains 上传是否使用 CDN 加速域名，默认 false
	UseCdnDomains bool `yaml:"useCdnDomains"`
	// PresignExpiry 预签名 URL（私有下载/上传凭证）默认有效期（秒），默认 3600
	PresignExpiry int `yaml:"presignExpiry"`
	// ResumeThreshold 超过该字节数启用分片（断点续传）上传，<=0 时按 SDK 默认（约 4MB 以上自动分片）
	ResumeThreshold int64 `yaml:"resumeThreshold"`
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

// normalize 填充默认值
func (c *Config) normalize() {
	if c.Zone == "" {
		c.Zone = "huadong"
	}
	if !c.UseHTTPS && !hasScheme(c.Domain) {
		// 未显式关闭 HTTPS 且域名无显式协议时，默认走 HTTPS
		c.UseHTTPS = true
	}
	if c.PresignExpiry <= 0 {
		c.PresignExpiry = 3600
	}
	if c.ResumeThreshold < 0 {
		c.ResumeThreshold = 0
	}
}

// presignDuration 返回预签名有效期
func (c *Config) presignDuration() time.Duration {
	return time.Duration(c.PresignExpiry) * time.Second
}

// isHTTP 仅判断字符串是否以 http:// 或 https:// 开头
func isHTTP(s string) bool {
	return hasScheme(s)
}

// hasScheme 判断字符串是否带 http:// / https:// 协议前缀
func hasScheme(s string) bool {
	return len(s) >= 7 && (s[:7] == "http://" || (len(s) >= 8 && s[:8] == "https://"))
}

// ensureScheme 给域名补全协议前缀（按 UseHTTPS 决定 http/https）；已带协议则原样返回
func ensureScheme(domain string, useHTTPS bool) string {
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
