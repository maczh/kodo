package kodo

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/qiniu/go-sdk/v7/auth/qbox"
	"github.com/qiniu/go-sdk/v7/storage"
	"github.com/sadlil/gologger"
	"gopkg.in/yaml.v3"
)

// kodoClient 是七牛 Kodo 对象存储插件，实现 mgin.MginPlugin 接口。
//
// 用法：
//
//	mgin.UsePlugin("kodo", kodo.NewKodo())
//	// 或手动初始化：
//	k := kodo.NewKodo()
//	k.Init(configBytes) // configBytes 为 go.kodo 节点的 yaml 字节
//
// 业务侧取桶：
//
//	b := kodo.GetKodo().Default()       // 默认桶
//	b := kodo.GetKodo().Get("avatar")   // 指定桶
//	b.Upload(ctx, "users/1.png", data, "image/png")
type kodoClient struct {
	cfg           *Config
	mac           *qbox.Mac
	bucketManager *storage.BucketManager
	buckets       map[string]*Bucket
	order         []string
	clientMu      sync.RWMutex
}

var (
	instance *kodoClient
	once     sync.Once
)

var logger = gologger.GetLogger()

// defaultResumeThreshold 当未配置 ResumeThreshold 时，超过该大小自动改用分片（断点续传）上传
const defaultResumeThreshold int64 = 100 * 1024 * 1024 // 100MB

// NewKodo 创建（或返回单例）七牛 Kodo 存储插件
func NewKodo() *kodoClient {
	once.Do(func() {
		instance = &kodoClient{buckets: make(map[string]*Bucket)}
	})
	return instance
}

// GetKodo 返回全局 kodoClient 插件单例
func GetKodo() *kodoClient {
	return NewKodo()
}

// Init 实现 mgin.MginPlugin 接口，解析配置并建立客户端。
// configData 为 go.kodo 节点对应的 yaml 字节。
func (k *kodoClient) Init(configData []byte) {
	if len(configData) == 0 {
		logger.Error("[kodo] 配置为空，未初始化")
		return
	}
	cfg := &Config{}
	if err := yaml.Unmarshal(configData, cfg); err != nil {
		logger.Error("[kodo] 配置解析失败: " + err.Error())
		return
	}
	cfg.normalize()
	if cfg.AccessKey == "" || cfg.SecretKey == "" {
		logger.Error("[kodo] accessKey/secretKey 未配置，Kodo 插件未初始化")
		return
	}
	k.cfg = cfg

	// 七牛鉴权
	k.mac = qbox.NewMac(cfg.AccessKey, cfg.SecretKey)

	// 存储配置
	storageCfg := storage.Config{
		UseHTTPS:      cfg.UseHTTPS,
		UseCdnDomains: cfg.UseCdnDomains,
	}
	if z := resolveZone(cfg.Zone); z != nil {
		storageCfg.Zone = z
	}

	// 桶管理器（Stat/Delete/List 等管理类 API 共用一个）
	k.bucketManager = storage.NewBucketManager(k.mac, &storageCfg)

	// 构建桶客户端
	bucketNames := make([]string, 0)
	if len(cfg.Buckets) == 0 {
		if cfg.SingleBucket == "" {
			logger.Error("[kodo] 未配置 buckets 也未配置 singleBucket，Kodo 插件未初始化")
			return
		}
		bucketNames = append(bucketNames, cfg.SingleBucket)
	} else {
		for _, b := range cfg.Buckets {
			if b.Name != "" {
				bucketNames = append(bucketNames, b.Name)
			}
		}
	}
	for _, name := range bucketNames {
		b := k.newBucket(name, &storageCfg)
		if b == nil {
			continue
		}
		k.buckets[name] = b
		k.order = append(k.order, name)
	}
	logger.Info("[kodo] 存储插件已初始化, zone=" + cfg.Zone + ", 桶=" + strings.Join(bucketNames, ","))
}

// newBucket 为指定桶名构造客户端与上传管理器
func (k *kodoClient) newBucket(name string, storageCfg *storage.Config) *Bucket {
	domain := ""
	isPrivate := false
	defaultCT := ""
	for _, b := range k.cfg.Buckets {
		if b.Name == name {
			domain = b.Domain
			isPrivate = b.Private
			defaultCT = b.DefaultContentType
			break
		}
	}
	// 单桶模式从顶层取 domain/private/defaultContentType
	if domain == "" && name == k.cfg.SingleBucket {
		domain = k.cfg.Domain
		isPrivate = k.cfg.Private
		defaultCT = k.cfg.DefaultContentType
	}
	if domain == "" {
		logger.Error("[kodo] 桶 " + name + " 未配置 domain（访问域名），跳过该桶")
		return nil
	}
	domain = ensureScheme(strings.TrimRight(domain, "/"), k.cfg.UseHTTPS)
	return &Bucket{
		name:           name,
		domain:         domain,
		isPrivate:      isPrivate,
		defaultCT:      defaultCT,
		mac:            k.mac,
		cfg:            k.cfg,
		bucketManager:  k.bucketManager,
		formUploader:   storage.NewFormUploader(storageCfg),
		resumeUploader: storage.NewResumeUploader(storageCfg),
		httpClient:     &http.Client{Timeout: 5 * time.Minute},
	}
}

// Default 返回默认桶（buckets 第一个，或 singleBucket）
func (k *kodoClient) Default() *Bucket {
	k.clientMu.RLock()
	defer k.clientMu.RUnlock()
	if len(k.order) == 0 {
		return nil
	}
	return k.buckets[k.order[0]]
}

// Get 按桶名返回桶客户端，不存在返回 nil
func (k *kodoClient) Get(name string) *Bucket {
	k.clientMu.RLock()
	defer k.clientMu.RUnlock()
	return k.buckets[name]
}

// Close 实现 mgin.MginPlugin 接口。Kodo 客户端无持久连接，此处仅做清理。
func (k *kodoClient) Close() {
	k.clientMu.Lock()
	k.buckets = make(map[string]*Bucket)
	k.order = nil
	k.clientMu.Unlock()
	logger.Info("[kodo] 存储插件已关闭")
}

// Check 实现 mgin.MginPlugin 接口，对默认桶做一次 GetBucketInfo 探活。
func (k *kodoClient) Check() error {
	b := k.Default()
	if b == nil {
		return errors.New("kodo 未初始化")
	}
	return b.Check(context.Background())
}

// ---------------------------------------------------------------- Bucket

// Bucket 代表一个七牛 Kodo 桶的客户端，提供上传/下载/删除/列举/预签名/分片上传等操作。
type Bucket struct {
	name           string
	domain         string
	isPrivate      bool
	defaultCT      string
	mac            *qbox.Mac
	cfg            *Config
	bucketManager  *storage.BucketManager
	formUploader   *storage.FormUploader
	resumeUploader *storage.ResumeUploader
	httpClient     *http.Client
}

// Name 返回桶名
func (b *Bucket) Name() string { return b.name }

// Domain 返回该桶的访问域名
func (b *Bucket) Domain() string { return b.domain }

// IsPrivate 是否私有空间
func (b *Bucket) IsPrivate() bool { return b.isPrivate }

// Check 探活：查询桶信息
func (b *Bucket) Check(ctx context.Context) error {
	_, err := b.bucketManager.GetBucketInfo(b.name)
	return wrapErr(err)
}

// PutObject / Put 别名：上传字节
func (b *Bucket) Put(ctx context.Context, key string, data []byte, contentType string) error {
	return b.Upload(ctx, key, bytes.NewReader(data), contentType)
}

// Upload 上传一个 Reader 内容到指定 key。
// contentType 为空时使用桶的默认类型，再为空时按扩展名推断，仍为空则 application/octet-stream。
// 内部按文件大小自动选择表单上传（FormUploader）或分片断点续传上传（ResumeUploader）。
func (b *Bucket) Upload(ctx context.Context, key string, body io.Reader, contentType string) error {
	if contentType == "" {
		contentType = b.resolveContentType(key)
	}
	reader, size, err := asBytesReader(body)
	if err != nil {
		return wrapErr(err)
	}
	upToken := b.uploadToken(key)
	ret := &storage.PutRet{}
	if b.shouldResume(size) {
		rputExtra := &storage.RputExtra{MimeType: contentType}
		err = b.resumeUploader.Put(ctx, ret, upToken, key, reader, size, rputExtra)
	} else {
		putExtra := &storage.PutExtra{MimeType: contentType}
		err = b.formUploader.Put(ctx, ret, upToken, key, reader, size, putExtra)
	}
	return wrapErr(err)
}

// UploadBytes 便捷方法：直接上传字节切片
func (b *Bucket) UploadBytes(ctx context.Context, key string, data []byte, contentType string) error {
	return b.Put(ctx, key, data, contentType)
}

// Download 将指定 key 的内容写入 w（io.WriterAt）。
// 通过该桶绑定的访问域名发起 HTTP 下载：私有空间使用带下载凭证的 URL，公有空间直接访问。
func (b *Bucket) Download(ctx context.Context, key string, w io.WriterAt) error {
	url, err := b.accessURL(ctx, key)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return wrapErr(err)
	}
	resp, err := b.httpClient.Do(req)
	if err != nil {
		return wrapErr(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return fmt.Errorf("download %s failed: %s", key, resp.Status)
	}
	aw := &writeAtWriter{w: w}
	if _, err := io.Copy(aw, resp.Body); err != nil {
		return wrapErr(err)
	}
	return nil
}

// DownloadBytes 下载指定 key 的完整内容并返回字节切片。
func (b *Bucket) DownloadBytes(ctx context.Context, key string) ([]byte, error) {
	buf := &writeAtBuf{}
	if err := b.Download(ctx, key, buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Delete 删除指定 key，不存在不报错。
func (b *Bucket) Delete(ctx context.Context, key string) error {
	err := b.bucketManager.Delete(b.name, key)
	if err != nil {
		if isNotFound(err) {
			return nil
		}
		return wrapErr(err)
	}
	return nil
}

// Exists 判断 key 是否存在
func (b *Bucket) Exists(ctx context.Context, key string) (bool, error) {
	_, err := b.bucketManager.Stat(b.name, key)
	if err != nil {
		if isNotFound(err) {
			return false, nil
		}
		return false, wrapErr(err)
	}
	return true, nil
}

// List 列举桶内对象，prefix 为前缀过滤，max 为最大返回数量（<=0 取 1000）。
// 返回的 ObjectInfo 含 Key/Size/LastModified/ETag/ContentType。
func (b *Bucket) List(ctx context.Context, prefix string, max int32) ([]ObjectInfo, error) {
	if max <= 0 {
		max = 1000
	}
	entries, _, _, _, err := b.bucketManager.ListFiles(b.name, prefix, "", "", int(max))
	if err != nil {
		return nil, wrapErr(err)
	}
	list := make([]ObjectInfo, 0, len(entries))
	for _, e := range entries {
		info := ObjectInfo{Key: e.Key, Size: e.Fsize, ETag: e.Hash, ContentType: e.MimeType}
		if e.PutTime > 0 {
			info.LastModified = putTimeToTime(e.PutTime)
		}
		list = append(list, info)
	}
	return list, nil
}

// Presign 生成下载预签名 URL，有效期使用配置的 presignExpiry。
// 私有空间返回带下载凭证的 URL；公有空间返回永久可访问的公开 URL。
func (b *Bucket) Presign(ctx context.Context, key string) (string, error) {
	return b.accessURL(ctx, key)
}

// PresignUpload 生成上传预签名凭证（upToken），客户端可凭此凭证直传到该桶。
// 有效期使用配置的 presignExpiry；scope 限定为 bucket:key，仅允许上传到指定 key。
func (b *Bucket) PresignUpload(ctx context.Context, key string) (string, error) {
	return b.uploadToken(key), nil
}

// UploadMultipart 分片（断点续传）上传。
// 返回七牛返回的文件 Hash（类 ETag）。partSize 参数七牛 v1 SDK 内部固定分片大小，
// 此处接受参数仅为对齐接口，实际分片粒度由 SDK 决定。
func (b *Bucket) UploadMultipart(ctx context.Context, key, contentType string, body io.Reader, partSize int64) (string, error) {
	if contentType == "" {
		contentType = b.resolveContentType(key)
	}
	reader, size, err := asBytesReader(body)
	if err != nil {
		return "", wrapErr(err)
	}
	_ = partSize
	upToken := b.uploadToken(key)
	rputExtra := &storage.RputExtra{MimeType: contentType}
	ret := &storage.PutRet{}
	if err := b.resumeUploader.Put(ctx, ret, upToken, key, reader, size, rputExtra); err != nil {
		return "", wrapErr(err)
	}
	return ret.Hash, nil
}

// accessURL 构造访问 URL：私有空间带下载凭证，公有空间直接拼接
func (b *Bucket) accessURL(ctx context.Context, key string) (string, error) {
	if !b.isPrivate {
		return storage.MakePublicURL(b.domain, key), nil
	}
	deadline := time.Now().Add(b.cfg.presignDuration()).Unix()
	return storage.MakePrivateURL(b.mac, b.domain, key, deadline), nil
}

// uploadToken 生成上传凭证，scope=bucket:key 限定只能上传到指定 key
func (b *Bucket) uploadToken(key string) string {
	scope := b.name
	if key != "" {
		scope = b.name + ":" + key
	}
	putPolicy := storage.PutPolicy{Scope: scope}
	putPolicy.Expires = uint64(b.cfg.presignDuration() / time.Second)
	if putPolicy.Expires == 0 {
		putPolicy.Expires = 3600
	}
	return putPolicy.UploadToken(b.mac)
}

// shouldResume 是否启用分片上传
func (b *Bucket) shouldResume(size int64) bool {
	if b.cfg.ResumeThreshold > 0 {
		return size > b.cfg.ResumeThreshold
	}
	return size > defaultResumeThreshold
}

// resolveContentType 推断内容类型
func (b *Bucket) resolveContentType(key string) string {
	if b.defaultCT != "" {
		return b.defaultCT
	}
	ext := strings.ToLower(filepath.Ext(key))
	switch ext {
	case ".png":
		return "image/png"
	case ".jpg", ".jpeg":
		return "image/jpeg"
	case ".gif":
		return "image/gif"
	case ".webp":
		return "image/webp"
	case ".svg":
		return "image/svg+xml"
	case ".pdf":
		return "application/pdf"
	case ".txt":
		return "text/plain"
	case ".html", ".htm":
		return "text/html"
	case ".json":
		return "application/json"
	case ".xml":
		return "application/xml"
	case ".zip":
		return "application/zip"
	case ".mp4":
		return "video/mp4"
	case ".mp3":
		return "audio/mpeg"
	case ".css":
		return "text/css"
	case ".js":
		return "application/javascript"
	default:
		return "application/octet-stream"
	}
}

// ObjectInfo 列举返回的对象摘要
type ObjectInfo struct {
	Key          string
	Size         int64
	LastModified time.Time
	ETag         string
	ContentType  string
}

// ---------------------------------------------------------------- 辅助

// asBytesReader 将任意 io.Reader 规整为 *bytes.Reader 及其长度。
// 已知长度的 *bytes.Reader / *bytes.Buffer 直接取长度；其余先读全到内存。
func asBytesReader(r io.Reader) (*bytes.Reader, int64, error) {
	if br, ok := r.(*bytes.Reader); ok {
		return br, int64(br.Len()), nil
	}
	if bb, ok := r.(*bytes.Buffer); ok {
		data := bb.Bytes()
		return bytes.NewReader(data), int64(len(data)), nil
	}
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, 0, err
	}
	return bytes.NewReader(data), int64(len(data)), nil
}

// writeAtWriter 将 io.WriterAt 适配为 io.Writer（流式写入，不占额外内存）
type writeAtWriter struct {
	w   io.WriterAt
	off int64
}

func (a *writeAtWriter) Write(p []byte) (int, error) {
	n, err := a.w.WriteAt(p, a.off)
	a.off += int64(n)
	return n, err
}

// writeAtBuf 简单的可增长 WriteAt 缓冲，用于 DownloadBytes
type writeAtBuf struct {
	buf []byte
}

func (b *writeAtBuf) WriteAt(p []byte, off int64) (int, error) {
	end := off + int64(len(p))
	if end > int64(len(b.buf)) {
		if int64(cap(b.buf)) < end {
			nb := make([]byte, len(b.buf), end*2)
			copy(nb, b.buf)
			b.buf = nb
		}
		b.buf = b.buf[:end]
	}
	copy(b.buf[off:], p)
	return len(p), nil
}

func (b *writeAtBuf) Bytes() []byte { return b.buf }

// putTimeToTime 七牛 PutTime 单位为 100 纳秒（自 1970-01-01 起），转换为 time.Time。
// 拆分秒与纳秒以避免直接相乘导致 int64 溢出。
func putTimeToTime(putTime int64) time.Time {
	sec := putTime / 1e7
	nsec := (putTime % 1e7) * 100
	return time.Unix(sec, nsec)
}

// isNotFound 判断七牛错误是否为「文件/资源不存在」
func isNotFound(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "no such file") ||
		strings.Contains(msg, "not exist") ||
		strings.Contains(msg, "not found") ||
		strings.Contains(msg, "612")
}

// resolveZone 将配置中的 zone 字符串映射为七牛 SDK 的 *storage.Zone
func resolveZone(zone string) *storage.Zone {
	switch strings.ToLower(zone) {
	case "z0", "huadong", "":
		return &storage.ZoneHuadong
	case "cn-east-2", "huadong2":
		return &storage.ZoneHuadongZheJiang2
	case "z1", "huabei":
		return &storage.ZoneHuabei
	case "z2", "huanan":
		return &storage.ZoneHuanan
	case "na0", "beimei":
		return &storage.ZoneBeimei
	case "as0", "xinjiapo", "singapore":
		return &storage.ZoneXinjiapo
	default:
		return &storage.ZoneHuadong
	}
}

// wrapErr 将存储错误原样返回（统一收敛点，便于后续扩展错误包装）
func wrapErr(err error) error {
	if err == nil {
		return nil
	}
	return err
}
