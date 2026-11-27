package kodo

import (
	"context"
	"fmt"
	"net/http"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/qiniu/go-sdk/v7/auth/qbox"
	qclient "github.com/qiniu/go-sdk/v7/client"
	"github.com/qiniu/go-sdk/v7/storage"
)

// clientState 记录插件的关闭状态。Init 会创建新 state 并把旧的标记为关闭，
// 这样 Close 之前已经取出的 *Bucket 引用在重新 Init 后不会「意外复活」。
type clientState struct {
	closed   atomic.Bool
	disabled atomic.Bool
}

// isClosed 判断是否已关闭（nil state 视为未关闭，便于零值可用）
func (s *clientState) isClosed() bool {
	return s != nil && s.closed.Load()
}

// markClosed 标记为已关闭
func (s *clientState) markClosed() {
	if s != nil {
		s.closed.Store(true)
	}
}

// isDisabled 判断是否被配置显式禁用（enabled: false）
func (s *clientState) isDisabled() bool {
	return s != nil && s.disabled.Load()
}

// markDisabled 标记为被配置显式禁用
func (s *clientState) markDisabled() {
	if s != nil {
		s.disabled.Store(true)
	}
}

// kodoClient 是七牛 Kodo 对象存储插件，实现 mgin v1(jh) 的 MginPlugin 接口
// （Init([]byte) / Close() / Check() error）。
//
// 用法：
//
//	mgin.UsePlugin("kodo", kodo.NewKodo())
//	// 或手动初始化：
//	k := kodo.NewKodo()
//	k.Init(configBytes) // configBytes 为 go.kodo 节点的 yaml 字节
//
// 业务侧取桶（务必判空，或用 TryGet/TryDefault 拿到明确错误）：
//
//	b := kodo.GetKodo().Default()
//	if b == nil {
//	    // 未初始化或未配置桶
//	}
//	b, err := kodo.GetKodo().TryGet("avatar")
//	b.Upload(ctx, "users/1.png", data, "image/png")
type kodoClient struct {
	// lifecycleMu 串行化 Init/Close，保证不会出现两个 Init 交叉写入
	lifecycleMu sync.Mutex
	// clientMu 保护以下字段的读写（Init/Close 写，Default/Get 读）
	clientMu       sync.RWMutex
	cfg            *Config
	mac            *qbox.Mac
	storageCfg     *storage.Config
	bucketManager  *storage.BucketManager
	formUploader   *storage.FormUploader
	resumeUploader *storage.ResumeUploader
	buckets        map[string]*Bucket
	order          []string
	httpClient     *http.Client
	state          *clientState
}

var (
	instance *kodoClient
	once     sync.Once
)

// NewKodo 创建（或返回单例）七牛 Kodo 存储插件
func NewKodo() *kodoClient {
	once.Do(func() {
		instance = &kodoClient{
			buckets: make(map[string]*Bucket),
			state:   &clientState{},
		}
	})
	return instance
}

// GetKodo 返回全局 kodoClient 插件单例
func GetKodo() *kodoClient {
	return NewKodo()
}

// Init 实现 mgin v1(jh) MginPlugin 接口，解析配置并建立客户端。
// configData 为 go.kodo 节点对应的 yaml 字节。
//
// 语义保证：
//   - **幂等**：重复 Init 会整体重建，不会重复 append 桶顺序；
//   - **原子**：先构建到局部变量，全部成功后才在写锁内整体替换；
//   - **失败不留半初始化状态**：解析/校验失败时保留旧状态（首次失败则保持未初始化）；
//   - **并发安全**：写锁保护，与 Default/Get 的读锁互斥。
func (k *kodoClient) Init(configData []byte) {
	if k == nil {
		logError("[kodo] Init 调用在 nil client 上，忽略")
		return
	}
	k.lifecycleMu.Lock()
	defer k.lifecycleMu.Unlock()
	defer func() {
		if r := recover(); r != nil {
			logError(fmt.Sprintf("[kodo] panic in Init: %s\n%s", panicText(r), debug.Stack()))
		}
	}()

	cfg, err := ParseConfig(configData)
	if err != nil {
		logError("[kodo] 初始化失败（保留原有状态）: " + errorText(err))
		return
	}
	// 显式 enabled: false：跳过初始化，保留桶定义但所有操作返回 ErrDisabled
	if cfg.Disabled() {
		k.applyDisabled(cfg)
		return
	}
	// 新 state 与新桶共用同一实例：Close 后旧桶引用同步失效
	newState := &clientState{}
	prepared, err := buildClientResources(cfg, newState, true)
	if err != nil {
		logError("[kodo] 初始化失败（保留原有状态）: " + errorText(err))
		return
	}

	oldState, oldHTTP := k.setResources(cfg, prepared, newState)

	// 旧连接池与旧 state 在锁外回收
	if oldState != nil {
		oldState.markClosed()
	}
	if oldHTTP != nil && oldHTTP != prepared.httpClient {
		oldHTTP.CloseIdleConnections()
	}

	logInfo("[kodo] 存储插件已初始化, zone=" + cfg.Zone + ", 桶=" + strings.Join(prepared.order, ","))
}

// setResources 在写锁内整体替换运行时资源，返回被替换掉的旧 state 与旧 http.Client。
//
// 为什么单独抽出来：解锁必须走 defer。若临界区内发生 panic（Init/Close 的
// recover 会吞掉它），一旦跳过了 Unlock，clientMu 就永久处于锁定状态，
// 之后 Default/Get/Ready/Names 全部死锁 —— 一次被成功 recover 的 panic
// 会演变成插件永久不可用，比 panic 本身更难排查。
//
// res 为 nil 表示「清空所有运行时资源」（Close 路径）。
func (k *kodoClient) setResources(cfg *Config, res *clientResources, state *clientState) (oldState *clientState, oldHTTP *http.Client) {
	k.clientMu.Lock()
	defer k.clientMu.Unlock()

	oldState, oldHTTP = k.state, k.httpClient
	if res == nil {
		k.cfg = nil
		k.mac = nil
		k.storageCfg = nil
		k.bucketManager = nil
		k.formUploader = nil
		k.resumeUploader = nil
		k.buckets = make(map[string]*Bucket)
		k.order = nil
		k.httpClient = nil
	} else {
		k.cfg = cfg
		k.mac = res.mac
		k.storageCfg = res.storageCfg
		k.bucketManager = res.bucketManager
		k.formUploader = res.formUploader
		k.resumeUploader = res.resumeUploader
		k.buckets = res.buckets
		k.order = res.order
		k.httpClient = res.httpClient
	}
	k.state = state
	return oldState, oldHTTP
}

// clientResources 是一次 Init 构建出的全部资源（用于原子替换）
type clientResources struct {
	mac            *qbox.Mac
	storageCfg     *storage.Config
	bucketManager  *storage.BucketManager
	formUploader   *storage.FormUploader
	resumeUploader *storage.ResumeUploader
	buckets        map[string]*Bucket
	order          []string
	httpClient     *http.Client
}

// buildClientResources 构建客户端资源。该函数不做网络请求。
func buildClientResources(cfg *Config, state *clientState, networkEnabled bool) (*clientResources, error) {
	if cfg == nil {
		return nil, wrapErrWith(ErrInvalidConfig, "配置为空")
	}
	if state == nil {
		state = &clientState{}
	}
	if !networkEnabled {
		return buildDisabledResources(cfg, state)
	}
	mac := qbox.NewMac(cfg.AccessKey, cfg.SecretKey)

	storageCfg := &storage.Config{
		UseHTTPS:      cfg.UseHTTPS,
		UseCdnDomains: cfg.UseCdnDomains,
	}
	if z := resolveZone(cfg.Zone); z != nil {
		storageCfg.Zone = z
		storageCfg.Region = z
	}

	httpClient, managerClient := newHTTPClients(cfg)
	// 通过 Ex 构造函数注入自建 http.Client —— 这是控制连接池的唯一入口。
	// 管理类 API 使用带总超时的客户端，避免 v1 SDK 无 ctx 时永久挂起。
	sdkClient := &qclient.Client{Client: httpClient}
	managerSDKClient := &qclient.Client{Client: managerClient}
	bucketManager := storage.NewBucketManagerEx(mac, storageCfg, managerSDKClient)
	formUploader := storage.NewFormUploaderEx(storageCfg, sdkClient)
	resumeUploader := storage.NewResumeUploaderEx(storageCfg, sdkClient)

	buckets := make(map[string]*Bucket)
	order := make([]string, 0, len(cfg.Buckets)+1)
	for _, def := range cfg.bucketDefs() {
		domain, domainErr := resolveDomain(def.domain, cfg.UseHTTPS)
		if domainErr != nil {
			logError("[kodo] 桶 " + def.name + " 的 domain 配置非法，跳过该桶: " + domainErr.Error())
			continue
		}
		if _, dup := buckets[def.name]; dup {
			logWarn("[kodo] 桶 " + def.name + " 重复配置，仅保留第一个")
			continue
		}
		buckets[def.name] = &Bucket{
			name:           def.name,
			domain:         domain,
			isPrivate:      def.private,
			defaultCT:      def.defaultContentType,
			mac:            mac,
			cfg:            cfg,
			bucketManager:  bucketManager,
			formUploader:   formUploader,
			resumeUploader: resumeUploader,
			httpClient:     httpClient,
			state:          state,
		}
		order = append(order, def.name)
	}
	if len(order) == 0 {
		return nil, wrapErrWith(ErrInvalidConfig, "没有任何可用桶（请检查 domain 配置）")
	}
	// Bucket 中的 state 必须与 client 使用同一个实例，便于 Close 后统一失效
	return &clientResources{
		mac:            mac,
		storageCfg:     storageCfg,
		bucketManager:  bucketManager,
		formUploader:   formUploader,
		resumeUploader: resumeUploader,
		buckets:        buckets,
		order:          order,
		httpClient:     httpClient,
	}, nil
}

// buildDisabledResources 构建「仅保留桶定义、不含任何鉴权/网络资源」的禁用态资源。
// 这样 Get/Default 仍能返回桶对象，其所有方法统一返回 ErrDisabled，语义一致。
func buildDisabledResources(cfg *Config, state *clientState) (*clientResources, error) {
	buckets := make(map[string]*Bucket)
	order := make([]string, 0, len(cfg.Buckets)+1)
	for _, def := range cfg.bucketDefs() {
		if def.name == "" {
			continue
		}
		if _, dup := buckets[def.name]; dup {
			continue
		}
		domain, domainErr := resolveDomain(def.domain, cfg.UseHTTPS)
		if domainErr != nil {
			logError("[kodo] 桶 " + def.name + " 的 domain 配置非法，跳过该桶: " + domainErr.Error())
			continue
		}
		buckets[def.name] = &Bucket{
			name:      def.name,
			domain:    domain,
			isPrivate: def.private,
			defaultCT: def.defaultContentType,
			cfg:       cfg,
			state:     state,
		}
		order = append(order, def.name)
	}
	return &clientResources{buckets: buckets, order: order}, nil
}

// applyDisabled 把插件切换到「显式禁用」状态：清空凭据与网络资源，保留桶定义。
func (k *kodoClient) applyDisabled(cfg *Config) {
	newState := &clientState{}
	newState.markDisabled()
	prepared, err := buildDisabledResources(cfg, newState)
	if err != nil {
		logError("[kodo] 禁用态构建失败: " + errorText(err))
		return
	}

	oldState, oldHTTP := k.setResources(cfg, prepared, newState)

	if oldState != nil {
		// 仅标记为「被禁用取代」，不标记 closed：旧桶引用应得到 ErrDisabled 而非 ErrClosed
		oldState.markDisabled()
	}
	if oldHTTP != nil {
		oldHTTP.CloseIdleConnections()
	}
	logWarn("[kodo] 配置 enabled=false，插件已显式禁用（所有操作返回 ErrDisabled）")
}

// Default 返回默认桶（buckets 第一个，或 singleBucket）。未初始化时返回 nil。
func (k *kodoClient) Default() *Bucket {
	if k == nil {
		return nil
	}
	k.clientMu.RLock()
	defer k.clientMu.RUnlock()
	if len(k.order) == 0 {
		return nil
	}
	return k.buckets[k.order[0]]
}

// Get 按桶名返回桶客户端，不存在（或未初始化）时返回 nil。
// 调用方应判空；需要明确错误请用 TryGet。
func (k *kodoClient) Get(name string) *Bucket {
	if k == nil {
		return nil
	}
	k.clientMu.RLock()
	defer k.clientMu.RUnlock()
	b, ok := k.buckets[name]
	if !ok {
		return nil
	}
	return b
}

// TryDefault 返回默认桶，并给出明确错误（未初始化 / 未配置桶 / 已关闭）。
func (k *kodoClient) TryDefault() (*Bucket, error) {
	if err := k.Ready(); err != nil {
		return nil, err
	}
	if b := k.Default(); b != nil {
		return b, nil
	}
	return nil, wrapErrWith(ErrNotInitialized, "没有可用的默认桶")
}

// TryGet 按桶名返回桶客户端，并给出明确错误（便于区分「未初始化」与「桶不存在」）。
func (k *kodoClient) TryGet(name string) (*Bucket, error) {
	if err := k.Ready(); err != nil {
		return nil, err
	}
	if b := k.Get(name); b != nil {
		return b, nil
	}
	return nil, wrapErrWith(ErrBucketNotFound, name)
}

// Names 返回已配置的桶名（按配置顺序）。未初始化时返回 nil。
func (k *kodoClient) Names() []string {
	if k == nil {
		return nil
	}
	k.clientMu.RLock()
	defer k.clientMu.RUnlock()
	if len(k.order) == 0 {
		return nil
	}
	names := make([]string, len(k.order))
	copy(names, k.order)
	return names
}

// Ready 判断插件是否已完成初始化且未被关闭。该方法不发起网络请求。
func (k *kodoClient) Ready() error {
	if k == nil {
		return ErrNotInitialized
	}
	k.clientMu.RLock()
	defer k.clientMu.RUnlock()
	if k.state.isClosed() {
		return ErrClosed
	}
	if k.state.isDisabled() {
		return ErrDisabled
	}
	if k.cfg == nil || k.mac == nil || k.bucketManager == nil || k.httpClient == nil {
		return ErrNotInitialized
	}
	if len(k.order) == 0 {
		return ErrNotInitialized
	}
	return nil
}

// IsDisabled 判断插件是否被配置显式禁用（enabled: false）。
func (k *kodoClient) IsDisabled() bool {
	if k == nil {
		return false
	}
	k.clientMu.RLock()
	defer k.clientMu.RUnlock()
	return k.state.isDisabled()
}

// IsClosed 判断插件是否已 Close 且尚未重新 Init。
func (k *kodoClient) IsClosed() bool {
	if k == nil {
		return true
	}
	k.clientMu.RLock()
	defer k.clientMu.RUnlock()
	return k.state.isClosed()
}

// Close 实现 mgin v1(jh) MginPlugin 接口。
// 关闭后所有操作返回 ErrClosed；再次 Init 可恢复可用。
func (k *kodoClient) Close() {
	if k == nil {
		return
	}
	k.lifecycleMu.Lock()
	defer k.lifecycleMu.Unlock()
	defer func() {
		if r := recover(); r != nil {
			logError(fmt.Sprintf("[kodo] panic in Close: %s\n%s", panicText(r), debug.Stack()))
		}
	}()

	closedState := &clientState{}
	closedState.markClosed()
	state, httpClient := k.setResources(nil, nil, closedState)

	if state != nil {
		state.markClosed()
	}
	if httpClient != nil {
		httpClient.CloseIdleConnections()
	}
	logInfo("[kodo] 存储插件已关闭")
}

// Check 实现 mgin v1(jh) MginPlugin 接口，对默认桶做一次 GetBucketInfo 探活。
func (k *kodoClient) Check() (err error) {
	defer recoverToError(&err, "Check")
	if err := k.Ready(); err != nil {
		return err
	}
	b := k.Default()
	if b == nil {
		return ErrNotInitialized
	}
	ctx, cancel := context.WithTimeout(context.Background(), DefaultCheckTimeout)
	defer cancel()
	return b.Check(ctx)
}

// ---------------------------------------------------------------- Bucket

// Bucket 代表一个七牛 Kodo 桶的客户端，提供上传/下载/删除/列举/预签名/分片上传等操作。
//
// 所有方法都是「nil 安全」的：当 *Bucket 为 nil（例如 k.Get("x") 返回 nil）或插件
// 已关闭时，会返回 ErrNotInitialized/ErrClosed 而不是 panic；Name/Domain/IsPrivate
// 返回零值。
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
	state          *clientState
}

// Name 返回桶名（nil 接收者返回空串）
func (b *Bucket) Name() string {
	if b == nil {
		return ""
	}
	return b.name
}

// Domain 返回该桶的访问域名（nil 接收者返回空串）
func (b *Bucket) Domain() string {
	if b == nil {
		return ""
	}
	return b.domain
}

// IsPrivate 是否私有空间（nil 接收者返回 false）
func (b *Bucket) IsPrivate() bool {
	if b == nil {
		return false
	}
	return b.isPrivate
}

// ready 校验桶是否可用（nil 接收者、未初始化、已关闭）
func (b *Bucket) ready() error {
	if b == nil {
		return wrapErrWith(ErrNotInitialized, "nil bucket（请检查 Get/Default 的返回值是否为空）")
	}
	if b.state.isClosed() {
		return wrapErrWith(ErrClosed, "bucket "+b.name)
	}
	if b.state.isDisabled() {
		return wrapErrWith(ErrDisabled, "bucket "+b.name)
	}
	if b.cfg == nil || b.mac == nil {
		return wrapErrWith(ErrNotInitialized, "bucket "+b.name)
	}
	if b.httpClient == nil {
		return wrapErrWith(ErrNotInitialized, "bucket "+b.name+" 缺少 http client")
	}
	return nil
}

// readyManager 校验管理类 API（Stat/Delete/List/Check）所需资源
func (b *Bucket) readyManager() error {
	if err := b.ready(); err != nil {
		return err
	}
	if b.bucketManager == nil {
		return wrapErrWith(ErrNotInitialized, "bucket "+b.name+" 缺少 bucket manager")
	}
	return nil
}

// readyUploader 校验上传类 API 所需资源
func (b *Bucket) readyUploader() error {
	if err := b.ready(); err != nil {
		return err
	}
	if b.formUploader == nil || b.resumeUploader == nil {
		return wrapErrWith(ErrNotInitialized, "bucket "+b.name+" 缺少 uploader")
	}
	return nil
}

// resolveContentType 推断内容类型
func (b *Bucket) resolveContentType(key string) string {
	if b == nil {
		return "application/octet-stream"
	}
	if b.defaultCT != "" {
		return b.defaultCT
	}
	return guessContentType(key)
}

// shouldResume 是否启用分片上传
func (b *Bucket) shouldResume(size int64) bool {
	threshold := DefaultResumeThreshold
	if b != nil && b.cfg != nil && b.cfg.ResumeThreshold > 0 {
		threshold = b.cfg.ResumeThreshold
	}
	return size > threshold
}

// resolveChunkSize 计算分片大小：显式参数优先，其次配置，最后交给 SDK 默认（0）
func (b *Bucket) resolveChunkSize(partSize int64) int {
	size := partSize
	if size <= 0 && b != nil && b.cfg != nil {
		size = b.cfg.ChunkSize
	}
	if size <= 0 {
		return 0
	}
	const maxInt32 = int64(^uint32(0) >> 1)
	if size > maxInt32 {
		size = maxInt32
	}
	if size > HardMaxChunkSize {
		// SDK 会为每个并发分片分配 make([]byte, 0, ChunkSize)，
		// 配置/参数误写（如 1GB）会造成「并发数 × ChunkSize」级别的内存占用。
		logWarn(fmt.Sprintf("[kodo] 分片大小 %d 字节超过硬上限 %d 字节，已钳制", size, HardMaxChunkSize))
		size = HardMaxChunkSize
	}
	if size < 1024*1024 || size > 8*1024*1024 {
		logWarn(fmt.Sprintf("[kodo] 分片大小 %d 字节不在建议区间 [1MiB, 8MiB]，可能导致上传失败", size))
	}
	return int(size)
}

// accessURL 构造访问 URL：私有空间带下载凭证，公有空间直接拼接。
// 使用 MakePublicURLv2/MakePrivateURLv2（会对 key 做 escape），避免 key 含
// 空格、'?'、'#' 时 URL 错误，以及 '?' 注入篡改签名参数。
func (b *Bucket) accessURL(key string) (string, error) {
	if b == nil {
		return "", ErrNotInitialized
	}
	if b.domain == "" {
		return "", wrapErrWith(ErrNotInitialized, "bucket "+b.name+" 未配置 domain")
	}
	if b.isPrivate {
		if b.mac == nil || b.cfg == nil {
			return "", wrapErrWith(ErrNotInitialized, "bucket "+b.name)
		}
		deadline := time.Now().Add(b.cfg.presignDuration()).Unix()
		return storage.MakePrivateURLv2(b.mac, b.domain, key, deadline), nil
	}
	return storage.MakePublicURLv2(b.domain, key), nil
}

// uploadToken 生成上传凭证，scope 强制限定为 bucket:key（禁止桶级越权上传）。
func (b *Bucket) uploadToken(key string) (string, error) {
	if b == nil || b.mac == nil || b.cfg == nil {
		return "", ErrNotInitialized
	}
	if err := ValidateKey(key); err != nil {
		return "", err
	}
	putPolicy := storage.PutPolicy{Scope: b.name + ":" + key}
	// PutPolicy.Expires 是相对秒数（SDK 内部会加上当前时间），此处用法正确
	putPolicy.Expires = b.cfg.presignExpirySeconds()
	if putPolicy.Expires == 0 {
		putPolicy.Expires = uint64(DefaultPresignExpiry)
	}
	return putPolicy.UploadToken(b.mac), nil
}
