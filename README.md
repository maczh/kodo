# mgin kodo 插件

七牛云 Kodo 对象存储插件（qiniu go-sdk v7），提供上传/下载/删除/列举/预签名/分片上传等操作，支持多桶、公有/私有空间、自动区域选择。

支持两条 MGin 分支：

| 分支 | 接入方式 | 模块 |
| --- | --- | --- |
| v1(jh)（`MginPlugin`） | `app.MGin.UsePlugin("kodo", kodo.NewKodo())` | `github.com/maczh/kodo` |
| v2（`plugin.Plugin`） | `plugin.Register(mginv2.New())` | `github.com/maczh/kodo/mginv2` |

与 `mgin/s3` 插件保持一致的 API 形态，便于在不同对象存储之间切换；针对七牛的特性做了以下适配：

- 每个桶必须绑定一个**访问域名**（`domain`）
- 桶区分**公有空间**和**私有空间**（`private`），私有空间下载需要下载凭证
- 上传使用七牛的**上传凭证**（upToken）机制，`scope` 强制限定为 `bucket:key`
- 区域用 `zone`（z0/z1/z2/na0/as0 等）而非 region
- 大文件自动选用**断点续传上传**（ResumeUploader）

## 安装

```
go get github.com/maczh/kodo
```

### 挂载（v1 / jh 分支）

```go
app.MGin.UsePlugin("kodo", kodo.NewKodo())
```

编译期护栏见 `compat/mginv1`（`var _ mgin.MginPlugin = kodo.NewKodo()`）。

### 挂载（v2 分支）

```go
import (
    "github.com/maczh/kodo/mginv2"
    "github.com/maczh/mgin/v2/pkg/plugin"
)

plugin.Register(mginv2.New())   // 组件名 "kodo"
// 或自定义组件名（同时作为配置前缀）：plugin.Register(mginv2.NewNamed("kodo2"))
```

`mginv2.New()` 实现 v2 的 `plugin.Plugin`：

| 方法 | 行为 |
| --- | --- |
| `Name()` | `"kodo"` |
| `Order()` | `60`（晚于 DB/缓存/registry(30)，早于 job(90)） |
| `Init(ctx)` | 取配置 → legacy `Init` → `Ready()` 校验（不发网络请求）；`ctx` 已取消返回 `ctx.Err()` |
| `Close(ctx)` | legacy `Close` |
| `Health()` | legacy `Check`（对默认桶 `GetBucketInfo`） |
| `Enabled()` | `go.config.used` 是否包含组件名 |

配置来源优先级：`go.config.prefix.<name>` 指向的外部配置 → `application.yml` 内的 `go.<name>` 节点。

编译期护栏见 `compat/mginv2`（`var _ plugin.Plugin = mginv2.New()`）。

## 配置

在 `go.config.used` 中加入 `kodo`，配置范例：

```yaml
go:
  kodo:
    enabled: true                    #是否启用，缺省=true；显式 false 则整体禁用（见下）
    accessKey: your-access-key       #七牛账号 AccessKey（必填）
    secretKey: your-secret-key       #七牛账号 SecretKey（必填）
    zone: z0                         #存储区域，默认 huadong
    useHTTPS: true                   #是否 HTTPS，默认 true；显式 false 会被尊重
    useCdnDomains: false             #上传是否使用 CDN 加速，默认 false
    presignExpiry: 3600              #预签名/上传凭证有效期，秒，默认 3600
    resumeThreshold: 104857600       #超过该字节数改用分片上传，默认 100MB
    chunkSize: 4194304               #分片大小（字节），0 用 SDK 默认 4MB，硬上限 8MiB
    manageTimeout: 60                #管理类 API（Stat/Delete/List/Check）总超时，秒，默认 60
    singleBucket: mybucket           #单桶模式桶名
    domain: https://cdn.example.com  #单桶模式访问域名
    private: false                   #单桶模式是否私有空间，默认 false
    # buckets:                       #多桶模式（配置后忽略单桶字段）
    #   - name: public
    #     domain: https://pub.example.com
    #     private: false
    #   - name: private
    #     domain: https://priv.example.com
    #     private: true
    #     defaultContentType: application/octet-stream
```

> **域名（domain）必填**：七牛每个桶都必须绑定访问域名才能下载/预签名。单桶模式写在顶层 `domain`，多桶模式写在每个桶的 `domain`。未配置 domain 的桶会被跳过并打印错误日志；若全部被跳过，`Init` 保留旧的可用状态并记录错误。
>
> **domain 校验规则**：只允许 `http` / `https` 两种 scheme，`file://`、`gopher://`、`ftp://` 等一律判为非法配置并跳过该桶；不允许 `user:pass@host` 形式的 userinfo（避免凭据泄漏）以及 `?query` / `#fragment`（会破坏 key 拼接）。未写 scheme 时按 `useHTTPS` 补全；**已显式写 scheme 时原样保留**——若 `useHTTPS: true` 但 domain 写成 `http://`，视为明文降级，仅打 `warn` 不会静默改写你的配置。

### 新增/可调配置项（含默认值）

| 配置项 | 默认值 | 说明 |
| --- | --- | --- |
| `enabled` | 缺省（`nil`）= 启用 | `enabled: false` 时 `Init` 跳过初始化，所有操作统一返回 `ErrDisabled`。**字段类型已由 `bool` 改为 `*bool`**（历史版本该字段从未被读取，属修复性破坏变更） |
| `chunkSize` | `0`（SDK 默认 4MB） | 分片大小（字节）。七牛对分片偏移有对齐要求，建议取 4MB 的倍数。**硬上限 8MiB**：SDK 会为每个并发分片分配同等大小的缓冲，配置过大（如 1GB）会直接 OOM |
| `maxUploadMemory` | `67108864`（64MB） | `bufferUnknownSize=true` 时未知长度流允许缓冲的上限。**硬上限 256MiB**（配置无法突破），超过的流应走默认的流式分片上传 |
| `bufferUnknownSize` | `false` | 是否把未知长度流读入内存再走表单上传；默认走流式分片（不占内存） |
| `maxDownloadSize` | `0` | 单次下载字节上限；`0` 表示仅受硬上限约束（流式 8GiB / 内存路径见下）。对 `DownloadBytes` 与 `maxDownloadBytes` 取较小值 |
| `maxDownloadBytes` | `67108864`（64MiB） | **`DownloadBytes` 单个对象允许占用内存的上限**，配置最多可抬到 1GiB（硬上限）。超出返回 `ErrDownloadTooLarge`；大对象请用 `Download`/`DownloadFile` |
| `maxDownloadManyBytes` | `268435456`（256MiB） | **`DownloadMany` 全部结果允许占用的内存总上限**（硬上限 2GiB）。批量结果同时驻留内存，故限制总和；不足时后续 key 返回 `ErrDownloadTooLarge` |
| `maxListObjects` | `100000` | **`ListAll` 允许累积的对象条数**（硬上限 1000000）。超限返回 `ErrListTooLarge`，不会静默截断；遍历更大规模请用 `ListEach`/`ListEachPage`（流式，不累积） |
| `dialTimeout` | `10`（秒） | TCP 建连超时 |
| `responseHeaderTimeout` | `30`（秒） | 等待响应头超时；**不限制 body 读取总时长** |
| `idleConnTimeout` | `90`（秒） | 空闲连接回收 |
| `maxIdleConns` | `100` | 连接池最大空闲连接数 |
| `maxIdleConnsPerHost` | `16` | 每 host 最大空闲连接数 |
| `manageTimeout` | `60`（秒） | **管理类 API（Stat/Delete/List/Check）的单次请求总超时**。七牛 v1 管理 API 不接受 `ctx`，靠这个总超时兜住「响应头已到、body 卡住」的永久挂起。上传/下载不受影响（总时长交给 `ctx`） |
| `retryMax` | `2` | 幂等操作最大重试次数（最多 3 次尝试），`0` 关闭重试 |
| `retryBaseDelayMs` | `200` | 重试退避基数（指数退避 + 抖动） |
| `maxConcurrency` | `8` | 批量操作默认并发度（硬上限 64） |
| `enableHTTP2` | `false` | 是否为 SDK 注入的 `http.Client` 开启 HTTP/2，见下 |

> `enableHTTP2` 默认关闭的原因：qiniu go-sdk v7.27.0 的 `internal/uplog` 在 HTTP/2 下
> 由 `httptrace` 回调跨 goroutine 读写同一变量（SDK 内部数据竞争），`go test -race` 会直接报错。
> 对象存储场景使用 HTTP/1.1 + 连接池已足够，如确需 HTTP/2 可显式开启。

## 使用

```go
k := kodo.GetKodo()
if err := k.Ready(); err != nil {
    // 未初始化 / 未配置桶 / 已 Close
}

b := k.Default()          // 默认桶（未初始化时为 nil）
b = k.Get("private")      // 指定桶（不存在时为 nil）

b, err := k.TryGet("private")    // 需要明确错误时用 TryGet / TryDefault
```

### 基础操作

```go
err := b.Upload(ctx, "users/1.png", reader, "image/png")   // 大文件自动分片
err = b.UploadBytes(ctx, "users/1.png", data, "image/png")
err = b.UploadFile(ctx, "backup/big.zip", "/data/big.zip", "application/zip") // 大文件推荐
hash, err := b.UploadMultipart(ctx, "big.zip", "application/zip", reader, 8<<20) // 强制分片，可指定分片大小

data, err := b.DownloadBytes(ctx, "users/1.png")            // 内存下载，默认上限 64MiB
data, err = b.DownloadBytesLimit(ctx, "big.png", 200<<20)  // 单次放宽（硬上限 1GiB）
err = b.Download(ctx, "big.iso", writerAt)                 // 流式下载到 io.WriterAt（不占内存）
err = b.DownloadFile(ctx, "backup/big.zip", "/data/big.zip") // 流式落盘，大文件推荐
ok, err := b.Exists(ctx, "users/1.png")
info, err := b.Stat(ctx, "users/1.png")
err = b.Delete(ctx, "users/1.png")                         // 幂等，不存在不报错

url, err := b.Presign(ctx, "users/1.png")                  // 私有=带凭证 URL，公有=公开 URL
token, err := b.PresignUpload(ctx, "users/1.png")          // 上传凭证，scope 锁定到该 key
```

### 列举

```go
list, err := b.List(ctx, "users/", 1000)   // 单页语义，单页最多 1000 条
all, err := b.ListAll(ctx, "users/")       // 自动翻页取全量（受 maxListObjects 约束）
more, err := b.ListAllLimit(ctx, "users/", 500000) // 单次指定条数上限（钳到硬上限 100 万）

// 超大桶请用流式遍历：不累积结果，内存只与单页大小相关
err = b.ListEach(ctx, "users/", func(o kodo.ObjectInfo) error {
    return indexOne(o)   // 返回 error 立即中止遍历
})
err = b.ListEachPage(ctx, "users/", func(page []kodo.ObjectInfo) error {
    return bulkInsert(page)   // 按页回调，批量写库/批量删除用这个
})
```

`ListAll` 的结果整体驻留内存，因此默认上限 10 万条（`maxListObjects`，硬上限 100 万）。超限返回 `ErrListTooLarge` 而**不是**静默截断——如果需要更多，改用 `ListEach`/`ListEachPage`。

### 并发批量操作

```go
// 并发删除；并发数 <=0 用配置默认值（8），>64 会被钳制
err := b.DeleteMany(ctx, keys, 8)
var multi *kodo.MultiError
if errors.As(err, &multi) {
    for _, e := range multi.Errors {
        var ke *kodo.KeyError
        if errors.As(e, &ke) {
            log.Printf("删除失败 key=%s err=%v", ke.Key, ke.Err)
        }
    }
}

files, err := b.DownloadMany(ctx, keys, 8)  // map[string][]byte（出错时仍保留成功的项）
exists, err := b.ExistsMany(ctx, keys, 16)  // map[string]bool
```

批量操作语义：`ctx` 取消即停止派发；每个 key 独立 `recover`，单 key panic 不影响其他 key；失败项全部聚合返回。

`DownloadMany` 会把全部结果同时留在内存里返回，因此整批共用一个总量预算（`maxDownloadManyBytes`，默认 256MiB）：预算用尽后后续 key 返回 `ErrDownloadTooLarge`，失败 key 已占用的额度会被归还。批量下载大文件请改用 `DownloadFile` 逐个落盘。

## 错误语义

所有错误可用 `errors.Is` / `errors.As` 判定，**不要匹配错误文案**。

| 哨兵错误 | 含义 |
| --- | --- |
| `ErrNotInitialized` | 未初始化即调用（也用于 nil `*Bucket` 接收者） |
| `ErrBucketNotFound` | `TryGet` 指定的桶不在配置中 |
| `ErrClosed` | 插件已 `Close`（再次 `Init` 可恢复） |
| `ErrDisabled` | 配置 `enabled: false` 显式禁用；`Ready`/`Check` 与全部 Bucket 方法统一返回该错误（`enabled: false` 时 `Get/Default` 仍返回桶对象，便于统一报错而非 nil 判空） |
| `ErrInvalidConfig` | 配置解析/校验失败 |
| `ErrInvalidKey` | key 为空、超长（>750 字节）或含控制字符/NUL |
| `ErrObjectNotFound` | 对象不存在（下载 404 / Stat 612 映射而来） |
| `ErrDownloadTooLarge` | 下载体积超过上限：内存路径为 `maxDownloadBytes`（默认 64MiB、硬上限 1GiB）或批量总量 `maxDownloadManyBytes`；流式路径为 `maxDownloadSize` 或 8GiB 硬上限 |
| `ErrListTooLarge` | `ListAll` 累积对象条数超过 `maxListObjects`（默认 10 万、硬上限 100 万）；改用 `ListEach`/`ListEachPage` 流式遍历 |
| `ErrNilCallback` | `ListEach`/`ListEachPage` 传入 nil 回调 |
| `ErrUploadTooLarge` | 未知长度流缓冲超过 `maxUploadMemory` |
| `ErrNilReader` / `ErrNilWriter` | 传入 nil reader / writer |

底层错误统一带上下文包装（`%w` 保留错误链）：

```
kodo Download bucket=private key=users/1.png: ...
kodo Delete bucket=private key=a.png: ...
```

```go
_, err := b.DownloadBytes(ctx, key)
if errors.Is(err, kodo.ErrObjectNotFound) { /* 不存在 */ }
if kodo.IsNotFound(err)                   { /* 统一判定：612 / ErrNoSuchFile / ErrObjectNotFound */ }
```

`kodo.IsNotFound(err)` 的判定顺序：`storage.ErrNoSuchFile`、`ErrObjectNotFound` → `*qiniu.QError{Code:"612"}` → `*client.ErrorInfo{Code:612}` → 文案兜底 `no such file` / `no such object`（**不再**匹配裸 `"612"`，避免 key 名或文件大小含 612 时被误判）。

## 区域对照

| 配置值 | 区域 |
| --- | --- |
| z0 / huadong | 华东 |
| cn-east-2 / huadong2 | 华东-浙江2 |
| z1 / huabei | 华北 |
| z2 / huanan | 华南 |
| na0 / beimei | 北美 |
| as0 / xinjiapo / singapore | 东南亚（新加坡） |

未知值会打 `warn` 日志并回落到华东。

## Bucket 方法一览

| 方法 | 说明 |
| --- | --- |
| `Check(ctx)` | 查询桶信息，探活 |
| `Put` / `Upload` | 上传（按大小自动选表单/分片，未知长度走流式分片） |
| `UploadBytes` | 上传字节切片 |
| `UploadFile` | 从本地文件上传（推荐大文件） |
| `UploadMultipart` | 强制分片/断点续传上传，可指定分片大小，返回文件 Hash |
| `Download` | 下载到 `io.WriterAt`（流式，不占额外内存，支持重试） |
| `DownloadBytes` | 下载完整字节切片（受 `maxDownloadBytes` 限制，默认 64MiB） |
| `DownloadBytesLimit` | 同上，可对单次调用指定内存上限（硬上限 1GiB） |
| `DownloadFile` | 流式下载到本地文件（大文件推荐，不受内存上限约束）；失败时文件被截断为 0，避免半截文件被误认为成功 |
| `Delete` | 删除，不存在不报错 |
| `Exists` | 判断是否存在 |
| `Stat` | 对象元信息 |
| `List` | 列举对象（单页，最多 1000 条） |
| `ListAll` | 列举对象（自动翻页取全量，受 `maxListObjects` 约束） |
| `ListAllLimit` | 同上，但本次调用可单独指定条数上限 |
| `ListEach` / `ListEachPage` | 流式遍历（逐条 / 按页回调），不累积结果，可用于百万级桶 |
| `Presign` | 下载预签名（私有=带凭证 URL，公有=公开 URL，key 会 escape） |
| `PresignUpload` | 上传预签名（upToken，`scope` 锁定到指定 key，空 key 报错） |
| `DeleteMany` / `DownloadMany` / `ExistsMany` | 并发批量操作（错误聚合） |
| `Name` / `Domain` / `IsPrivate` | 桶属性（nil 接收者返回零值） |

## 健壮性设计

- **Init 幂等且原子**：重复 `Init` 整体重建（不会重复追加桶），失败保留旧状态，写锁保护读路径。
- **nil 安全**：`Get`/`Default` 返回 nil 时，所有方法返回 `ErrNotInitialized` 而不是 panic。
- **typed-nil 防线**：`var r io.Reader = (*bytes.Reader)(nil)` 这类「接口非 nil、内部指针为 nil」的输入，会在发请求前被识别并返回 `ErrNilReader` / `ErrNilWriter`（`Upload`/`UploadFile`/`Download`/`Put` 全路径生效）。
- **panic 兜底**：所有会触及 SDK / 网络 / 第三方回调的导出方法，入口都 `defer recover`（纯取值器如 `Name`/`Get`/`Ready` 只做 nil 判断，不引入第三方调用），SDK 内部 panic 不会打穿业务 HTTP handler。写法必须是 `defer recoverToError(&err, "X")`（`recover()` 只在 defer 注册的函数体内直接调用时生效，再包一层闭包会静默失效，仓库内有 AST 测试防止回退）。
- **上传 body 的协程级兜底**：上传请求的 body 由 `net/http` 的 **writeLoop 协程**（分片上传还有 SDK 的 worker 池）读取，业务 `io.Reader`/`io.ReaderAt` 的 panic 发生在那些协程里，方法入口的 `recover` 兜不住。因此交给 SDK 的读取入口统一包成 `safeReader`/`safeReaderAt`，把 panic 与非法读取计数转成普通 error（上传失败，而不是进程退出）。
- **日志出口 panic 安全**：`gologger` 通过 `runtime.Caller(3)` + `FuncForPC` 推断调用方，栈退化时会越界；日志是旁路能力，统一经 `logInfo/logWarn/logError` 包装吞掉 panic（尤其是 recover 路径中再 panic 会直接终止进程）。
- **错误判定 panic 安全**：`IsNotFound`/`isRetryable` 接受任意第三方错误（可能带 typed-nil 节点或会 panic 的 `Error`/`Unwrap`），判定失败退化为 false，不会反向打崩调用方。
- **连接池复用**：通过 `NewXxxEx(cfg, &client.Client{Client: hc})` 注入自建 `http.Client`；`Close()` 调用 `CloseIdleConnections()`。
- **幂等重试**：Stat/Delete/List/Check/Download 支持指数退避 + 抖动的有界重试（`net.Error` 超时/临时错误、`io.ErrUnexpectedEOF`、连接重置、HTTP 5xx/429）；4xx 与 ctx 取消不重试；上传因 body 已消费不自动重试。
- **内存相关配置全部有硬上限**：`maxUploadMemory` 256MiB（`bufferUnknownSize` 会把整条流读进内存）、`chunkSize` 8MiB（SDK 按分片大小 × 并发数分配缓冲）、`maxDownloadBytes` 1GiB、`maxDownloadManyBytes` 2GiB、`maxListObjects` 100 万。超限一律「钳制或明确报错」，不做静默放大。
- **极端配置钳制**：`retryMax` 上限 `MaxRetryCount`(10)（防止配置误写导致重试风暴）；秒级配置（`presignExpiry`/`dialTimeout`/`responseHeaderTimeout`/`idleConnTimeout`）上限 `MaxConfigSeconds`(math.MaxInt32)，避免 `× time.Second` 溢出成负数后静默变成「无超时」或「生成即过期」的 URL；`maxConcurrency` 上限 64。
- **内存上限分层**：流式下载上限 8GiB 只作用于 `Download`/`DownloadFile`（不占内存）；`DownloadBytes` 单对象默认 64MiB（硬上限 1GiB）、`DownloadMany` 批量总量默认 256MiB（硬上限 2GiB）、`ListAll` 默认 10 万条（硬上限 100 万）。超限一律返回明确错误（`ErrDownloadTooLarge`/`ErrListTooLarge`）并附逃生路径，绝不静默截断——Go 的 OOM 是 runtime throw，`recover` 兜不住，只能事前拦住。
- **列举可流式**：`ListEach`/`ListEachPage` 走同一套分页循环（`listPages`：ctx 每轮检查、marker 不推进即退出），内存只与单页 1000 条相关，百万级桶也不会堆内存。
- **无隐藏硬超时（数据面）**：上传/下载客户端不设置 `http.Client.Timeout`，大文件不会被 5 分钟截断；总时长由调用方 `ctx` 控制。
- **管理面有总超时**：`Stat`/`Delete`/`List`/`Check` 走独立的 `http.Client`（与数据面共用同一 `Transport`/连接池），带 `manageTimeout` 总超时。七牛 v1 管理 API 不接受 `ctx`，没有这层保护时「响应头到达后 body 卡住」会永久挂起（健康检查也会挂）。
- **重试前清理目标**：下载重试时会先清空上一次的部分写入——对 `Reset()` 缓冲（`writeAtBuf`）或 `Truncate(int64) error` 写入端（如 `*os.File`）截断到 0，避免对象在两次尝试间被覆盖变短后残留旧尾部数据。注意 `Truncate` 作用于文件本身：多个 goroutine 共用同一个 `*os.File` 并发下载不同对象时，其中一路重试会清空整份文件，此类用法请为每个下载使用独立文件。
- **下载完整性校验**：`Download` 记录 `Content-Length` 与实际写入字节数，不一致返回 `io.ErrUnexpectedEOF`（可重试），避免「连接中途断开 → `io.Copy` 以 EOF 正常结束 → 业务拿到半个文件却以为成功」。
- **内存下载不会 OOM（本插件唯一「事前拦截」而非 recover 的防线）**：Go 的 OOM 是 runtime throw，`recover` 兜不住，只能不让它发生——
  1. 内存路径与流式路径上限分离：`DownloadBytes` 默认 64MiB（`maxDownloadBytes`），`Download`/`DownloadFile` 才是 8GiB；
  2. 配置无法突破硬上限：单对象 1GiB、批量总和 2GiB；
  3. **读取 body 前**先按 `Content-Length` 预检并与上限比较，超限直接失败，一个字节都不读、不分配；
  4. 已知长度时按精确长度一次性预分配（`PresizeDownload`），避免边读边扩容造成「旧缓冲 + 新缓冲」的 2~3 倍峰值；未知长度才按 1.5 倍增长（不是 2 倍）；
  5. `DownloadMany` 共享总量预算，扩容前先申请额度，占不到就失败，而不是先分配再报错；
  6. 逃生路径：`DownloadFile`（流式落盘，不受内存上限约束）与 `DownloadBytesLimit`（单次显式放宽）。
- **日志安全**：日志与错误信息只包含 op / 桶名 / key，不输出 SecretKey、upToken、预签名 URL 的 token 参数；domain 校验失败的报错也不回显原始 domain（可能含 userinfo）。
- **不可信长度不上传**：已知长度会走 `ResumeUploader.Put(fsize)`，SDK 的分片循环是 `for off < totalSize`。若 `io.Seeker` 上报的长度是谎言（自定义 Seeker 返回 `MaxInt64` 等），循环会在「读到 EOF 但 off 远未达标」之间空转数十亿次——CPU 打满的假死，比 panic 更难发现。因此只有落在 `[0, 5TiB]` 的长度才被信任，超出一律降级为 `PutWithoutSize` 流式上传（结果仍正确，只损失 ReaderAt 的并发度）。
- **Seek 复位失败即失败**：测量长度需要 `Seek(0, SeekEnd)` 再复位。若复位失败，reader 停在流末尾，继续上传会写出一个「成功但内容为空」的对象（静默数据损坏），因此这种情形直接返回 `errSeekPositionLost`，而不是降级。
- **被 recover 的 panic 不留下持锁状态**：`Init`/`Close`/`enabled:false` 的资源替换统一走 `setResources`，写锁用 `defer` 释放；批量下载/存在的 map 写入同样用 `defer` 解锁。否则一次被成功 recover 的 panic 会让 mutex 永久处于锁定状态，后续所有 `Default`/`Get`/`Ready` 全部死锁——比 panic 本身更难排查。
- **错误响应不排空**：下载用的 `http.Client` 刻意不设总超时（否则大文件必被截断）。因此只有「body 已读到 EOF」才做排空以复用连接；非 2xx/超限/读失败一律直接 `Close`（net/http 的 earlyClose 不会等待 body 读完）。否则服务端「已回头部但 body 不再发数据」时，这次排空会永久阻塞，把一次应立即返回的错误变成卡死的 goroutine。注意判定口径是「仅 2xx 为成功」：3xx 与 4xx/5xx 一样返回 `statusError`（旧实现只拦 `>=400`，会把无 Location 的重定向页当作对象内容写出）。
- **批量 map 预分配有上限**：`make(map, len(keys))` 的提示被钳制到 4096，避免不可控的 key 数量导致一次性预分配巨量桶数组（OOM 不可 recover）。
- **关于「断线重连 / 重新订阅」**：七牛是 HTTP 对象存储，不存在长连接与订阅机制，因此本插件中与之对等的能力是：连接池复用 + 幂等操作有界重试（指数退避 + 抖动）+ `Init` 可重入（配置热重载时整体重建客户端）。

## 已知限制

- 七牛 v1 管理类 API（`BucketManager.Stat`/`Delete`/`GetBucketInfo`）不接受 `context`，方法签名保留 `ctx` 以维持接口一致，但这些调用只在派发前检查 `ctx`，无法打断已发出的请求。
- `List` 为单页语义（七牛单页上限 1000）；需要全量请用 `ListAll`（结果累积到内存，默认上限 10 万条），超大桶请用 `ListEach`/`ListEachPage` 流式遍历。
- `DownloadBytes`/`DownloadMany` 会把内容整体放入内存。默认上限已收紧到单对象 64MiB / 批量 256MiB（硬上限 1GiB / 2GiB），超限返回 `ErrDownloadTooLarge`；超大对象请用 `Download(ctx, key, writerAt)` 或 `DownloadFile`。
- `DownloadFile` 不做原子替换：先 `O_TRUNC` 打开再写，失败时会把文件截断为 0（不留半截内容），但「中途失败 → 文件消失」仍不是原子发布；需要「要么完整可见、要么不存在」请自行写临时文件 + rename。
- `DownloadBytes` 在服务端谎报超大 `Content-Length` 时，会按配置上限预分配（不多于人为允许的额度），这是「精确预分配」的固有代价。
- 未知长度的流式上传走 `PutWithoutSize`，SDK 内部分片行为不受 `chunkSize` 完全控制；需要精确分片请用 `UploadFile`/`UploadMultipart`。
- 传入自定义 `io.Seeker`（而非 `*os.File`）时，长度只在 `[0, 5TiB]` 内被信任：超出或无法测量时降级为流式上传（正确但少一点并发）；若测量过程中「已 seek 到末尾却无法复位」，会直接以 `errSeekPositionLost` 失败（避免写出空对象）。
- 当前环境 `proxy.golang.org` 不可用，构建请设置 `GOPROXY=https://goproxy.cn,direct`。
