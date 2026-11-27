# 七牛 Kodo MGIN 插件 — 开发概述

## 目标

参考 https://github.com/maczh/s3（MGIN S3 对象存储插件），开发七牛云 Kodo 的 MGIN 插件，保持 API 形态一致，便于在 S3 与七牛之间切换；同时保持 v1(jh) 插件接口不变，并额外提供 v2 适配层。

## 交付物

仓库目录：`kodo/`

| 文件 | 说明 |
| --- | --- |
| `go.mod` / `go.sum` | 模块 `github.com/maczh/kodo`，依赖 `qiniu/go-sdk/v7 v7.27.0` |
| `config.go` | 配置结构、`DefaultConfig()`、`ParseConfig()`、`Normalize()` 与各默认值访问器 |
| `client.go` | `kodoClient` 生命周期（Init/Close/Check/Default/Get/TryGet/Ready/Names）+ `Bucket` 结构与就绪校验 |
| `bucket.go` | 全部存储操作（上传/下载/删除/列举/预签名/分片/Stat）；`DownloadFile` 流式落盘、`DownloadBytesLimit` 单次内存上限 |
| `parallel.go` | 并发批量操作 `DeleteMany`/`DownloadMany`/`ExistsMany` + 工作池；`DownloadMany` 共享 `byteBudget` 总量预算 |
| `transport.go` | 自建 `http.Client`/连接池（数据面无总超时、管理面带 `manageTimeout`，共用同一 `Transport`）+ 幂等重试策略 + 可重试错误判定 |
| `io_util.go` | 上传源规整（seek 取长度/流式）、`byteBudget` 内存预算、`writeAtBuf`（精确预分配 + 有界扩容）、`io.WriterAt` 适配、`safeReader`/`safeReaderAt`（上传 body 跨协程 panic 兜底）、`drainAndClose`（响应清理 panic 兜底）、`recoverToError`/`panicText` |
| `logging.go` | panic 安全日志出口（`logInfo`/`logWarn`/`logError`） |
| `errors.go` | 哨兵错误、`IsNotFound`、`KeyError`/`MultiError`、`ValidateKey` |
| `util.go` | `ObjectInfo`、zone 映射（返回值拷贝）、Content-Type 推断、`isNilValue`/`errorText` |
| `*_test.go`（13 个） | `package kodo` 内部测试：144 个 `Test*` 函数 / 239 个用例，`httptest` 假域名驱动，无真实七牛端到端 |
| `mginv2/` | 独立模块 `github.com/maczh/kodo/mginv2`，实现 v2 `plugin.Plugin` |
| `compat/mginv1/` | 独立模块，v1(jh) 挂接编译期断言 |
| `compat/mginv2/` | 独立模块，v2 挂接编译期断言 |
| `README.md` | 安装、配置（含新增项与默认值）、使用、错误语义、区域对照、已知限制 |

## API 形态（与 s3 插件对齐）

```
kodo.NewKodo() / kodo.GetKodo()
.Ready() / .Default() / .Get(name) / .TryGet(name) / .TryDefault() / .Names() / .IsClosed()
Bucket: Check / Put / Upload / UploadBytes / UploadFile
        Download / DownloadBytes / DownloadBytesLimit / DownloadFile
        Delete / Exists / Stat / List / ListAll / ListAllLimit / ListEach / ListEachPage
        Presign / PresignUpload / UploadMultipart
        DeleteMany / DownloadMany / ExistsMany
```

公开 API 只做新增，未改动任何既有签名；v1 插件接口 `Init([]byte)`/`Close()`/`Check() error` 保持原样。

## 关键决策与七牛适配

1. **domain 必填**：七牛每个桶必须绑定访问域名才能下载/预签名；未配置 domain 的桶被跳过并告警，全部跳过时 `Init` 失败但保留旧状态。
2. **公有/私有空间**：`private` 控制下载是否需要下载凭证；`Presign` 私有返回 `MakePrivateURLv2`，公有返回 `MakePublicURLv2`（均正确 escape key）。
3. **上传凭证**：`PresignUpload`/内部上传的 `scope` 恒为 `bucket:key`，空 key 直接报 `ErrInvalidKey`，杜绝桶级越权上传。
4. **自动选型**：已知长度按 `resumeThreshold` 选 `FormUploader`/`ResumeUploader`；**未知长度一律 `PutWithoutSize` 流式分片**（不读内存），可选 `bufferUnknownSize` 走有界内存缓冲。
5. **流式下载**：`writeAtWriter` 把 `io.WriterAt` 适配为 `io.Writer`；不设 `http.Client.Timeout`，改用 `ResponseHeaderTimeout` + 连接池，避免大文件被总超时截断。
6. **内存下载 vs 流式下载分离**：`DownloadBytes`/`DownloadMany` 走内存，默认上限 64MiB / 256MiB（硬上限 1GiB / 2GiB），读取前按 `Content-Length` 预检 + 精确预分配 + 批量总量预算；`Download`/`DownloadFile` 走流式，上限才是 8GiB。理由：Go 的 OOM 是 runtime throw，`recover` 兜不住，只能事前拦住。
7. **连接池可控、超时分层**：用 `NewFormUploaderEx/NewResumeUploaderEx/NewBucketManagerEx` 注入自建 `http.Client`；数据面（上传/下载）不设总超时（大文件由 `ctx` 控制），管理面（v1 管理 API 无 `ctx`）单独带 `manageTimeout`，两者共用同一 `Transport`。默认关闭 HTTP/2 以规避 SDK `internal/uplog` 的数据竞争（可配置开启）。
8. **错误判定严格化**：`IsNotFound` 改为类型化判定（`storage.ErrNoSuchFile` / `QError 612` / `ErrorInfo 612`），删除裸 `"612"` 文本匹配。
9. **并发加固**：批量操作有并发上限、ctx 取消即停止派发、逐 key `recover`、失败项聚合为 `*MultiError`（内含 `*KeyError`）。
10. **消除 nil panic**：`Get/Default` 仍返回 nil（保持既有语义），但所有方法对 nil 接收者、未初始化、`Close` 后状态返回明确错误。

## 缺陷修复对照

| # | 缺陷 | 修复 |
| --- | --- | --- |
| 1 | Init 与读路径 data race、重复 append、失败留半状态 | `lifecycleMu` 串行化 Init/Close，构建到局部变量后写锁内原子替换，失败保留旧状态 |
| 2 | `Get(...).Upload()` nil 指针 panic | 所有导出方法 nil/未初始化/已关闭保护，新增 `ErrNotInitialized`/`ErrBucketNotFound`/`ErrClosed` |
| 3 | `useHTTPS: false` 被 normalize 改回 true | 解析目标为 `DefaultConfig()`（UseHTTPS=true），`normalize()` 不再改写；导出 `DefaultConfig()` |
| 4 | `isNotFound` 用裸 "612" 文本匹配导致误判 | 类型化判定 + 文案兜底收窄；导出 `IsNotFound` |
| 5 | 下载 5 分钟硬超时导致大文件必失败 | 去掉总 Timeout，改 `ResponseHeaderTimeout` + 连接池参数；`Close()` 回收空闲连接 |
| 6 | URL 未转义（key 含 `?` 可注入篡改签名） | 改用 `MakePublicURLv2`/`MakePrivateURLv2` |
| 7 | 未知长度上传 `io.ReadAll` OOM、阈值失效 | seek 取长度 / `PutWithoutSize` 流式；`MaxUploadMemory` 限量缓冲；新增 `UploadFile` |
| 8 | 下载缓冲无上限、`cap=end*2` 可能巨大 | 8GiB 硬上限 + `MaxDownloadSize`，Content-Length 预检 + 边读边累计超限即中止 |
| 9 | List 静默截断 | `List` 明确单页语义并封顶 1000；新增 `ListAll` marker 分页（防死循环） |
| 10 | resolveZone 返回全局变量地址 | 返回值拷贝；未知 zone 告警后回落华东 |
| 11 | `PresignUpload("")` 生成桶级凭证（越权） | 空 key 拒绝 + `ValidateKey`（非空/无控制字符/≤750 字节） |
| 12 | SDK panic 打穿业务 handler | 所有导出方法入口 `defer recover`，panic 转 error 并打堆栈 |
| 13 | Close 后状态未定义 | 关闭状态机：`ErrClosed`，再次 Init 恢复；旧 `*Bucket` 引用保持失效 |
| 14 | `Config.Enabled` 是死字段（配 `enabled: false` 仍服务） | 改为 `*bool`（nil=启用）；`Init` 检测到显式禁用即跳过初始化并置 disabled 态，`Ready`/`Check`/全部业务方法统一返回 `ErrDisabled`；禁用态不清空桶定义，`Get` 仍可返回桶对象 |
| 15 | 下载截断静默成功 | `downloadOnce` 校验 `Content-Length` 与实际写入字节数，不一致返回 `io.ErrUnexpectedEOF`（且可重试） |
| 16 | domain 协议无白名单 | 新增 `resolveDomain`：仅允许 http/https，拒绝 userinfo/query/fragment，非法桶跳过并告警；`useHTTPS=true` + 显式 `http://` 仅 warn、不改写配置 |
| 17 | `wrapErr` 空壳丢上下文 | 改为 `wrapOp(op, bucket, key, err)`，统一 `%w` 包装（保持 `errors.Is/As` 与 `IsNotFound`、`isRetryable` 判定链） |
| 18 | `defer func(){ recoverToError(&err, op) }()` 间接调用 `recover()` 失效（Go 规范：只有 defer 顶层函数直接调用 recover 才有效），全部导出方法的 panic 兜底形同虚设（实际 19 个调用点） | 全部改为 `defer recoverToError(&err, op)`（19 个实际调用点，均在带命名 error 返回值的函数内）；闭包内**直接**调用 `recover()` 的 `callSafely` 与 `mginv2` 保持不变 |
| 19 | 下载重试时只有 `Reset()` 写入端会清理残留 | `downloadOnce` 返回实际写入字节数；重试分支在 `lastWritten > 0` 时对 `Reset()` 或 `Truncate(int64)` 写入端清空，避免对象变短后残留旧尾部（并发共享 `*os.File` 的注意事项已写入 README） |
| 20 | typed-nil 输入 panic：`var r io.Reader = (*bytes.Reader)(nil)` 时 `r != nil`，`uploadSource` 的 `v.Size()/Len()/Bytes()` 直接解引用 nil 指针 | 新增 `isNilValue`（reflect，只对可 nil 的 kind 生效），`Upload`/`Put`/`Download`/`uploadSource`/`readAllBounded`/内部适配器全部前置拦截，返回 `ErrNilReader`/`ErrNilWriter` |
| 21 | 上传 body 的 panic 无法被 `recover` 兜住：body 由 net/http 的 writeLoop 协程读取，分片上传还经过 SDK worker 池，panic 发生在其它协程 → 进程直接退出 | 新增 `safeReader`/`safeReaderAt` 包装交给 SDK 的所有读取入口，把 Read/ReadAt 的 panic 与非法读取计数（n<0 或 n>len(p)）转成普通 error |
| 22 | 第三方日志库 panic 会打穿 recover 路径：`gologger` 用 `runtime.Caller(3)`+`FuncForPC`+`parts[pl-2][0]`，栈退化/函数名无包路径时越界；recover 中再 panic 会终止进程 | 新增 `logging.go`：`logInfo/logWarn/logError` 统一经 `safeLog` 吞掉日志实现的 panic；`recoverToError`/`callSafely`/`Init`/`Close` 全部改走安全日志 |
| 23 | panic 值本身不可格式化（`String()`/`Error()` 会 panic）时，`fmt.Sprintf("%v", r)` 在 recover 路径二次 panic | 新增 `panicText`，格式化再兜一层 recover，退化为 `<unprintable panic value: %T>` |
| 24 | `IsNotFound`/`isRetryable` 对 typed-nil 或 `Error()`/`Unwrap()` 会 panic 的第三方错误反向崩溃；`wrapOp` 会把 typed-nil 包装成「非 nil 空错误」 | 两个判定函数加 typed-nil 拦截 + 顶层 recover（失败退化为 false）；`wrapOp` 对 typed-nil 直接返回 nil；`statusError.Error()`/`retryable()` 加 nil 接收者保护 |
| 25 | `MultiError.Errors` 是对外暴露切片，混入 nil 时 `Error()` 直接 panic | `Error()` 跳过 nil/typed-nil 项并在摘要中标注数量；`errors.Is/As` 仍可穿透 |
| 26 | 整数溢出导致的静默错误：`readAllBounded` 的 `limit+1`（limit=MaxInt64）溢出成负数 → 立即 EOF 返回空内容；秒级配置 `× time.Second` 溢出成负数 → 变成「无超时」或「URL 生成即过期」；`retryMax` 巨大 → 长时间重试风暴 | `readAllBounded` 先把 limit 压到 `MaxInt64-1`；`normalize()` 与各 getter 用 `MaxConfigSeconds`(MaxInt32) 钳制秒级字段；`retryPolicy`/`retryDo` 用 `MaxRetryCount`(10) 钳制重试次数；`retryBaseDelay` 钳制到 `MaxRetryDelay` |
| 27 | 其它边界：`writeAtBuf`/`writeAtWriter`/`limitedReader` 的 nil 接收者；`WriteAt` 返回负数计数导致偏移倒退；`http.Client.Do` 返回空 Body；`UploadFile` 先开文件后校验桶；导出的 `ParseConfig` 无 panic 兜底 | 全部加 nil 保护与计数钳制；`downloadOnce` 判空 `resp.Body`；`UploadFile` 先 `readyUploader`+`ValidateKey` 再 `os.Open`；`ParseConfig` 加 recover 并置空返回值 |
| 28 | **`DownloadBytes` 超大对象 OOM（不可 recover）**：内存路径沿用流式的 8GiB 上限，且 `writeAtBuf` 按 2 倍扩容，一个 5GiB 对象即可让「旧缓冲 + 新缓冲」峰值超过 7GiB；Go 的 OOM 是 runtime throw，`recoverToError` 兜不住 | 内存路径与流式路径上限分离：`maxDownloadBytes` 默认 64MiB、硬上限 1GiB；`writeAtBuf` 只受 `HardMaxDownloadBytes` 约束；增长改为 1.5 倍、最小步进 64KiB |
| 29 | 已知 `Content-Length` 仍边读边扩容，产生 2~3 倍瞬时峰值；谎报超大长度时会先读再报错 | `downloadOnce` 在**读取 body 前**做 Content-Length 预检，并调用 `PresizeDownload` 按精确长度一次性分配；超限错误附带 `Download`/`DownloadFile` 逃生路径 |
| 30 | `DownloadMany` 把全部结果留在内存，单对象上限 × key 数量仍可 OOM；失败的 key 还会继续占用额度 | 新增 `byteBudget` 批量总量预算（`maxDownloadManyBytes` 默认 256MiB、硬上限 2GiB），扩容前先申请容量；失败 key 按实际容量精确归还 |
| 31 | 缺少「不占内存」的下载落盘入口，用户只能自己拼 `os.File` + `Download` | 新增 `DownloadFile(ctx, key, localPath)`（流式落盘，受 `Truncate` 重试清理保护）与 `DownloadBytesLimit`（单次显式放宽，仍受 1GiB 硬上限约束） |
| 32 | `ListAll` 无任何条数上限，全量结果驻留内存且 `append` 扩容期间翻倍，超大桶可 OOM（stream throw 不可 recover）；也没有流式遍历入口 | 新增 `maxListObjects`（默认 10 万、硬上限 100 万），超限返回 `ErrListTooLarge` 而非静默截断；分页循环收敛为 `listPages` 单一实现（ctx 检查、marker 不推进即退出），新增 `ListAllLimit`、`ListEach`、`ListEachPage` 流式接口（内存只与单页 1000 条相关） |
| 33 | `maxUploadMemory` 没有硬上限：`bufferUnknownSize: true` + 误写 1TB 会把整条流读进内存，OOM 不可 recover | 新增 `HardMaxUploadMemory`（256MiB），`normalize()` 强制钳制；超限的流应走默认的 `PutWithoutSize` 流式分片 |
| 34 | `chunkSize` 没有硬上限：SDK 会为**每个并发分片**分配 `make([]byte, 0, ChunkSize)`，配置 1GB 时直接按「并发数 × 1GB」爆内存 | 新增 `HardMaxChunkSize`（8MiB），`resolveChunkSize` 对显式参数与配置值统一钳制并打 warn |
| 35 | 管理类 API（Stat/Delete/List/Check）共用「无总超时」的客户端：七牛 v1 SDK 不接受 ctx，遇到「响应头已到、body 卡住」会永久挂起（健康检查也挂） | 拆出 `newHTTPClients`：数据面（上传/下载）保持无总超时，管理面单独用带 `Timeout` 的 `http.Client`（`manageTimeout` 默认 60s）。两者**共用同一个 Transport**，不会出现两个连接池 |
| 36 | `DownloadFile` 只在成功/普通失败路径关闭文件，`download` panic 时文件描述符泄漏；失败还会留下「看起来完整」的半截文件 | 改为 `defer` 关闭（幂等 `closed` 标记）；任何失败路径先 `Truncate(0)` 再返回错误 |
| 37 | `DownloadBytes` 只在返回 error 时归还批量预算，panic 路径（`recoverToError` 兜住）会让预算永久被占用，后续 key 被误判「总量不足」 | 改为 `defer` 统一处理：panic 时先 `release(buf.Cap())` 再 re-panic；error 时同样归还 |
| 38 | `downloadOnce` 的「排空 4KB + 关闭 body」写在 defer 里：数据已完整写入后，自定义 Transport 的 Read/Close panic 会把成功下载变成失败，且 Close 可能被跳过 | 新增 `drainAndClose`/`safeCloseBody`：读残留 panic 被吞、Close 必定执行且其 panic 也被吞 |
| 39 | `KeyError.Error()` 直接 `%v` 格式化 `Err`：调用方自行构造的 KeyError 若带 nil/typed-nil/会 panic 的 Error()，日志与聚合路径反向崩溃 | 改用 `errorText`（typed-nil + panic 双兜底），空值标注 `<nil>` |
| 40 | `ValidateKey` 只拒绝 ASCII 控制字符（`<0x20`、`0x7F`），放过 C1（U+0080-U+009F），它们同样会污染 URL/HTTP 头 | 改用 `unicode.IsControl`，覆盖 C0/C1/DEL；中文、emoji、空格、`?` 等正常 key 不受影响 |
| 41 | `limitedReader.Read` 的 `remaining+1` 在 `limit=MaxInt64` 时溢出为负数，随后 `p[:负数]` panic | 切片前加 `remaining < math.MaxInt64` 守卫（生产路径 limit ≤ 8GiB，属通用读取器的防御） |
| 42 | mginv2 适配器在 recover 里用 `%v` 格式化 panic 值，panic 值自带会 panic 的 `String()/Error()` 时二次 panic（无更外层 recover，进程终止） | 新增 `safePanicText`，与根包 `panicText` 同语义 |

## 验证结果

- `GOPROXY=https://goproxy.cn,direct go build ./...`：通过
- `GOPROXY=https://goproxy.cn,direct go vet ./...`：通过
- `gofmt -l .`：无输出
- `GOPROXY=https://goproxy.cn,direct go test ./... -count=1`：通过

  ```
  ok  	github.com/maczh/kodo	3.0s
  ```

- `GOPROXY=https://goproxy.cn,direct go test ./... -race -count=1`：通过（无 data race 告警）

  ```
  ok  	github.com/maczh/kodo	2.4s
  ```
- `mginv2/`：`go mod tidy` + `go build ./...` + `go vet ./...` 通过
- `compat/mginv1`：`go build ./...` 通过（`var _ mgin.MginPlugin = kodo.NewKodo()`）
- `compat/mginv2`：`go build ./...` 通过（`var _ plugin.Plugin = mginv2.New()`）
- 新增项自检：`enabled: false` → `ErrDisabled`（Ready/Check/业务方法/Presign）、重新启用恢复；`file://` / `gopher://` / userinfo 域名被拒且旧状态保留、显式 `http://` 原样保留、无 scheme 按 useHTTPS 补全；截断响应 → `io.ErrUnexpectedEOF`；`wrapOp` 包装后 `IsNotFound` 仍成立
- 并发/正确性自检：已**固化为仓库内测试**（早期用 /tmp 临时程序验证的用例已由 QA 全部落库），覆盖 nil 安全、Init 幂等/原子替换、Close/Re-Init、key 校验、`IsNotFound` 严格性、下载转义与体积上限、`DeleteMany` 聚合与 ctx 取消、并发 Init/Close/Get
- 测试规模与手段：14 个测试文件、159 个 `Test*` 函数、254 个用例（含子用例）全部 PASS；用 `httptest` 假域名 + `Hijack` 构造截断响应 + 5xx 重试计数 + panic writer/reader + 自定义 `RoundTripper`（固定状态码、可观测 body、不读即断言）等手段构造边界；**不含**真实七牛空间端到端（无 AK/SK），上传/分片/私有凭证仅在 SDK 契约层面校验
- 第二轮 panic 面专项（`panic_audit_test.go`，18 个用例组）：typed-nil reader/writer、`MultiError` 混入 nil、`statusError` nil 接收者、`Error()` 会 panic 的错误类型、日志实现 panic、`panicText` 不可格式化 panic 值、内部适配器 nil 接收者、非法读取/写入计数、`readAllBounded` 的 MaxInt64 溢出、极端配置钳制与超大 retry policy、`upload()` 真实接线 `safeReader`
- 内存下载专项（`memory_download_test.go`，14 个用例）：默认值与硬钳制、Content-Length 超限在分配前拒绝、精确预分配（`cap(data)==len(data)`）、未知长度扩容不突破上限、单次 `DownloadBytesLimit` 可升可降、`DownloadFile` 不受内存上限约束、`DownloadMany` 总量预算（4×600B / 1024B 只成功 1 个）、失败 key 归还预算（去掉 `release` 该用例即失败，已实测）、`byteBudget` 并发精确性（100 goroutine 抢 1000 字节恰好成功 10 次）、谎报 Content-Length 被拦截

- 第三轮审核专项（`audit_round3_test.go`，10 个用例）：`maxUploadMemory`/`chunkSize` 硬钳制（含 yaml 路径）、`manageTimeout` 默认/显式/极端值、管理客户端与数据客户端共用 `Transport` 且管理端有总超时、`DownloadFile` 失败截断为 0、`DownloadFile` 吞掉 Transport panic、`DownloadBytes` 在「已预分配后 panic」时归还预算（去掉 defer release 该用例即失败，已实测）、body 排空/Close panic 不影响成功下载（换回旧 defer 该用例即失败，已实测）、`KeyError` 对 nil/typed-nil/panic 错误的格式化、`ValidateKey` 拒绝 C1、`limitedReader` 的 MaxInt64 边界

- 第四轮审核专项（`audit_round4_test.go`，15 个用例）——针对「panic 被 recover 之后的二次伤害」：
  1. **不可信长度**：`io.Seeker` 上报 `MaxInt64`/`2^62`/`2^50`/`5TiB+1` 时不再被当作已知长度（否则 SDK 分片循环空转数十亿次 = CPU 假死），边界值 `5TiB` 仍可信；
  2. **位置丢失**：`Seek(0,SeekEnd)` 成功但复位失败时返回 `errSeekPositionLost`（旧实现会从末尾流式上传 → 静默写出空对象），且 `Upload` 端到端失败、**零网络请求**；SeekEnd 失败但复位成功时安全降级；
  3. **错误响应不排空**：500/404/403 的 body 一次都不读、必须 Close、且 2s 内返回（旧 `defer drainAndClose` 会读 4KB，服务端卡住 body 即永久阻塞）；200 正常路径仍排空以复用连接；
  4. **锁不因 panic 泄漏**：`setResources` 并发替换 200 轮 + 读路径不阻塞（5s 超时守护）；上传路径 panic 被兜住后 `Ready`/`Names` 仍可用；
  5. **批量预分配**：`mapHint` 对 `-1/0/1/4096/4097/2^30` 的钳制；`DownloadMany` 256 key 全成功；
  6. `3xx` 不再被当作成功下载（旧判定「>=400 才报错」会把重定向页当对象内容写出）；控制字符 key（NUL/CRLF/C1）在 uploader 就绪后仍被 `ErrInvalidKey` 拒绝；nil bucket 补齐 `DownloadBytesLimit`/`ListEach`/`ListEachPage`/`ListAllLimit`。

## 已知限制

- 七牛 v1 管理类 API（`Stat`/`Delete`/`GetBucketInfo`）不接受 `context`，重试与取消只能作用于派发前；挂死由 `manageTimeout`（默认 60s）兜底。
- `List` 单页上限 1000（服务端限制）；`ListAll` 结果累积在内存，受 `maxListObjects`（默认 10 万、硬上限 100 万）约束，超出返回 `ErrListTooLarge`；超大桶请用 `ListEach`/`ListEachPage`。
- `DownloadBytes`/`DownloadMany` 结果整体驻留内存：单对象默认 64MiB（硬上限 1GiB）、批量总和默认 256MiB（硬上限 2GiB），超限返回 `ErrDownloadTooLarge`；超大对象请用 `Download`（流式）或 `DownloadFile`（落盘）。
- `DownloadFile` 先 `O_TRUNC` 打开再写，失败时会 `Truncate(0)`（不留半截文件），但仍不是原子发布；需要「要么完整可见、要么不存在」请自行写临时文件后 rename。
- 未知长度流式上传的分片细节由 SDK `PutWithoutSize` 决定，`chunkSize` 不完全生效。
- 测试全部为仓库内离线测试（`httptest` 假域名、`Hijack` 截断、5xx 重试计数、panic writer），**未接真实七牛空间做端到端验证**（缺 AK/SK）。
- `Download` 不设 HTTP 总超时（避免大文件被 5 分钟硬超时截断）：若服务端既不下发数据也不断开、且调用方未给 `ctx` deadline，会长时间等待；生产调用请自行设置 `ctx` 超时。错误响应（4xx/5xx）的 body 不再被排空（直接 Close），因此这些响应不会复用连接。
- 自定义 `io.Seeker` 上报的长度只在 `[0, 5TiB]` 内被信任，超出则降级为流式上传；「已 seek 到末尾却复位失败」直接失败（`errSeekPositionLost`）。真实 `*os.File` 不受影响。
- 共享同一个 `*os.File` 并发下载不同对象是**不安全用法**：某一路触发重试时会 `Truncate(0)` 清空整份文件，并可能因后续远端偏移写入产生零洞；请为每个下载使用独立文件，或传入不实现 `Truncate`/`Reset` 的 WriterAt 包装。
- 当前环境 `proxy.golang.org` 不可用，构建请设置 `GOPROXY=https://goproxy.cn,direct`。
- `enabled: false` 但配置里既无账号也无桶时，`Get/Default` 返回 nil（此时 nil 桶方法返回 `ErrNotInitialized`）；`Ready/Check` 仍返回 `ErrDisabled`。

## 后续可选项

- 接入真实七牛空间做端到端集成测试（上传/下载/私有凭证/分片/批量）。
- 支持上传进度回调（`RputExtra.Notify`）与断点续传记录（`Recorder`）。
- 支持自定义 `UpHost` / 加速上传域名等更细粒度网络配置。
