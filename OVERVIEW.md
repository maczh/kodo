# 七牛 Kodo MGIN 插件 — 开发概述

## 目标
参考 https://github.com/maczh/s3（MGIN S3 对象存储插件），开发七牛云 Kodo 的 MGIN 插件，保持 API 形态一致，便于在 S3 与七牛之间切换。

## 交付物
仓库目录：`kodo/`

| 文件 | 说明 |
| --- | --- |
| `go.mod` / `go.sum` | 模块 `github.com/maczh/kodo`，依赖 `qiniu/go-sdk/v7 v7.27.0` |
| `config.go` | 配置结构（accessKey/secretKey/zone/useHTTPS/useCdnDomains/presignExpiry/resumeThreshold/singleBucket/domain/private/buckets） |
| `client.go` | `kodoClient` 单例 + `Bucket`，实现 `mgin.MginPlugin` 接口（Init/Check/Close/Default/Get）及全部存储操作 |
| `client_test.go` | 10 个纯逻辑测试，全部 PASS |
| `README.md` | 安装、配置、使用、区域对照表、方法一览 |

## API 形态（与 s3 插件对齐）
```
kodo.NewKodo() / kodo.GetKodo()
.Default() / .Get(name)
Bucket: Check / Put / Upload / UploadBytes
        Download / DownloadBytes / Delete / Exists / List
        Presign / PresignUpload / UploadMultipart
```

## 关键决策与七牛适配
1. **domain 必填**：七牛每个桶必须绑定访问域名才能下载/预签名；未配置 domain 的桶会被跳过并告警
2. **公有/私有空间**：`private` 控制下载是否需要下载凭证；`Presign` 私有返回 `MakePrivateURL` 带凭证 URL，公有返回公开 URL
3. **上传凭证**：`PresignUpload` 返回 upToken（scope 限定 `bucket:key`），客户端可直传
4. **自动选型**：按文件大小自动选择 `FormUploader`（小文件）或 `ResumeUploader`（断点续传，阈值默认 100MB，可配 `resumeThreshold`）
5. **流式下载**：用 `writeAtWriter` 把 `io.WriterAt` 适配为 `io.Writer`，配合 HTTP GET 实现流式下载、不占额外内存

## 验证结果
- `go vet ./...` 通过
- `go build ./...` 通过
- `go test ./...`：10/10 PASS

## 已知限制
- 七牛 v1 API 的 `BucketManager.Delete` / `Stat` / `ListFiles` 不接受 context，方法签名保留 ctx 以维持接口一致但管理类调用无法响应 ctx 取消
- `UploadMultipart` 的 `partSize` 参数七牛 v1 SDK 内部分片大小固定，参数仅为对齐接口（已在文档说明）
- 当前环境 `proxy.golang.org` 不可用，需设置 `GOPROXY=https://goproxy.cn,direct`

## 后续可选项
- 接入真实七牛空间做端到端集成测试（当前仅纯逻辑单测，未连真实服务）
- 如需流式大文件上传（避免未知长度 Reader 读入内存），可增加 `UploadFile(path)` 基于 `ResumeUploader.PutFile`
