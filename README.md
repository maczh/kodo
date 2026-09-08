# mgin kodo 插件

七牛云 Kodo 对象存储插件（qiniu go-sdk v7），提供上传/下载/删除/列举/预签名/分片上传等操作，支持多桶、公有/私有空间、自动区域选择。

与 `mgin/s3` 插件保持一致的 API 形态，便于在不同对象存储之间切换；针对七牛的特性做了以下适配：

- 每个桶必须绑定一个**访问域名**（`domain`）
- 桶区分**公有空间**和**私有空间**（`private`），私有空间下载需要下载凭证
- 上传使用七牛的**上传凭证**（upToken）机制
- 区域用 `zone`（z0/z1/z2/na0/as0 等）而非 region
- 大文件自动选用**断点续传上传**（ResumeUploader）

## 安装

```
go get github.com/maczh/kodo
```

### 挂载

```go
app.MGin.UsePlugin("kodo", kodo.NewKodo())
```

## 配置

在 `go.config.used` 中加入 `kodo`，配置范例：

```yaml
go:
  kodo:
    accessKey: your-access-key       #七牛账号 AccessKey
    secretKey: your-secret-key       #七牛账号 SecretKey
    zone: z0                         #存储区域，可选 z0/z1/z2/na0/as0 或 huadong/huabei/huanan/beimei/xinjiapo，默认 huadong
    useHTTPS: true                   #是否 HTTPS，默认 true
    useCdnDomains: false             #上传是否使用 CDN 加速，默认 false
    presignExpiry: 3600              #预签名（下载凭证/上传凭证）有效期，秒，默认 3600
    # resumeThreshold: 0             #超过该字节数改用分片上传，<=0 走默认 100MB
    singleBucket: mybucket          #单桶模式桶名
    domain: https://cdn.example.com  #单桶模式访问域名
    private: false                   #单桶模式是否私有空间，默认 false
    # buckets:                        #多桶模式
    #   - name: public
    #     domain: https://pub.example.com
    #     public: true
    #   - name: private
    #     domain: https://priv.example.com
    #     private: true
    #     defaultContentType: application/octet-stream
```

> **域名（domain）必填**：七牛的每个桶都必须绑定访问域名才能下载/预签名。单桶模式写在顶层 `domain`，多桶模式写在每个桶的 `domain`。未配置 domain 的桶会被跳过并打印错误日志。

## 使用

```go
k := kodo.GetKodo()
if k != nil {
    b := k.Default()                     //默认桶
    b := k.Get("private")                //指定桶
    _ = b.Upload(ctx, "users/1.png", data, "image/png")

    data, err := b.DownloadBytes(ctx, "users/1.png")
    url, err := b.Presign(ctx, "users/1.png")           //私有空间返回带下载凭证 URL，公有空间返回公开 URL
    token, err := b.PresignUpload(ctx, "users/1.png")   //返回上传凭证，客户端可凭此直传
    etag, err := b.UploadMultipart(ctx, "big.zip", "application/zip", reader, 0) //分片/断点续传
}
```

## 区域对照

| 配置值         | 区域            |
| -------------- | --------------- |
| z0 / huadong   | 华东            |
| cn-east-2 / huadong2 | 华东-浙江2  |
| z1 / huabei    | 华北            |
| z2 / huanan    | 华南            |
| na0 / beimei   | 北美            |
| as0 / xinjiapo | 东南亚（新加坡） |

## Bucket 方法一览

| 方法            | 说明                                                         |
| --------------- | ------------------------------------------------------------ |
| `Check(ctx)`    | 查询桶信息，探活                                             |
| `Put/Upload`    | 上传（按大小自动选表单/分片）                                |
| `UploadBytes`   | 上传字节切片                                                 |
| `Download`      | 下载到 `io.WriterAt`（流式，不占额外内存）                   |
| `DownloadBytes` | 下载完整字节切片                                             |
| `Delete`        | 删除，不存在不报错                                           |
| `Exists`        | 判断是否存在                                                 |
| `List`          | 列举对象（前缀过滤）                                         |
| `Presign`       | 下载预签名（私有=带凭证 URL，公有=公开 URL）                |
| `PresignUpload` | 上传预签名（返回上传凭证 upToken，客户端直传）              |
| `UploadMultipart` | 分片/断点续传上传，返回文件 Hash                           |
