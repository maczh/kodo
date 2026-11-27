package kodo

import (
	"path/filepath"
	"reflect"
	"strings"
	"time"

	"github.com/qiniu/go-sdk/v7/storage"
)

// isNilValue 判断接口值是否为 nil **或持有 typed-nil**（如 (*bytes.Reader)(nil)）。
//
// 为什么需要：`var r io.Reader = (*bytes.Reader)(nil)` 时 `r != nil`，
// 但调用 r 的方法会解引用 nil 指针直接 panic。插件对外接收 io.Reader/io.WriterAt，
// 业务侧传「声明但未初始化」的指针是常见写法，必须在边界拦住。
func isNilValue(v any) bool {
	if v == nil {
		return true
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Pointer, reflect.Map, reflect.Slice, reflect.Chan, reflect.Func,
		reflect.Interface, reflect.UnsafePointer:
		return rv.IsNil()
	default:
		return false
	}
}

// errorText 安全地取错误的文本表示。
//
// err != nil 不代表 err.Error() 安全：typed-nil（指针为 nil 但接口非 nil）或
// 第三方错误类型的 Error() 内部 panic 都会让判定函数（IsNotFound/isRetryable）
// 反过来把调用方打崩。这里既拦 typed-nil，也兜住 Error() 自身的 panic。
func errorText(err error) (text string) {
	if err == nil || isNilValue(err) {
		return ""
	}
	defer func() {
		if recover() != nil {
			text = ""
		}
	}()
	return err.Error()
}

// ObjectInfo 列举/Stat 返回的对象摘要
type ObjectInfo struct {
	Key          string
	Size         int64
	LastModified time.Time
	ETag         string
	ContentType  string
}

// toObjectInfos 把七牛 ListItem 转为 ObjectInfo
func toObjectInfos(page *storage.ListFilesRet) []ObjectInfo {
	if page == nil || len(page.Items) == 0 {
		return nil
	}
	list := make([]ObjectInfo, 0, len(page.Items))
	for _, item := range page.Items {
		info := ObjectInfo{
			Key:         item.Key,
			Size:        item.Fsize,
			ETag:        item.Hash,
			ContentType: item.MimeType,
		}
		if item.PutTime > 0 {
			info.LastModified = putTimeToTime(item.PutTime)
		}
		list = append(list, info)
	}
	return list
}

// putTimeToTime 七牛 PutTime 单位为 100 纳秒（自 1970-01-01 起），转换为 time.Time。
// 拆分秒与纳秒以避免直接相乘导致 int64 溢出。
func putTimeToTime(putTime int64) time.Time {
	sec := putTime / 1e7
	nsec := (putTime % 1e7) * 100
	return time.Unix(sec, nsec)
}

// resolveZone 将配置中的 zone 字符串映射为七牛 SDK 的 *storage.Zone。
//
// 注意：SDK 的 storage.ZoneHuadong 等是**包级全局变量**，直接返回 &storage.ZoneHuadong
// 会把共享可变全局暴露给 SDK 内部（任何写入都会污染其他使用者），因此这里返回值拷贝。
// 未知 zone 会打 warn 日志后回落到华东。
func resolveZone(zone string) *storage.Zone {
	normalized := strings.ToLower(strings.TrimSpace(zone))
	var src storage.Zone
	switch normalized {
	case "z0", "huadong", "":
		src = storage.ZoneHuadong
	case "cn-east-2", "huadong2", "z0-2":
		src = storage.ZoneHuadongZheJiang2
	case "z1", "huabei":
		src = storage.ZoneHuabei
	case "z2", "huanan":
		src = storage.ZoneHuanan
	case "na0", "beimei":
		src = storage.ZoneBeimei
	case "as0", "xinjiapo", "singapore":
		src = storage.ZoneXinjiapo
	default:
		logWarn("[kodo] 未知 zone \"" + zone + "\"，已回落到 huadong（华东）")
		src = storage.ZoneHuadong
	}
	zoneCopy := src
	return &zoneCopy
}

// guessContentType 按扩展名推断 Content-Type
func guessContentType(key string) string {
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
	case ".bmp":
		return "image/bmp"
	case ".svg":
		return "image/svg+xml"
	case ".ico":
		return "image/x-icon"
	case ".pdf":
		return "application/pdf"
	case ".txt", ".log":
		return "text/plain"
	case ".html", ".htm":
		return "text/html"
	case ".css":
		return "text/css"
	case ".js":
		return "application/javascript"
	case ".json":
		return "application/json"
	case ".xml":
		return "application/xml"
	case ".csv":
		return "text/csv"
	case ".zip":
		return "application/zip"
	case ".gz", ".tgz":
		return "application/gzip"
	case ".tar":
		return "application/x-tar"
	case ".mp4":
		return "video/mp4"
	case ".webm":
		return "video/webm"
	case ".mov":
		return "video/quicktime"
	case ".mp3":
		return "audio/mpeg"
	case ".wav":
		return "audio/wav"
	case ".ogg":
		return "audio/ogg"
	case ".doc":
		return "application/msword"
	case ".docx":
		return "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
	case ".xls":
		return "application/vnd.ms-excel"
	case ".xlsx":
		return "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
	case ".ppt":
		return "application/vnd.ms-powerpoint"
	case ".pptx":
		return "application/vnd.openxmlformats-officedocument.presentationml.presentation"
	default:
		return "application/octet-stream"
	}
}
