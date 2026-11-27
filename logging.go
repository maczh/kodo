package kodo

import "github.com/sadlil/gologger"

// logger 插件日志出口（与 mgin 其余插件一致，使用 gologger）。
var logger = gologger.GetLogger()

// 日志统一走下面的 panic 安全包装。
//
// 原因：gologger 在 logPrinter 里用 runtime.Caller(3) + runtime.FuncForPC 推断调用方，
// 再对 strings.Split(name, ".") 的结果做 parts[pl-2][0] 取值。若调用栈退化（pc 解析不到
// 函数名）或函数名不含包路径，这里会直接 nil 解引用 / 切片越界。
//
// 日志是旁路能力，不允许它把插件打崩；更关键的是 recover 路径里再 panic 会让
// 进程直接退出（Go 不支持 panic 中再次 panic），所以 recover 内的日志必须安全。
func logInfo(msg string)  { safeLog(logger.Info, msg) }
func logWarn(msg string)  { safeLog(logger.Warn, msg) }
func logError(msg string) { safeLog(logger.Error, msg) }

// safeLog 调用日志实现并吞掉其内部 panic。
func safeLog(fn func(string), msg string) {
	defer func() { _ = recover() }()
	fn(msg)
}
