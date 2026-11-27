// Package mginv1 是 v1(jh) 分支的编译期挂接断言。
//
// 该模块只做一件事：证明 github.com/maczh/kodo 的根包类型确实实现了
// mgin v1.25.40-jh 的 MginPlugin 接口（Init([]byte) / Close() / Check() error）。
// 若根包签名被改动，本模块 go build 会直接失败 —— 这就是挂接的回归护栏。
//
// 构建：
//
//	cd compat/mginv1 && GOPROXY=https://goproxy.cn,direct go build ./...
package mginv1

import (
	"github.com/maczh/kodo"
	"github.com/maczh/mgin"
)

// 编译期断言 1：NewKodo() 的返回值实现 mgin v1 MginPlugin 接口
var _ mgin.MginPlugin = kodo.NewKodo()

// 编译期断言 2：单例入口同样满足该接口
var _ mgin.MginPlugin = kodo.GetKodo()

// Register 演示如何把插件挂到 mgin v1(jh) 的 App 上。
// 本函数不会被测试调用，仅用于编译期验证挂接写法。
func Register(app *mgin.App) {
	if app == nil || app.MGin == nil {
		return
	}
	app.MGin.UsePlugin("kodo", kodo.NewKodo())
}

// LoadWithRawConfig 演示手动初始化路径（不经过 mgin 配置中心时使用）。
// 仅用于编译期验证签名，不会被实际调用。
func LoadWithRawConfig(configData []byte) error {
	k := kodo.NewKodo()
	k.Init(configData)
	if err := k.Ready(); err != nil {
		return err
	}
	return k.Check()
}
