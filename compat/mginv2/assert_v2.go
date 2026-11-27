// Package mginv2 是 mgin v2 分支的编译期挂接断言。
//
// 该模块证明 github.com/maczh/kodo/mginv2 的适配器确实实现了
// mgin v2 的 plugin.Plugin 接口（Name/Order/Init/Close/Health/Enabled）。
//
// 构建：
//
//	cd compat/mginv2 && GOPROXY=https://goproxy.cn,direct go build ./...
package mginv2

import (
	"context"

	kodov2 "github.com/maczh/kodo/mginv2"
	"github.com/maczh/mgin/v2/pkg/plugin"
)

// 编译期断言 1：默认构造返回 plugin.Plugin
var _ plugin.Plugin = kodov2.New()

// 编译期断言 2：具名构造同样返回 plugin.Plugin
var _ plugin.Plugin = kodov2.NewNamed("kodo")

// Register 演示如何把插件注册进 v2 插件管理器（仅编译期验证，不会被执行）。
func Register() error {
	p := kodov2.New()
	if p.Order() != kodov2.PluginOrder {
		return nil
	}
	plugin.Register(p)
	return nil
}

// Lifecycle 演示 v2 生命周期的标准调用顺序（仅编译期验证）。
func Lifecycle(ctx context.Context) error {
	p := kodov2.New()
	if !p.Enabled() {
		return nil
	}
	if err := p.Init(ctx); err != nil {
		return err
	}
	if err := p.Health(); err != nil {
		_ = p.Close(ctx)
		return err
	}
	return p.Close(ctx)
}
