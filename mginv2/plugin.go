// Package mginv2 是七牛 Kodo 插件面向 mgin v2 插件接口（plugin.Plugin）的适配层。
//
// 根包 github.com/maczh/kodo 保持 v1(jh) MginPlugin 语义（Init([]byte)/Close()/Check() error）；
// 本包把这些方法包装成 v2 的 plugin.Plugin：
//
//	Name() string             -> "kodo"
//	Order() int               -> 60（介于 registry(30) 与 job(90) 之间）
//	Init(ctx) error           -> 读取配置 -> legacy Init -> 校验就绪
//	Close(ctx) error          -> legacy Close
//	Health() error            -> legacy Check
//	Enabled() bool            -> go.config.used 中是否包含组件名
//
// 用法：
//
//	import (
//	    "github.com/maczh/kodo/mginv2"
//	    mginv2pkg "github.com/maczh/mgin/v2/pkg/plugin"
//	)
//
//	mginv2pkg.Register(mginv2.New())              // 默认组件名 "kodo"
//	mginv2pkg.Register(mginv2.NewNamed("kodo2"))  // 多实例（配置前缀同名）
package mginv2

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/maczh/kodo"
	"github.com/maczh/mgin/v2/pkg/config"
	"github.com/maczh/mgin/v2/pkg/plugin"
	"gopkg.in/yaml.v3"
)

const (
	// PluginName 默认组件名，同时也是 go.config.used 中的关键字
	PluginName = "kodo"
	// PluginOrder 插件启动顺序：大于 DB(10)/缓存(15)/注册中心(30)，小于 job(90)
	PluginOrder = 60
)

// legacy 是根包实现的 mgin v1(jh) 插件接口。
// 根包返回的具体类型未导出，因此这里用最小接口持有它，避免依赖其内部类型。
type legacy interface {
	Init(configData []byte)
	Close()
	Check() error
}

// readyChecker 是根包提供的「无网络请求的就绪检查」扩展接口。
// 使用接口断言而非直接依赖，便于根包与适配器解耦。
type readyChecker interface {
	Ready() error
}

// kodoPlugin 实现 mgin v2 的 plugin.Plugin 接口
type kodoPlugin struct {
	name   string
	client legacy
}

// New 创建组件名为 "kodo" 的 v2 插件
func New() plugin.Plugin {
	return NewNamed(PluginName)
}

// NewNamed 创建指定组件名的 v2 插件。
// name 同时用作 go.config.used 的匹配关键字与 go.config.prefix.<name> 的配置前缀。
//
// 注意：底层持有的是根包的**进程级单例**客户端，因此同一进程内多个 name 共享同一份
// 七牛账号配置（name 只影响配置前缀与 enabled 判定），不要用它在一个进程里挂两套账号。
func NewNamed(name string) plugin.Plugin {
	trimmed := strings.TrimSpace(name)
	if trimmed == "" {
		trimmed = PluginName
	}
	return &kodoPlugin{name: trimmed, client: kodo.NewKodo()}
}

// Name 返回组件名
func (p *kodoPlugin) Name() string {
	if p == nil {
		return ""
	}
	return p.name
}

// Order 返回启动顺序
func (p *kodoPlugin) Order() int {
	return PluginOrder
}

// Init 初始化插件：取配置字节 -> legacy Init -> 就绪校验（不发起网络请求）。
//
// 配置来源优先级：
//  1. go.config.prefix.<name> 指向的独立配置文件（与 mgin 其他组件一致）；
//  2. 当前 application.yml 中的 go.<name> 节点（便于零额外文件接入）。
func (p *kodoPlugin) Init(ctx context.Context) (err error) {
	if p == nil {
		return errors.New("kodo: nil plugin")
	}
	if p.client == nil {
		return errors.New("kodo: plugin client is nil")
	}
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("kodo: panic in Init: %s", safePanicText(r))
		}
	}()
	if ctx != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
	}
	data := p.configData()
	if len(data) == 0 {
		return fmt.Errorf("%w: 未取到 go.%s 配置，请检查 go.config.used 与 go.config.prefix.%s",
			kodo.ErrInvalidConfig, p.name, p.name)
	}
	p.client.Init(data)
	// legacy Init 无返回值，通过 Ready 判断是否真正初始化成功
	if checker, ok := p.client.(readyChecker); ok {
		if readyErr := checker.Ready(); readyErr != nil {
			// 配置显式 enabled: false 属于「按预期禁用」，不应让进程启动失败
			if errors.Is(readyErr, kodo.ErrDisabled) {
				return nil
			}
			return fmt.Errorf("kodo: 初始化后未就绪: %w", readyErr)
		}
	}
	if ctx != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
	}
	return nil
}

// Close 关闭插件。
//
// 与其他生命周期入口一样自带 recover：mgin 的关闭流程通常在进程退出阶段，
// 此时任何 panic 都没有外层兜底，会直接把「优雅关闭」变成崩溃退出。
func (p *kodoPlugin) Close(ctx context.Context) (err error) {
	if p == nil || p.client == nil {
		return nil
	}
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("kodo: panic in Close: %s", safePanicText(r))
		}
	}()
	if ctx != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
	}
	p.client.Close()
	return nil
}

// Health 探活：对默认桶做一次 GetBucketInfo
func (p *kodoPlugin) Health() (err error) {
	if p == nil || p.client == nil {
		return errors.New("kodo: plugin not constructed")
	}
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("kodo: panic in Health: %s", safePanicText(r))
		}
	}()
	return p.client.Check()
}

// Enabled 判断 go.config.used 中是否包含本组件名
func (p *kodoPlugin) Enabled() bool {
	if p == nil {
		return false
	}
	if config.Config == nil {
		return false
	}
	for _, item := range strings.Split(config.Config.Config.Used, ",") {
		if strings.TrimSpace(item) == p.name {
			return true
		}
	}
	return false
}

// configData 按优先级解析配置字节；无可用配置时返回 nil。
func (p *kodoPlugin) configData() []byte {
	if config.Config == nil {
		return nil
	}
	prefix := config.Config.GetConfigString("go.config.prefix." + p.name)
	if prefix == "" {
		prefix = p.name
	}
	if data := config.Config.GetConfigData(prefix); len(data) > 0 {
		return data
	}
	return p.inlineConfigData()
}

// inlineConfigData 回退到 application.yml 内的 go.<name> 节点。
func (p *kodoPlugin) inlineConfigData() []byte {
	if config.Config == nil || config.Config.Cnf == nil {
		return nil
	}
	node := config.Config.Cnf.Cut("go." + p.name).Raw()
	if len(node) == 0 {
		return nil
	}
	data, err := yaml.Marshal(node)
	if err != nil {
		return nil
	}
	return data
}

// safePanicText 安全格式化 panic 值。
// panic 值可能自带会 panic 的 String()/Error()，在 recover 路径里二次 panic
// 会直接终止进程（没有更外层 recover），因此这里再兜一层。
func safePanicText(r any) (text string) {
	defer func() {
		if recover() != nil {
			text = fmt.Sprintf("<unprintable panic value: %T>", r)
		}
	}()
	return fmt.Sprintf("%v", r)
}

// 编译期断言：适配器实现 mgin v2 插件接口
var _ plugin.Plugin = (*kodoPlugin)(nil)
