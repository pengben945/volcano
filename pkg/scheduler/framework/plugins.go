/*
Copyright 2018 The Kubernetes Authors.
Copyright 2018-2023 The Volcano Authors.

Modifications made by Volcano authors:
- Added support for loading custom plugins from external .so files

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package framework

import (
	"fmt"
	"path/filepath"
	"plugin"
	"strings"
	"sync"

	"k8s.io/klog/v2"
)

// pluginMutex 保护 pluginBuilders 和 actionMap 的并发读写安全。
var pluginMutex sync.RWMutex

// PluginBuilder 是创建 Plugin 实例的工厂函数签名。
// 接收解析后的 Arguments 参数，返回具体 Plugin 实例。
type PluginBuilder = func(Arguments) Plugin

// pluginBuilders 保存所有已注册的插件构建函数，以插件名称为键。
var pluginBuilders = map[string]PluginBuilder{}

// RegisterPluginBuilder 注册一个插件构建器。
// 内置插件通常在各插件包的 init() 函数中调用此方法完成自我注册。
func RegisterPluginBuilder(name string, pc PluginBuilder) {
	pluginMutex.Lock()
	defer pluginMutex.Unlock()

	pluginBuilders[name] = pc
}

// CleanupPluginBuilders 清空所有已注册的插件构建器（主要用于单元测试隔离）。
func CleanupPluginBuilders() {
	pluginMutex.Lock()
	defer pluginMutex.Unlock()

	pluginBuilders = map[string]PluginBuilder{}
}

// GetPluginBuilder 根据插件名称查找对应的插件构建器。
func GetPluginBuilder(name string) (PluginBuilder, bool) {
	pluginMutex.RLock()
	defer pluginMutex.RUnlock()

	pb, found := pluginBuilders[name]
	return pb, found
}

// LoadCustomPlugins 从指定目录动态加载以 Go Plugin (.so) 形式构建的外部自定义插件。
// 使得用户无需重新编译整个 Volcano 调度器即可扩展调度算法。
func LoadCustomPlugins(pluginsDir string) error {
	pluginPaths, _ := filepath.Glob(fmt.Sprintf("%s/*.so", pluginsDir))
	for _, pluginPath := range pluginPaths {
		pluginBuilder, err := loadPluginBuilder(pluginPath)
		if err != nil {
			return err
		}
		pluginName := getPluginName(pluginPath)
		RegisterPluginBuilder(pluginName, pluginBuilder)
		klog.V(4).Infof("Custom plugin %s loaded", pluginName)
	}

	return nil
}

// getPluginName 从动态库文件路径中提取插件名（去掉后缀）。
func getPluginName(pluginPath string) string {
	return strings.TrimSuffix(filepath.Base(pluginPath), filepath.Ext(pluginPath))
}

// loadPluginBuilder 从动态库 (.so) 中解析符号 "New" 并转换为 PluginBuilder。
func loadPluginBuilder(pluginPath string) (PluginBuilder, error) {
	plug, err := plugin.Open(pluginPath)
	if err != nil {
		return nil, err
	}

	symBuilder, err := plug.Lookup("New")
	if err != nil {
		return nil, err
	}

	builder, ok := symBuilder.(PluginBuilder)
	if !ok {
		return nil, fmt.Errorf("unexpected plugin: %s, failed to convert PluginBuilder `New`", pluginPath)
	}

	return builder, nil
}

// actionMap 保存所有已注册的 Action 实例，以 Action 名称为键（如 "enqueue", "allocate" 等）。
var actionMap = map[string]Action{}

// RegisterAction 注册一个 Action 动作实现。
// 内置 Action 在各自包的 init() 中调用此方法注册。
func RegisterAction(act Action) {
	pluginMutex.Lock()
	defer pluginMutex.Unlock()

	actionMap[act.Name()] = act
}

// GetAction 根据动作名称查找对应的 Action 实例。
func GetAction(name string) (Action, bool) {
	pluginMutex.RLock()
	defer pluginMutex.RUnlock()

	act, found := actionMap[name]
	return act, found
}
