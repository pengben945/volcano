/*
Copyright 2019 The Kubernetes Authors.
Copyright 2019-2025 The Volcano Authors.

Modifications made by Volcano authors:
- Added generic argument parsing support with automatic type conversion
- Enhanced argument handling to support multiple data types
- Added utility functions for reading action arguments from scheduler configuration

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
	"strconv"

	"github.com/mitchellh/mapstructure"
	"k8s.io/klog/v2"

	"volcano.sh/volcano/pkg/scheduler/conf"
)

// Arguments 存储插件或动作的配置参数键值对（来自调度器配置文件中的 arguments 字段）。
type Arguments map[string]interface{}

// GetInt 从 Arguments 中提取键为 key 的整数值并写入 ptr。
// 如果键不存在或转换失败，则保持 ptr 原值不变并记录警告日志。
func (a Arguments) GetInt(ptr *int, key string) {
	if ptr == nil {
		return
	}

	argv, ok := a[key]
	if !ok {
		return
	}

	value, ok := argv.(int)
	if !ok {
		klog.Warningf("Could not parse argument: %v for key %s to int", argv, key)
		return
	}

	*ptr = value
}

// GetFloat64 从 Arguments 中提取键为 key 的 float64 浮点数值并写入 ptr。
// 支持将 int 自动提升转换为 float64；若转换失败则记录警告日志。
func (a Arguments) GetFloat64(ptr *float64, key string) {
	if ptr == nil {
		return
	}

	argv, ok := a[key]
	if !ok {
		return
	}

	value, ok := argv.(float64)
	if !ok {
		if intVal, ok := argv.(int); ok {
			*ptr = float64(intVal)
			return
		}
		klog.Warningf("Could not parse argument: %v for key %s to float64", argv, key)
		return
	}

	*ptr = value
}

// GetBool 从 Arguments 中提取键为 key 的布尔值并写入 ptr。
// 支持原生 bool 类型以及字符串类型（"true"/"false" 等）的解析；若转换失败则记录警告日志。
func (a Arguments) GetBool(ptr *bool, key string) {
	if ptr == nil {
		return
	}

	argv, ok := a[key]
	if !ok {
		return
	}

	value, ok := argv.(bool)
	if !ok {
		if stringValue, ok := argv.(string); ok {
			parsed, err := strconv.ParseBool(stringValue)
			if err != nil {
				klog.Warningf("Could not parse argument: %v for key %s to bool", argv, key)
				return
			}
			*ptr = parsed
			return
		}
		klog.Warningf("Could not parse argument: %v for key %s to bool", argv, key)
		return
	}

	*ptr = value
}

// GetString 从 Arguments 中提取键为 key 的字符串并写入 ptr。
// 若键不存在或非 string 类型，则记录警告并保持原值。
func (a Arguments) GetString(ptr *string, key string) {
	if ptr == nil {
		return
	}

	argv, ok := a[key]
	if !ok {
		return
	}

	value, ok := argv.(string)
	if !ok {
		klog.Warningf("Could not parse argument: %v for key %s to string", argv, key)
		return
	}

	*ptr = value
}

// Get 是泛型参数提取函数，利用 mapstructure 将参数自动反序列化为泛型类型 T（如复杂结构体或切片）。
// - 若参数解析成功，返回转换后的值和 true；
// - 若参数不存在，返回默认零值和 false；
// - 若参数存在但解析类型失败，则输出 Fatal 日志并终止程序。
func Get[T any](a Arguments, key string) (T, bool) {
	var result T
	argv, ok := a[key]
	if !ok {
		return result, false
	}

	err := mapstructure.Decode(argv, &result)
	if err != nil {
		klog.Fatalf("Could not parse argument for key %s to type %T: %v", key, result, err)
	}

	return result, true
}

// GetArgOfActionFromConf 从调度器全局配置（configurations 数组）中提取指定 Action 对应的 Arguments。
func GetArgOfActionFromConf(configurations []conf.Configuration, actionName string) Arguments {
	for _, c := range configurations {
		if c.Name == actionName {
			return c.Arguments
		}
	}

	return nil
}
