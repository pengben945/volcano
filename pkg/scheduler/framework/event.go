/*
Copyright 2018 The Kubernetes Authors.
Copyright 2018-2024 The Volcano Authors.

Modifications made by Volcano authors:
- Added Err field to Event structure for error handling

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
	"volcano.sh/volcano/pkg/scheduler/api"
)

// Event 表示调度框架内部在资源变更（分配或释放）时产生的事件。
// 当 Task 被 Allocate（分配）、Evict（驱逐）或恢复时，框架会触发该事件并通知已注册的 EventHandler。
type Event struct {
	// Task 涉及该事件的 Task 信息
	Task *api.TaskInfo
	// Err 操作过程中发生的错误（若有）
	Err  error
	// Restore 表示当前事件是否是在事务回滚（Discard Statement）时恢复任务状态。
	// 例如：当抢占失败事务回滚时，被标记驱逐的受害者 Pod 会被重新恢复（unevict）。
	// 此时 Task 仍然绑定在原节点上，事件处理器仅需恢复自身缓存状态，而不需要重复执行首次准入的完整流程。
	Restore bool
}

// EventHandler 定义事件监听处理器。
// 调度插件或框架组件可实现该结构，在 Task 分配（Allocate）或释放（Deallocate）时得到通知以同步其内部状态。
type EventHandler struct {
	// AllocateFunc 当任务成功分配节点资源（Allocate）时触发的回调函数
	AllocateFunc   func(event *Event)
	// DeallocateFunc 当任务被释放或驱逐（Evict/Deallocate）时触发的回调函数
	DeallocateFunc func(event *Event)
}
