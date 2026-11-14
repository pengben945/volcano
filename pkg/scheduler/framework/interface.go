/*
Copyright 2017 The Kubernetes Authors.
Copyright 2017-2025 The Volcano Authors.

Modifications made by Volcano authors:
- Added BindContextHandler interface for bind context extension setup

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
	k8sframework "k8s.io/kubernetes/pkg/scheduler/framework"

	"volcano.sh/volcano/pkg/scheduler/cache"
)

// Action 定义了 Volcano 调度动作（Action）的标准接口。
// Action 代表调度流程中的一个具体阶段/动作，例如入队(enqueue)、分配(allocate)、抢占(preempt)、回收(reclaim)、回填(backfill)等。
// 在每个调度周期中，配置中指定的一系列 Action 将按序执行。
type Action interface {
	// Name 返回该 Action 的唯一名称（如 "enqueue", "allocate", "preempt"）。
	Name() string

	// Initialize 初始化 Action，在调度器启动时调用一次，用于加载必要的资源或初始化状态。
	Initialize()

	// Execute 执行调度动作的核心逻辑，接收当前调度周期的会话（Session）快照，
	// 通过调用 Session 中的插件扩展点和事务操作来完成本阶段的资源调度。
	Execute(ssn *Session)

	// UnInitialize 反初始化 Action，在调度器关闭或重载时释放资源。
	UnInitialize()
}

// Plugin 定义了 Volcano 调度插件（Plugin）的标准接口。
// 插件是 Volcano 可扩展算法的核心实现载体（如 gang, drf, proportion, predicates, priority, nodeorder 等）。
// 插件通过在 Session 上注册回调函数（扩展点/Hooks）来影响调度决策。
type Plugin interface {
	// Name 返回该 Plugin 的唯一名称（如 "gang", "drf", "predicates"）。
	Name() string

	// OnSessionOpen 在每次调度周期的会话（Session）打开时被调用。
	// 插件在此方法中向 Session 注册各类过滤、打分、排序、准入和抢占等回调函数。
	OnSessionOpen(ssn *Session)

	// OnSessionClose 在每次调度周期的会话即将关闭时被调用。
	// 插件在此方法中执行清理、状态更新（如回写 PodGroup 状态）或指标上报等收尾工作。
	OnSessionClose(ssn *Session)
}

// BindContextHandler 定义了设置绑定上下文（BindContext）扩展信息的接口。
// 允许插件（如 DRA、网络插件等）在 Pod 绑定到节点前，将必要的周期状态（CycleState）传递至异步绑定上下文中。
type BindContextHandler interface {
	// SetupBindContextExtension 允许插件在 BindContext 中注入扩展数据，
	// 便于在后置的 Bind 阶段使用这些上下文信息。
	SetupBindContextExtension(state *k8sframework.CycleState, bindCtx *cache.BindContext)
}
