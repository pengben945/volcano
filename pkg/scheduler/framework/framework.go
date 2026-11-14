/*
Copyright 2018 The Kubernetes Authors.
Copyright 2018-2023 The Volcano Authors.

Modifications made by Volcano authors:
- Enhanced session initialization with configuration support

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
	"time"

	"k8s.io/klog/v2"

	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/cache"
	"volcano.sh/volcano/pkg/scheduler/conf"
	"volcano.sh/volcano/pkg/scheduler/metrics"
	"volcano.sh/volcano/pkg/scheduler/unschedulable"
)

// sessionOptions 保存构建 Session 时的可选配置参数。
type sessionOptions struct {
	// unschedulableJobCache 不可调度 Job 缓存，用于跨 Session 缓存判定结果，避免无意义的重复计算。
	unschedulableJobCache unschedulableJobCache
}

// unschedulableJobCache 定义了调度会话（Session）使用的不可调度 Job 缓存接口。
// 它能够在调度失败时记录 Rejection 原因，并在下个周期通过 HintProvider 判断集群事件是否足以触发重试。
type unschedulableJobCache interface {
	// AddHintProvider 为指定插件注册调度提示提供器，用于感知外部事件并尝试激活不可调度作业
	AddHintProvider(pluginName string, provider unschedulable.HintProvider)
	// BeginSession 开启新的 Session 缓存上下文
	BeginSession()
	// Record 记录作业的不可调度原因（Rejections）
	Record(job *api.JobInfo, rejections []unschedulable.Rejection)
	// CachedRejections 获取作业此前缓存的拒绝原因
	CachedRejections(job *api.JobInfo) []unschedulable.Rejection
	// Forget 从缓存中移除指定的 Job
	Forget(jobID api.JobID)
}

// SessionOption 用于配置 Session 可选依赖的函数式选项模式。
type SessionOption func(*sessionOptions)

// WithUnschedulableJobCache 配置调度会话所使用的不可调度作业缓存。
func WithUnschedulableJobCache(jobCache unschedulableJobCache) SessionOption {
	return func(options *sessionOptions) {
		options.unschedulableJobCache = jobCache
	}
}

// OpenSession 开启一个新的调度周期会话（Session）。
// 这是每次调度周期的入口点：
// 1. 从 schedulerCache 创建当前集群状态的数据快照（Nodes, Jobs, Queues 等）；
// 2. 根据配置加载分层的插件配置（tiers）和全局配置（configurations）；
// 3. 实例化所有配置的插件，并依次触发插件的 OnSessionOpen 回调，注册插件函数；
// 4. 初始化 Kubernetes framework CycleState 并应用不可调度作业的跳过策略。
func OpenSession(schedulerCache cache.Cache, tiers []conf.Tier, configurations []conf.Configuration, opts ...SessionOption) *Session {
	openStart := time.Now()
	options := &sessionOptions{}
	for _, opt := range opts {
		opt(options)
	}
	// 创建底层 Session 快照对象
	ssn := openSession(schedulerCache, options.unschedulableJobCache)
	ssn.Tiers = tiers
	ssn.Configurations = configurations
	ssn.NodeMap = GenerateNodeMapAndSlice(ssn.Nodes)
	ssn.PodLister = NewPodLister(ssn)

	// 遍历多层级插件配置，按层级依次初始化插件并调用 OnSessionOpen
	for _, tier := range tiers {
		for _, plugin := range tier.Plugins {
			if pb, found := GetPluginBuilder(plugin.Name); !found {
				klog.Errorf("Failed to get plugin %s.", plugin.Name)
			} else {
				plugin := pb(plugin.Arguments)
				ssn.plugins[plugin.Name()] = plugin
				onSessionOpenStart := time.Now()
				// 触发插件的 OnSessionOpen，插件在此方法中向 ssn 注册扩展点（如 JobOrderFn, PreemptableFn 等）
				plugin.OnSessionOpen(ssn)
				metrics.UpdatePluginDuration(plugin.Name(), metrics.OnSessionOpen, metrics.Duration(onSessionOpenStart))
			}
		}
	}

	// 初始化与 k8s scheduler framework 兼容的 CycleState
	ssn.InitCycleState()
	// 应用不可调度作业的缓存跳过逻辑，避免重复调度已判定不可调度的任务
	ssn.applyCachedSkips()
	// 记录 Session 开启耗时监控指标
	metrics.UpdateOpenSessionDuration(time.Since(openStart))

	return ssn
}

// CloseSession 关闭当前的调度周期会话。
// 调度周期结束时的收尾工作：
// 1. 依次调用所有插件的 OnSessionClose 回调，插件可在此处写回状态（如 PodGroup Condition）或记录指标；
// 2. 关闭底层 Session，清理快照数据并提交缓存更新。
func CloseSession(ssn *Session) {
	for _, plugin := range ssn.plugins {
		onSessionCloseStart := time.Now()
		plugin.OnSessionClose(ssn)
		metrics.UpdatePluginDuration(plugin.Name(), metrics.OnSessionClose, metrics.Duration(onSessionCloseStart))
	}

	closeSession(ssn)
}
