/*
Copyright 2018 The Kubernetes Authors.
Copyright 2018-2023 The Volcano Authors.

Modifications made by Volcano authors:
- Enhanced preemption logic with support for both inter-job and intra-job priority-based preemption
- Added job starving detection functionality to improve resource allocation efficiency

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

package priority

import (
	"k8s.io/klog/v2"

	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/framework"
	"volcano.sh/volcano/pkg/scheduler/plugins/util"
)

// PluginName indicates name of volcano scheduler plugin.
// 插件的唯一名称，用于在配置文件 (scheduler conf) 和调度框架中唯一标识该插件。
const PluginName = "priority"

// priorityPlugin 实现了 framework.Plugin 接口，负责基于优先级的调度与抢占。
// 它主要提供两大核心能力：
// 1. 静态优先级排序：支持 Job 间排序 (JobOrder)、同 Job 内的 Task 间排序 (TaskOrder) 以及 SubJob 间排序；
// 2. 优先级抢占控制：支持同队列内 Job 之间的抢占 (Inter-Job) 和同 Job 内部不同 Task 之间的抢占 (Intra-Job)，
//    确保高优先级的作业或任务优先获得资源，低优先级的作业或任务在资源匮乏时被让渡或抢占。
type priorityPlugin struct {
	// Arguments given for the plugin
	// 保存从配置文件中传递给 priority 插件的自定义参数。
	pluginArguments framework.Arguments
}

// New return priority plugin
// 插件工厂方法，符合 framework.PluginBuilder 签名，用于创建 priorityPlugin 实例。
func New(arguments framework.Arguments) framework.Plugin {
	return &priorityPlugin{pluginArguments: arguments}
}

// Name 返回插件的唯一名称 "priority"。
func (pp *priorityPlugin) Name() string {
	return PluginName
}

// OnSessionOpen 在每个调度会话（Session）打开时被框架调用。
// priority 插件在此方法中向 Session 注册核心扩展点：
// - AddTaskOrderFn: 决定同一个 Job 内不同 Task（Pod）的调度先后顺序；
// - AddJobOrderFn: 决定同一个 Queue 内不同 Job 之间的调度先后顺序；
// - AddSubJobOrderFn: 决定子作业之间的调度先后顺序；
// - AddPreemptableFn: 过滤在抢占 (preempt) 阶段哪些 Pod 可以被牺牲 (Victims)；
// - AddUnifiedEvictableFn: 统一驱逐架构下的受害者过滤；
// - AddJobStarvingFns: 判定 Job 是否处于饥饿状态，决定其是否具备发起抢占的资格。
func (pp *priorityPlugin) OnSessionOpen(ssn *framework.Session) {
	// 1. 注册 Task（Pod 级别）排序函数
	// 适用场景：同一个 Job 内部包含多个 Task 时（例如一个 Job 包含 1 个 master 和 10 个 worker），
	// 调度器在为该 Job 挑选 Task 逐一进行分配 (allocate) 时，优先选择优先级高的 Task 先调度。
	taskOrderFn := func(l interface{}, r interface{}) int {
		lv := l.(*api.TaskInfo)
		rv := r.(*api.TaskInfo)

		klog.V(4).Infof("Priority TaskOrder: <%v/%v> priority is %v, <%v/%v> priority is %v",
			lv.Namespace, lv.Name, lv.Priority, rv.Namespace, rv.Name, rv.Priority)

		if lv.Priority == rv.Priority {
			return 0
		}

		// 注意：Volcano 中的 TaskOrderFn 排序约定：
		// 返回 -1 表示 lv 应该排在 rv 前面（高优先级排前面，先调度）；返回 1 表示 lv 排在后面。
		if lv.Priority > rv.Priority {
			return -1
		}

		return 1
	}

	// Add Task Order function
	ssn.AddTaskOrderFn(pp.Name(), taskOrderFn)

	// 2. 注册 Job 级别排序函数
	// 适用场景：在队列分配 (allocate) 或入队 (enqueue) 阶段，决定哪个 Job 优先出队并分配资源。
	jobOrderFn := func(l, r interface{}) int {
		lv := l.(*api.JobInfo)
		rv := r.(*api.JobInfo)

		klog.V(4).Infof("Priority JobOrderFn: <%v/%v> priority: %d, <%v/%v> priority: %d",
			lv.Namespace, lv.Name, lv.Priority, rv.Namespace, rv.Name, rv.Priority)

		// 返回 -1 表示 lv 优先级高于 rv，lv 排在前面，优先调度
		if lv.Priority > rv.Priority {
			return -1
		}

		// 返回 1 表示 lv 优先级低于 rv，rv 排在前面
		if lv.Priority < rv.Priority {
			return 1
		}

		// 优先级相同时返回 0，让出决策权，交给责任链中后续的插件（如 drf、gang 等）继续裁决
		return 0
	}

	ssn.AddJobOrderFn(pp.Name(), jobOrderFn)

	// 3. 注册 SubJob（子作业）级别排序函数
	// 针对多层级复杂作业结构中的 SubJob，按 SubJob 的 Priority 进行降序排列。
	subJobOrderFn := func(l, r interface{}) int {
		lv := l.(*api.SubJobInfo)
		rv := r.(*api.SubJobInfo)

		klog.V(4).Infof("Priority SubJobOrderFn: <%v> priority: %d, <%v> priority: %d",
			lv.UID, lv.Priority, rv.UID, rv.Priority)

		if lv.Priority > rv.Priority {
			return -1
		}

		if lv.Priority < rv.Priority {
			return 1
		}

		return 0
	}

	ssn.AddSubJobOrderFn(pp.Name(), subJobOrderFn)

	// 4. 注册可抢占过滤函数 (PreemptableFn)
	// 在 preempt（抢占）动作中，当高优先级任务因为资源不足无法调度时，框架会寻找一批可能被驱逐的候选者 (preemptees)。
	// 本函数逐一审查候选者，只允许抢占优先级严格低于抢占者 (preemptor) 的任务，绝对禁止低抢高或同级抢占！
	preemptableFn := func(preemptor *api.TaskInfo, preemptees []*api.TaskInfo) ([]*api.TaskInfo, int) {
		preemptorJob := ssn.Jobs[preemptor.Job]
		if preemptorJob == nil {
			klog.Warningf("[priority] Skip preemption: preemptor job <%s> not found in session (orphaned task from deleted PodGroup)",
				preemptor.Job)
			return nil, util.Permit
		}

		var victims []*api.TaskInfo
		for _, preemptee := range preemptees {
			preempteeJob := ssn.Jobs[preemptee.Job]
			if preempteeJob == nil {
				klog.Warningf("[priority] Skip preemptee <%s/%s>: job <%s> not found in session (orphaned task from deleted PodGroup)",
					preemptee.Namespace, preemptee.Name, preemptee.Job)
				continue
			}

			// 情况 A：跨 Job 抢占 (Inter-Job Preemption，同一个 Queue 内不同作业之间)
			if preempteeJob.UID != preemptorJob.UID {
				// 保护机制：被抢占者的 Job 优先级大于或等于抢占者的 Job 优先级，严禁抢占！
				if preempteeJob.Priority >= preemptorJob.Priority { // Preemption between Jobs within Queue
					klog.V(4).Infof("[priority] Can not preempt task <%v/%v> "+
						"because preemptee job has greater or equal job priority (%d) than preemptor (%d)",
						preemptee.Namespace, preemptee.Name, preempteeJob.Priority, preemptorJob.Priority)
				} else {
					// 仅当抢占者 Job 优先级严格大于被抢者 Job 优先级时，该任务才被选入受害者名单
					victims = append(victims, preemptee)
				}
			} else {
				// 情况 B：同 Job 内部抢占 (Intra-Job Preemption，同一作业内部不同 Task 之间)
				// 常见于一个大型分布式作业内部资源受限，优先保证核心 Task（如 master）能够抢占次要 Task（如 worker）。
				// 此时比较 Task 级别的 Priority：
				if preemptee.Priority >= preemptor.Priority {
					klog.V(4).Infof("[priority] Can not preempt task <%v/%v> "+
						"because preemptee task has greater or equal task priority (%d) than preemptor (%d)",
						preemptee.Namespace, preemptee.Name, preemptee.Priority, preemptor.Priority)
				} else {
					victims = append(victims, preemptee)
				}
			}
		}

		klog.V(4).Infof("[priority] Victims from Priority plugin are %+v", victims)
		return victims, util.Permit
	}
	ssn.AddPreemptableFn(pp.Name(), preemptableFn)

	// Currently handles GangReclaim and GangPreempt only.
	// Support for legacy task-level preempt/reclaim will be added in the future.
	// 5. 注册统一驱逐判定函数 (UnifiedEvictableFn)
	// 统一驱逐架构将传统的 Preempt（队列内抢占）和 Reclaim（跨队列回收）抽象为统一模型。
	ssn.AddUnifiedEvictableFn(pp.Name(), func(evictCtx *api.EvictionContext, candidates []*api.TaskInfo) ([]*api.TaskInfo, int) {
		// 跨队列回收 (GangReclaim)：
		// Reclaim 的核心是由队列间资源配额公平性驱动的（从超配额的队列中收回资源还给欠配额的队列）。
		// 优先级插件不对 Reclaim 做拦截（所有候选者都放行），由 capacity/proportion 插件主导。
		if evictCtx.Kind == api.EvictionKindGangReclaim {
			// Reclaim is fairness-driven (cross-queue); priority does not gate
			// reclaimability — consistent with legacy reclaim where priority
			// registers no ReclaimableFn.
			return candidates, util.Permit
		}

		// 队列内抢占 (GangPreempt)：
		// 仍然遵循优先级铁律：只有被驱逐候选者所在 Job 的优先级严格低于发起抢占者的 Job 优先级时，才允许驱逐。
		var victims []*api.TaskInfo
		for _, candidate := range candidates {
			candidateJob := ssn.Jobs[candidate.Job]
			if candidateJob.Priority >= evictCtx.Job.Priority {
				klog.V(4).Infof("Can not evict task <%v/%v> because victim job priority (%d) >= preemptor job priority (%d)",
					candidate.Namespace, candidate.Name, candidateJob.Priority, evictCtx.Job.Priority)
			} else {
				victims = append(victims, candidate)
			}
		}
		klog.V(4).Infof("Victims from Priority UnifiedEvictableFn are %+v", victims)
		return victims, util.Permit
	})

	// 6. 注册作业饥饿检测函数 (JobStarvingFn)
	// 在 preempt Action 中，只有被判定为“饥饿 (Starving)”状态的作业，才有资格触发抢占流程。
	// 判定标准：当前作业处于 Ready（已运行）+ Waiting（等待中）的 Task 数量小于该作业声明的 Tasks 总数。
	// 说明该作业还有 Task 无法被调度分配，需要通过抢占其他低优先级作业来补齐资源。
	jobStarvingFn := func(obj interface{}) bool {
		ji := obj.(*api.JobInfo)
		return ji.ReadyTaskNum()+ji.WaitingTaskNum() < int32(len(ji.Tasks))
	}
	ssn.AddJobStarvingFns(pp.Name(), jobStarvingFn)
}

// OnSessionClose 在调度会话结束时调用。
// 由于 priority 插件只是一个无状态的比较和过滤规则集合，不需要在会话结束时更新 CRD 状态或持久化数据，
// 因此此处为空实现。
func (pp *priorityPlugin) OnSessionClose(ssn *framework.Session) {}
