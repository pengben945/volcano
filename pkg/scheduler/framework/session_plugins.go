/*
Copyright 2018 The Kubernetes Authors.
Copyright 2019-2025 The Volcano Authors.

Modifications made by Volcano authors:
- Added additional plugin extension points

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
	"context"
	"errors"
	"sort"

	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2"
	fwk "k8s.io/kube-scheduler/framework"

	"volcano.sh/apis/pkg/apis/scheduling"
	"volcano.sh/volcano/pkg/controllers/job/helpers"
	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/unschedulable"
	"volcano.sh/volcano/pkg/scheduler/util"
)

// AddJobOrderFn 注册作业排序函数（JobOrderFn），用于在调度周期中对待调度 Job 进行优先级排序。
func (ssn *Session) AddJobOrderFn(name string, cf api.CompareFn) {
	ssn.jobOrderFns[name] = cf
}

// AddQueueOrderFn 注册队列排序函数（QueueOrderFn），用于在分配资源时确定多个队列的调度处理次序。
func (ssn *Session) AddQueueOrderFn(name string, qf api.CompareFn) {
	ssn.queueOrderFns[name] = qf
}

// AddVictimQueueOrderFn 注册受害者队列排序函数，在发生跨队列抢占/回收时决定优先从哪个队列驱逐资源。
func (ssn *Session) AddVictimQueueOrderFn(name string, vcf api.VictimCompareFn) {
	ssn.victimQueueOrderFns[name] = vcf
}

// AddClusterOrderFn 注册多集群排序函数，用于多集群调度场景中挑选最合适的集群。
func (ssn *Session) AddClusterOrderFn(name string, qf api.CompareFn) {
	ssn.clusterOrderFns[name] = qf
}

// AddTaskOrderFn 注册任务排序函数（TaskOrderFn），决定同一个 Job 内部各个 Task 被调度的先后次序。
func (ssn *Session) AddTaskOrderFn(name string, cf api.CompareFn) {
	ssn.taskOrderFns[name] = cf
}

// AddPreemptableFn 注册抢占判定函数（PreemptableFn），在同一队列内部有高优先级任务因资源不足受阻时，
// 筛选允许被该任务抢占的低优先级受害者任务列表。
func (ssn *Session) AddPreemptableFn(name string, cf api.EvictableFn) {
	if ssn.preemptableFns == nil {
		ssn.preemptableFns = map[string]api.EvictableFn{}
	}
	ssn.preemptableFns[name] = cf
}

// AddReclaimableFn 注册资源回收判定函数（ReclaimableFn），用于在跨队列场景下，
// 当某队列尚未达到保障配额（Deserved/Guarantee）时，从超用队列中筛选允许回收的受害者任务。
func (ssn *Session) AddReclaimableFn(name string, rf api.EvictableFn) {
	if ssn.reclaimableFns == nil {
		ssn.reclaimableFns = map[string]api.EvictableFn{}
	}
	ssn.reclaimableFns[name] = rf
}

// AddUnifiedEvictableFn 注册统一受害者过滤函数，支持感知 Gang 约束的驱逐判定，保证受害者被驱逐时不破坏受害者所属 Job 的最小成团数。
func (ssn *Session) AddUnifiedEvictableFn(name string, fn api.UnifiedEvictableFn) {
	if ssn.unifiedEvictableFns == nil {
		ssn.unifiedEvictableFns = map[string]api.UnifiedEvictableFn{}
	}
	ssn.unifiedEvictableFns[name] = fn
}

// AddJobReadyFn 注册作业就绪判定函数（JobReadyFn），判断 Job 是否已满足最小可用任务数（minAvailable）等 Gang 就绪条件。
func (ssn *Session) AddJobReadyFn(name string, vf api.ValidateFn) {
	ssn.jobReadyFns[name] = vf
}

// AddJobPipelinedFn 注册作业流水线判定函数（JobPipelinedFn），判断 Job 是否在等待被驱逐任务释放资源后能够就绪（Pipelined 状态）。
func (ssn *Session) AddJobPipelinedFn(name string, vf api.VoteFn) {
	ssn.jobPipelinedFns[name] = vf
}

// AddPredicateFn 注册节点预选过滤函数（PredicateFn），用于判断单个 Node 是否满足 Task 的硬性运行约束（如资源余量、节点亲和、污点容忍等）。
func (ssn *Session) AddPredicateFn(name string, pf api.PredicateFn) {
	ssn.predicateFns[name] = pf
}

// AddQueueNodeFilterFn 注册队列维度的节点过滤器，预先过滤掉不属于该队列可用范围的节点。
func (ssn *Session) AddQueueNodeFilterFn(name string, fn api.QueueNodeFilterFn) {
	ssn.queueNodeFilterFns[name] = fn
}

// AddPrePredicateFn 注册预选前置检查函数（PrePredicateFn），用于在批量预选前预先计算全局或拓扑缓存。
func (ssn *Session) AddPrePredicateFn(name string, pf api.PrePredicateFn) {
	ssn.prePredicateFns[name] = pf
}

// AddBestNodeFn 注册最优节点挑选函数，直接计算并返回任务最适合运行的节点。
func (ssn *Session) AddBestNodeFn(name string, pf api.BestNodeFn) {
	ssn.bestNodeFns[name] = pf
}

// AddNodeOrderFn 注册节点打分函数（NodeOrderFn），用于对通过预选的候选节点逐个进行打分优选。
func (ssn *Session) AddNodeOrderFn(name string, pf api.NodeOrderFn) {
	ssn.nodeOrderFns[name] = pf
}

// AddHyperNodeOrderFn 注册超节点拓扑域打分排序函数。
func (ssn *Session) AddHyperNodeOrderFn(name string, fn api.HyperNodeOrderFn) {
	ssn.hyperNodeOrderFns[name] = fn
}

// AddBatchNodeOrderFn 注册批量节点打分函数（BatchNodeOrderFn），支持对一批节点批量并行评估打分。
func (ssn *Session) AddBatchNodeOrderFn(name string, pf api.BatchNodeOrderFn) {
	ssn.batchNodeOrderFns[name] = pf
}

// AddNodeMapFn 注册 Map-Reduce 打分流程中的 Map 函数。
func (ssn *Session) AddNodeMapFn(name string, pf api.NodeMapFn) {
	ssn.nodeMapFns[name] = pf
}

// AddNodeReduceFn 注册 Map-Reduce 打分流程中的 Reduce 函数。
func (ssn *Session) AddNodeReduceFn(name string, pf api.NodeReduceFn) {
	ssn.nodeReduceFns[name] = pf
}

// AddOverusedFn 注册队列超用判定函数（OverusedFn），判断指定队列当前使用的资源是否已超出其分配配额。
func (ssn *Session) AddOverusedFn(name string, fn api.ValidateFn) {
	ssn.overusedFns[name] = fn
}

// AddPreemptiveFn 注册可抢占候选验证函数，判断特定候选是否允许被抢占。
func (ssn *Session) AddPreemptiveFn(name string, fn api.ValidateWithCandidateFn) {
	ssn.preemptiveFns[name] = fn
}

// AddAllocatableFn 注册可分配判定函数，判断当前是否允许向队列或 Job 分配资源。
func (ssn *Session) AddAllocatableFn(name string, fn api.AllocatableFn) {
	ssn.allocatableFns[name] = fn
}

// AddJobValidFn 注册作业准入校验函数（JobValidFn），在处理作业前校验其规范配置是否合法有效。
func (ssn *Session) AddJobValidFn(name string, fn api.ValidateExFn) {
	ssn.jobValidFns[name] = fn
}

// AddJobEnqueueableFn 注册作业可入队判定函数，判断一个处于 Pending 状态的 Job 当前是否允许进入 Inqueue 状态。
func (ssn *Session) AddJobEnqueueableFn(name string, fn api.VoteFn) {
	ssn.jobEnqueueableFns[name] = fn
}

// AddJobEnqueuedFn 注册作业入队后的回调函数，用于在作业入队成功后执行插件相关后置处理。
func (ssn *Session) AddJobEnqueuedFn(name string, fn api.JobEnqueuedFn) {
	ssn.jobEnqueuedFns[name] = fn
}

// AddTargetJobFn 注册目标作业查找函数，在抢占等场景下定位触发调度的目标 Job。
func (ssn *Session) AddTargetJobFn(name string, fn api.TargetJobFn) {
	ssn.targetJobFns[name] = fn
}

// AddReservedNodesFn 注册节点预留函数，将特定节点资源预留给指定作业或场景，避免被其他任务占用。
func (ssn *Session) AddReservedNodesFn(name string, fn api.ReservedNodesFn) {
	ssn.reservedNodesFns[name] = fn
}

// AddVictimTasksFns 注册受害者任务筛选函数链。
func (ssn *Session) AddVictimTasksFns(name string, fns []api.VictimTasksFn) {
	ssn.victimTasksFns[name] = fns
}

// AddJobStarvingFns 注册作业饥饿判定函数（JobStarvingFn），检测作业是否由于长时间得不到资源而处于饥饿状态，以便触发抢占。
func (ssn *Session) AddJobStarvingFns(name string, fn api.ValidateFn) {
	ssn.jobStarvingFns[name] = fn
}

func (ssn *Session) AddSimulateAddTaskFn(name string, fn api.SimulateAddTaskFn) {
	ssn.simulateAddTaskFns[name] = fn
}

func (ssn *Session) AddSimulateRemoveTaskFn(name string, fn api.SimulateRemoveTaskFn) {
	ssn.simulateRemoveTaskFns[name] = fn
}

func (ssn *Session) AddSimulateAllocatableFn(name string, fn api.SimulateAllocatableFn) {
	ssn.simulateAllocatableFns[name] = fn
}

func (ssn *Session) AddSimulatePredicateFn(name string, fn api.SimulatePredicateFn) {
	ssn.simulatePredicateFns[name] = fn
}

// AddSubJobReadyFn 注册子作业就绪判定函数。
func (ssn *Session) AddSubJobReadyFn(name string, vf api.ValidateFn) {
	ssn.subJobReadyFns[name] = vf
}

// AddSubJobPipelinedFn 注册子作业流水线判定函数。
func (ssn *Session) AddSubJobPipelinedFn(name string, vf api.VoteFn) {
	ssn.subJobPipelinedFns[name] = vf
}

// AddSubJobOrderFn 注册子作业排序函数。
func (ssn *Session) AddSubJobOrderFn(name string, fn api.CompareFn) {
	ssn.subJobOrderFns[name] = fn
}

// AddHyperNodeGradientForJobFn 注册作业级别的超节点拓扑梯度评估函数。
func (ssn *Session) AddHyperNodeGradientForJobFn(name string, fn api.HyperNodeGradientForJobFn) {
	ssn.hyperNodeGradientForJobFns[name] = fn
}

// AddHyperNodeGradientForSubJobFn 注册子作业级别的超节点拓扑梯度评估函数。
func (ssn *Session) AddHyperNodeGradientForSubJobFn(name string, fn api.HyperNodeGradientForSubJobFn) {
	ssn.hyperNodeGradientForSubJobFns[name] = fn
}

// Reclaimable 调用各插件注册的跨队列资源回收（Reclaim）函数。
// 分层（Tier）评估逻辑：
// 1. 按 Tier 优先级从高到低遍历；
// 2. 在同一 Tier 内部，多个插件返回的候选受害者名单取交集（Intersection）；
// 3. 一旦某个 Tier 的插件做出明确决策（筛选出有效受害者），立即采纳并返回，不再评估后续低优先级 Tier。
func (ssn *Session) Reclaimable(reclaimer *api.TaskInfo, reclaimees []*api.TaskInfo) []*api.TaskInfo {
	var victims []*api.TaskInfo

	for _, tier := range ssn.Tiers {
		for _, plugin := range tier.Plugins {
			if !isEnabled(plugin.EnabledReclaimable) {
				continue
			}
			rf, found := ssn.reclaimableFns[plugin.Name]
			if !found {
				continue
			}

			candidates, abstain := rf(reclaimer, reclaimees)
			if abstain == 0 {
				continue
			}
			if len(candidates) == 0 {
				victims = nil
				break
			}
			// first iteration - initialize victims list
			if victims == nil {
				victims = candidates
			} else {
				var intersection []*api.TaskInfo
				// Get intersection of victims and candidates.
				for _, v := range victims {
					for _, c := range candidates {
						if v.UID == c.UID {
							intersection = append(intersection, v)
						}
					}
				}

				// Update victims to intersection
				victims = intersection
			}
		}
		// Plugins in this tier made decision if victims is not nil
		if victims != nil {
			return victims
		}
	}

	return victims
}

// Preemptable 调用各插件注册的队列内抢占（Preempt）函数。
// 用于在同一队列内部，为高优先级任务挑选可抢占的低优先级受害者任务。语义与 Tier 评估规则与 Reclaimable 一致。
func (ssn *Session) Preemptable(preemptor *api.TaskInfo, preemptees []*api.TaskInfo) []*api.TaskInfo {
	var victims []*api.TaskInfo

	for _, tier := range ssn.Tiers {
		for _, plugin := range tier.Plugins {
			if !isEnabled(plugin.EnabledPreemptable) {
				continue
			}

			pf, found := ssn.preemptableFns[plugin.Name]
			if !found {
				continue
			}
			candidates, abstain := pf(preemptor, preemptees)
			if abstain == 0 {
				continue
			}
			// intersection will be nil if length is 0, don't need to do any more check
			if len(candidates) == 0 {
				victims = nil
				break
			}
			// first iteration - initialize victims list
			if victims == nil {
				victims = candidates
			} else {
				var intersection []*api.TaskInfo
				// Get intersection of victims and candidates.
				for _, v := range victims {
					for _, c := range candidates {
						if v.UID == c.UID {
							intersection = append(intersection, v)
						}
					}
				}

				// Update victims to intersection
				victims = intersection
			}
		}
		// Plugins in this tier made decision if victims is not nil
		if victims != nil {
			return victims
		}
	}

	return victims
}

// UnifiedEvictable 调用统一受害者过滤函数，支持感知 Gang 约束的驱逐判定。
// 分层遍历及交集求取语义与 Preemptable/Reclaimable 保持一致。
func (ssn *Session) UnifiedEvictable(ctx *api.EvictionContext, candidates []*api.TaskInfo) []*api.TaskInfo {
	var victims []*api.TaskInfo

	for _, tier := range ssn.Tiers {
		for _, plugin := range tier.Plugins {
			fn, found := ssn.unifiedEvictableFns[plugin.Name]
			if !found {
				continue
			}
			result, abstain := fn(ctx, candidates)
			if abstain == 0 {
				continue
			}
			if len(result) == 0 {
				victims = nil
				break
			}
			if victims == nil {
				victims = result
			} else {
				var intersection []*api.TaskInfo
				for _, v := range victims {
					for _, c := range result {
						if v.UID == c.UID {
							intersection = append(intersection, v)
						}
					}
				}
				victims = intersection
			}
		}
		if victims != nil {
			return victims
		}
	}

	return victims
}

// Overused 调用各插件的超用检测函数，判断指定队列当前是否超配额使用。
func (ssn *Session) Overused(queue *api.QueueInfo) bool {
	for _, tier := range ssn.Tiers {
		for _, plugin := range tier.Plugins {
			if !isEnabled(plugin.EnabledOverused) {
				continue
			}
			of, found := ssn.overusedFns[plugin.Name]
			if !found {
				continue
			}
			if of(queue) {
				return true
			}
		}
	}

	return false
}

// Preemptive 调用各插件的可抢占验证函数，校验 candidates 列表是否允许被抢占。
func (ssn *Session) Preemptive(queue *api.QueueInfo, candidates []*api.TaskInfo) bool {
	for _, tier := range ssn.Tiers {
		for _, plugin := range tier.Plugins {
			of, found := ssn.preemptiveFns[plugin.Name]
			if !isEnabled(plugin.EnablePreemptive) {
				continue
			}
			if !found {
				continue
			}
			if !of(queue, candidates) {
				return false
			}
		}
	}

	return true
}

// Allocatable 调用各插件的可分配判定函数，检查指定队列是否允许向 candidate 任务分配资源。
func (ssn *Session) Allocatable(queue *api.QueueInfo, candidate *api.TaskInfo) bool {
	for _, tier := range ssn.Tiers {
		for _, plugin := range tier.Plugins {
			if !isEnabled(plugin.EnabledAllocatable) {
				continue
			}
			af, found := ssn.allocatableFns[plugin.Name]
			if !found {
				continue
			}
			if !af(queue, candidate) {
				if ssn.unschedulableJobCacheEnabled && candidate.Job != "" {
					ssn.AddRejection(candidate.Job, plugin.Name, unschedulable.RejectionAllocatable, candidate.UID)
				}
				return false
			}
		}
	}

	return true
}

func (ssn *Session) SubJobReady(job *api.JobInfo, subJob *api.SubJobInfo) bool {
	if !job.ContainsSubJobPolicy() {
		return ssn.JobReady(job)
	}

	for _, tier := range ssn.Tiers {
		for _, plugin := range tier.Plugins {
			if !isEnabled(plugin.EnabledSubJobReady) {
				continue
			}
			fn, found := ssn.subJobReadyFns[plugin.Name]
			if !found {
				continue
			}

			if !fn(subJob) {
				return false
			}
		}
	}

	return true
}

func (ssn *Session) SubJobPipelined(job *api.JobInfo, subJob *api.SubJobInfo) bool {
	if !job.ContainsSubJobPolicy() {
		return ssn.JobPipelined(job)
	}

	var hasFound bool
	for _, tier := range ssn.Tiers {
		for _, plugin := range tier.Plugins {
			if !isEnabled(plugin.EnabledSubJobPipelined) {
				continue
			}
			fn, found := ssn.subJobPipelinedFns[plugin.Name]
			if !found {
				continue
			}

			res := fn(subJob)
			if res < 0 {
				return false
			}
			if res > 0 {
				hasFound = true
			}
		}
		// if plugin exists that votes permit, meanwhile other plugin votes abstention,
		// permit job to be pipelined, do not check next tier
		if hasFound {
			return true
		}
	}

	return true
}

// JobReady invoke jobready function of the plugins
func (ssn *Session) JobReady(obj interface{}) bool {
	for _, tier := range ssn.Tiers {
		for _, plugin := range tier.Plugins {
			if !isEnabled(plugin.EnabledJobReady) {
				continue
			}
			jrf, found := ssn.jobReadyFns[plugin.Name]
			if !found {
				continue
			}

			if !jrf(obj) {
				return false
			}
		}
	}

	return true
}

// JobPipelined invoke pipelined function of the plugins
// Check if job has get enough resource to run
func (ssn *Session) JobPipelined(obj interface{}) bool {
	var hasFound bool
	for _, tier := range ssn.Tiers {
		for _, plugin := range tier.Plugins {
			if !isEnabled(plugin.EnabledJobPipelined) {
				continue
			}
			jrf, found := ssn.jobPipelinedFns[plugin.Name]
			if !found {
				continue
			}

			res := jrf(obj)
			if res < 0 {
				return false
			}
			if res > 0 {
				hasFound = true
			}
		}
		// if plugin exists that votes permit, meanwhile other plugin votes abstention,
		// permit job to be pipelined, do not check next tier
		if hasFound {
			return true
		}
	}

	return true
}

// JobStarving invoke jobStarving function of the plugins
// Check if job still need more resource
func (ssn *Session) JobStarving(obj interface{}) bool {
	var hasFound bool
	for _, tier := range ssn.Tiers {
		for _, plugin := range tier.Plugins {
			if !isEnabled(plugin.EnabledJobStarving) {
				continue
			}
			jrf, found := ssn.jobStarvingFns[plugin.Name]
			if !found {
				continue
			}
			hasFound = true

			if !jrf(obj) {
				return false
			}
		}
		// this tier registered function
		if hasFound {
			return true
		}
	}

	return false
}

// JobValid invoke jobvalid function of the plugins
func (ssn *Session) JobValid(obj interface{}) *api.ValidateResult {
	for _, tier := range ssn.Tiers {
		for _, plugin := range tier.Plugins {
			jrf, found := ssn.jobValidFns[plugin.Name]
			if !found {
				continue
			}

			if vr := jrf(obj); vr != nil && !vr.Pass {
				return vr
			}
		}
	}

	return nil
}

// JobEnqueueable invoke jobEnqueueableFns function of the plugins
func (ssn *Session) JobEnqueueable(obj interface{}) bool {
	var hasFound bool
	for _, tier := range ssn.Tiers {
		for _, plugin := range tier.Plugins {
			if !isEnabled(plugin.EnabledJobEnqueued) {
				continue
			}
			fn, found := ssn.jobEnqueueableFns[plugin.Name]
			if !found {
				continue
			}

			res := fn(obj)
			if res < 0 {
				if ssn.unschedulableJobCacheEnabled {
					if job, ok := obj.(*api.JobInfo); ok {
						ssn.AddRejection(job.UID, plugin.Name, unschedulable.RejectionEnqueue)
					}
				}
				return false
			}
			if res > 0 {
				hasFound = true
			}
		}
		// if plugin exists that votes permit, meanwhile other plugin votes abstention,
		// permit job to be enqueueable, do not check next tier
		if hasFound {
			return true
		}
	}

	return true
}

// JobEnqueued invoke jobEnqueuedFns function of the plugins
func (ssn *Session) JobEnqueued(obj interface{}) {
	for _, tier := range ssn.Tiers {
		for _, plugin := range tier.Plugins {
			if !isEnabled(plugin.EnabledJobEnqueued) {
				continue
			}
			fn, found := ssn.jobEnqueuedFns[plugin.Name]
			if !found {
				continue
			}

			fn(obj)
		}
	}
}

// TargetJob invoke targetJobFns function of the plugins
func (ssn *Session) TargetJob(jobs []*api.JobInfo) *api.JobInfo {
	for _, tier := range ssn.Tiers {
		for _, plugin := range tier.Plugins {
			if !isEnabled(plugin.EnabledTargetJob) {
				continue
			}
			fn, found := ssn.targetJobFns[plugin.Name]
			if !found {
				continue
			}
			return fn(jobs)
		}
	}
	return nil
}

// VictimTasks returns the victims selected
func (ssn *Session) VictimTasks(tasks []*api.TaskInfo) map[*api.TaskInfo]bool {
	// different filters may add the same task to victims, so use a map to remove duplicate tasks.
	victimSet := make(map[*api.TaskInfo]bool)
	for _, tier := range ssn.Tiers {
		for _, plugin := range tier.Plugins {
			if !isEnabled(plugin.EnabledVictim) {
				continue
			}
			fns, found := ssn.victimTasksFns[plugin.Name]
			if !found {
				continue
			}
			for _, fn := range fns {
				victimTasks := fn(tasks)
				for _, victim := range victimTasks {
					victimSet[victim] = true
				}
			}
		}
		if len(victimSet) > 0 {
			return victimSet
		}
	}
	return victimSet
}

// ReservedNodes invoke ReservedNodes function of the plugins
func (ssn *Session) ReservedNodes() {
	for _, tier := range ssn.Tiers {
		for _, plugin := range tier.Plugins {
			if !isEnabled(plugin.EnabledReservedNodes) {
				continue
			}
			fn, found := ssn.reservedNodesFns[plugin.Name]
			if !found {
				continue
			}
			fn()
		}
	}
}

func (ssn *Session) SubJobOrderFn(l, r interface{}) bool {
	for _, tier := range ssn.Tiers {
		for _, plugin := range tier.Plugins {
			if !isEnabled(plugin.EnabledSubJobOrder) {
				continue
			}
			fn, found := ssn.subJobOrderFns[plugin.Name]
			if !found {
				continue
			}
			if j := fn(l, r); j != 0 {
				return j < 0
			}
		}
	}

	// If no subJob order funcs, order subJob by MatchIndex and UID.
	lv := l.(*api.SubJobInfo)
	rv := r.(*api.SubJobInfo)
	if lv.MatchIndex != rv.MatchIndex {
		return lv.MatchIndex < rv.MatchIndex
	}
	return lv.UID < rv.UID
}

// JobOrderCompareFn compares l and r by running enabled JobOrder plugins in
// order and returning the first non-zero comparison result. It returns 0 if
// all plugins consider l and r equal.
func (ssn *Session) JobOrderCompareFn(l, r interface{}) int {
	for _, tier := range ssn.Tiers {
		for _, plugin := range tier.Plugins {
			if !isEnabled(plugin.EnabledJobOrder) {
				continue
			}
			jof, found := ssn.jobOrderFns[plugin.Name]
			if !found {
				continue
			}
			if j := jof(l, r); j != 0 {
				return j
			}
		}
	}

	return 0
}

// JobOrderFn invoke joborder function of the plugins
func (ssn *Session) JobOrderFn(l, r interface{}) bool {
	if res := ssn.JobOrderCompareFn(l, r); res != 0 {
		return res < 0
	}

	// If no job order funcs, order job by CreationTimestamp first, then by UID.
	lv := l.(*api.JobInfo)
	rv := r.(*api.JobInfo)
	if lv.CreationTimestamp.Equal(&rv.CreationTimestamp) {
		return lv.UID < rv.UID
	}
	return lv.CreationTimestamp.Before(&rv.CreationTimestamp)
}

// ClusterOrderFn invoke ClusterOrderFn function of the plugins
func (ssn *Session) ClusterOrderFn(l, r interface{}) bool {
	for _, tier := range ssn.Tiers {
		for _, plugin := range tier.Plugins {
			if !isEnabled(plugin.EnabledClusterOrder) {
				continue
			}
			cof, found := ssn.clusterOrderFns[plugin.Name]
			if !found {
				continue
			}
			if j := cof(l, r); j != 0 {
				return j < 0
			}
		}
	}

	// If no cluster order funcs, order cluster by ClusterID
	lv := l.(*scheduling.Cluster)
	rv := r.(*scheduling.Cluster)
	return lv.Name < rv.Name
}

// QueueOrderFn invoke queueorder function of the plugins
func (ssn *Session) QueueOrderFn(l, r interface{}) bool {
	for _, tier := range ssn.Tiers {
		for _, plugin := range tier.Plugins {
			if !isEnabled(plugin.EnabledQueueOrder) {
				continue
			}
			qof, found := ssn.queueOrderFns[plugin.Name]
			if !found {
				continue
			}
			if j := qof(l, r); j != 0 {
				return j < 0
			}
		}
	}

	// If no queue order funcs, order queue by CreationTimestamp first, then by UID.
	lv := l.(*api.QueueInfo)
	rv := r.(*api.QueueInfo)
	if lv.Queue.CreationTimestamp.Equal(&rv.Queue.CreationTimestamp) {
		return lv.UID < rv.UID
	}
	return lv.Queue.CreationTimestamp.Before(&rv.Queue.CreationTimestamp)
}

// VictimQueueOrderFn invoke victimqueueorder function of the plugins
func (ssn *Session) VictimQueueOrderFn(l, r, preemptor interface{}) bool {
	for _, tier := range ssn.Tiers {
		for _, plugin := range tier.Plugins {
			qof, found := ssn.victimQueueOrderFns[plugin.Name]
			if !found {
				continue
			}
			if j := qof(l, r, preemptor); j != 0 {
				return j < 0
			}
		}
	}

	return !ssn.QueueOrderFn(l, r)
}

// TaskCompareFns invoke taskorder function of the plugins
func (ssn *Session) TaskCompareFns(l, r interface{}) int {
	for _, tier := range ssn.Tiers {
		for _, plugin := range tier.Plugins {
			if !isEnabled(plugin.EnabledTaskOrder) {
				continue
			}
			tof, found := ssn.taskOrderFns[plugin.Name]
			if !found {
				continue
			}
			if j := tof(l, r); j != 0 {
				return j
			}
		}
	}

	return 0
}

// TaskOrderFn invoke taskorder function of the plugins
func (ssn *Session) TaskOrderFn(l, r interface{}) bool {
	if res := ssn.TaskCompareFns(l, r); res != 0 {
		return res < 0
	}

	// If no task order funcs, order task by default func.
	lv := l.(*api.TaskInfo)
	rv := r.(*api.TaskInfo)
	return helpers.CompareTask(lv, rv)
}

// QueueNodeFilterFn narrows candidate nodes before per-task predicates are evaluated,
// using queue-level constraints that remain valid throughout the session.
// Implementations must preserve input order and must not add nodes or modify the input.
func (ssn *Session) QueueNodeFilterFn(queue *api.QueueInfo, nodes []*api.NodeInfo) []*api.NodeInfo {
	for _, tier := range ssn.Tiers {
		for _, plugin := range tier.Plugins {
			enabled := plugin.EnabledQueueNodeFilter
			// Preserve existing predicate settings unless the filter is explicitly configured.
			if enabled == nil {
				enabled = plugin.EnabledPredicate
			}
			if !isEnabled(enabled) {
				continue
			}
			if fn, found := ssn.queueNodeFilterFns[plugin.Name]; found {
				nodes = fn(queue, nodes)
			}
		}
	}
	return nodes
}

// PredicateFn invoke predicate function of the plugins
func (ssn *Session) PredicateFn(task *api.TaskInfo, node *api.NodeInfo) error {
	for _, tier := range ssn.Tiers {
		for _, plugin := range tier.Plugins {
			if !isEnabled(plugin.EnabledPredicate) {
				continue
			}
			pfn, found := ssn.predicateFns[plugin.Name]
			if !found {
				continue
			}
			err := pfn(task, node)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// SimulateAllocatableFn invoke simulateAllocatableFn function of the plugins
func (ssn *Session) SimulateAllocatableFn(ctx context.Context, state fwk.CycleState, queue *api.QueueInfo, task *api.TaskInfo) bool {
	for _, tier := range ssn.Tiers {
		for _, plugin := range tier.Plugins {
			if !isEnabled(plugin.EnabledAllocatable) {
				continue
			}
			caf, found := ssn.simulateAllocatableFns[plugin.Name]
			if !found {
				continue
			}
			if !caf(ctx, state, queue, task) {
				return false
			}
		}
	}
	return true
}

// SimulatePredicateFn invoke simulatePredicateFn function of the plugins
func (ssn *Session) SimulatePredicateFn(ctx context.Context, state fwk.CycleState, task *api.TaskInfo, node *api.NodeInfo) error {
	for _, tier := range ssn.Tiers {
		for _, plugin := range tier.Plugins {
			if !isEnabled(plugin.EnabledPredicate) {
				continue
			}
			pfn, found := ssn.simulatePredicateFns[plugin.Name]
			if !found {
				continue
			}
			err := pfn(ctx, state, task, node)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// SimulateRemoveTaskFn invoke simulateRemoveTaskFn function of the plugins
func (ssn *Session) SimulateRemoveTaskFn(ctx context.Context, state fwk.CycleState, taskToSchedule *api.TaskInfo, taskToRemove *api.TaskInfo, nodeInfo *api.NodeInfo) error {
	for _, tier := range ssn.Tiers {
		for _, plugin := range tier.Plugins {
			if !isEnabled(plugin.EnabledPreemptable) && !isEnabled(plugin.EnabledAllocatable) {
				continue
			}
			pfn, found := ssn.simulateRemoveTaskFns[plugin.Name]
			if !found {
				continue
			}
			err := pfn(ctx, state, taskToSchedule, taskToRemove, nodeInfo)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// SimulateAddTaskFn invoke simulateAddTaskFn function of the plugins
func (ssn *Session) SimulateAddTaskFn(ctx context.Context, state fwk.CycleState, taskToSchedule *api.TaskInfo, taskToAdd *api.TaskInfo, nodeInfo *api.NodeInfo) error {
	for _, tier := range ssn.Tiers {
		for _, plugin := range tier.Plugins {
			if !isEnabled(plugin.EnabledPreemptable) && !isEnabled(plugin.EnabledAllocatable) {
				continue
			}
			pfn, found := ssn.simulateAddTaskFns[plugin.Name]
			if !found {
				continue
			}
			err := pfn(ctx, state, taskToSchedule, taskToAdd, nodeInfo)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// PrePredicateFn invoke predicate function of the plugins
func (ssn *Session) PrePredicateFn(task *api.TaskInfo) error {
	for _, tier := range ssn.Tiers {
		for _, plugin := range tier.Plugins {
			// we use same option as predicates for they are
			if !isEnabled(plugin.EnabledPredicate) {
				continue
			}
			pfn, found := ssn.prePredicateFns[plugin.Name]
			if !found {
				continue
			}
			err := pfn(task)
			if err == nil {
				continue
			}
			if ssn.unschedulableJobCacheEnabled {
				var rejection *api.PrePredicateError
				if errors.As(err, &rejection) {
					ssn.AddRejection(task.Job, rejection.Plugin, unschedulable.RejectionPredicate, task.UID)
				}
			}
			return err
		}
	}
	return nil
}

// BestNodeFn invoke bestNode function of the plugins
func (ssn *Session) BestNodeFn(task *api.TaskInfo, nodeScores map[float64][]*api.NodeInfo) *api.NodeInfo {
	for _, tier := range ssn.Tiers {
		for _, plugin := range tier.Plugins {
			if !isEnabled(plugin.EnabledBestNode) {
				continue
			}
			pfn, found := ssn.bestNodeFns[plugin.Name]
			if !found {
				continue
			}
			// Only the first plugin that enables and realizes bestNodeFn is allowed to choose best node for task
			if bestNode := pfn(task, nodeScores); bestNode != nil {
				return bestNode
			}
		}
	}
	return nil
}

// NodeOrderFn invoke node order function of the plugins
func (ssn *Session) NodeOrderFn(task *api.TaskInfo, node *api.NodeInfo) (float64, error) {
	priorityScore := 0.0
	for _, tier := range ssn.Tiers {
		for _, plugin := range tier.Plugins {
			if !isEnabled(plugin.EnabledNodeOrder) {
				continue
			}
			pfn, found := ssn.nodeOrderFns[plugin.Name]
			if !found {
				continue
			}
			score, err := pfn(task, node)
			if err != nil {
				return 0, err
			}
			priorityScore += score
		}
	}
	return priorityScore, nil
}

// BatchNodeOrderFn invoke node order function of the plugins
func (ssn *Session) BatchNodeOrderFn(task *api.TaskInfo, nodes []*api.NodeInfo) (map[string]float64, error) {
	priorityScore := make(map[string]float64, len(nodes))
	for _, tier := range ssn.Tiers {
		for _, plugin := range tier.Plugins {
			if !isEnabled(plugin.EnabledNodeOrder) {
				continue
			}
			pfn, found := ssn.batchNodeOrderFns[plugin.Name]
			if !found {
				continue
			}
			score, err := pfn(task, nodes)
			if err != nil {
				return nil, err
			}
			for nodeName, score := range score {
				priorityScore[nodeName] += score
			}
		}
	}
	return priorityScore, nil
}

func isEnabled(enabled *bool) bool {
	return enabled != nil && *enabled
}

// NodeOrderMapFn invoke node order function of the plugins
func (ssn *Session) NodeOrderMapFn(task *api.TaskInfo, node *api.NodeInfo) (map[string]float64, float64, error) {
	nodeScoreMap := map[string]float64{}
	var priorityScore float64
	for _, tier := range ssn.Tiers {
		for _, plugin := range tier.Plugins {
			if !isEnabled(plugin.EnabledNodeOrder) {
				continue
			}
			if pfn, found := ssn.nodeOrderFns[plugin.Name]; found {
				score, err := pfn(task, node)
				if err != nil {
					return nodeScoreMap, priorityScore, err
				}
				priorityScore += score
			}
			if pfn, found := ssn.nodeMapFns[plugin.Name]; found {
				score, err := pfn(task, node)
				if err != nil {
					return nodeScoreMap, priorityScore, err
				}
				nodeScoreMap[plugin.Name] = score
			}
		}
	}
	return nodeScoreMap, priorityScore, nil
}

// HyperNodeOrderMapFn invoke hyperNode order function of the plugins
func (ssn *Session) HyperNodeOrderMapFn(subJob *api.SubJobInfo, hyperNodes map[string][]*api.NodeInfo) (map[string]map[string]float64, error) {
	nodeGroupScore := make(map[string]map[string]float64)
	for _, tier := range ssn.Tiers {
		for _, plugin := range tier.Plugins {
			if !isEnabled(plugin.EnabledHyperNodeOrder) {
				continue
			}
			pfn, found := ssn.hyperNodeOrderFns[plugin.Name]
			if !found {
				continue
			}
			scoreTmp, err := pfn(subJob, hyperNodes)
			if err != nil {
				return nodeGroupScore, err
			}

			nodeGroupScore[plugin.Name] = scoreTmp
		}
	}
	return nodeGroupScore, nil
}

// NodeOrderReduceFn invoke node order function of the plugins
func (ssn *Session) NodeOrderReduceFn(task *api.TaskInfo, pluginNodeScoreMap map[string]fwk.NodeScoreList) (map[string]float64, error) {
	nodeScoreMap := map[string]float64{}
	for _, tier := range ssn.Tiers {
		for _, plugin := range tier.Plugins {
			if !isEnabled(plugin.EnabledNodeOrder) {
				continue
			}
			pfn, found := ssn.nodeReduceFns[plugin.Name]
			if !found {
				continue
			}
			if err := pfn(task, pluginNodeScoreMap[plugin.Name]); err != nil {
				return nodeScoreMap, err
			}
			for _, hp := range pluginNodeScoreMap[plugin.Name] {
				nodeScoreMap[hp.Name] += float64(hp.Score)
			}
		}
	}
	return nodeScoreMap, nil
}

// HyperNodeGradientForJobFn intersects gradients from every enabled registered plugin.
func (ssn *Session) HyperNodeGradientForJobFn(job *api.JobInfo, hyperNode *api.HyperNodeInfo, purpose api.SearchPurpose) ([][]*api.HyperNodeInfo, *api.HyperNodeGradientStats) {
	var gradientByPlugin []api.HyperNodePluginGradient
	for _, tier := range ssn.Tiers {
		for _, plugin := range tier.Plugins {
			if !isEnabled(plugin.EnabledHyperNodeGradient) {
				continue
			}
			fn, found := ssn.hyperNodeGradientForJobFns[plugin.Name]
			if !found {
				continue
			}
			result := fn(job, hyperNode, purpose)
			gradientByPlugin = append(gradientByPlugin, api.HyperNodePluginGradient{
				PluginName: plugin.Name, Unconstrained: result.Unconstrained, Gradients: result.Gradients,
			})
		}
	}
	return ssn.resolveHyperNodeGradients(gradientByPlugin, hyperNode, purpose)
}

// HyperNodeGradientForSubJobFn intersects gradients from every enabled registered plugin.
func (ssn *Session) HyperNodeGradientForSubJobFn(subJob *api.SubJobInfo, hyperNode *api.HyperNodeInfo, purpose api.SearchPurpose) ([][]*api.HyperNodeInfo, *api.HyperNodeGradientStats) {
	var gradientByPlugin []api.HyperNodePluginGradient
	for _, tier := range ssn.Tiers {
		for _, plugin := range tier.Plugins {
			if !isEnabled(plugin.EnabledHyperNodeGradient) {
				continue
			}
			fn, found := ssn.hyperNodeGradientForSubJobFns[plugin.Name]
			if !found {
				continue
			}
			result := fn(subJob, hyperNode, purpose)
			gradientByPlugin = append(gradientByPlugin, api.HyperNodePluginGradient{
				PluginName: plugin.Name, Unconstrained: result.Unconstrained, Gradients: result.Gradients,
			})
		}
	}
	return ssn.resolveHyperNodeGradients(gradientByPlugin, hyperNode, purpose)
}

// resolveHyperNodeGradients distinguishes absent callbacks, unconstrained results,
// and constrained results (including rejection). Only constrained plugins
// participate in intersection and exclusion diagnostics.
func (ssn *Session) resolveHyperNodeGradients(gradientByPlugin []api.HyperNodePluginGradient, root *api.HyperNodeInfo, purpose api.SearchPurpose) ([][]*api.HyperNodeInfo, *api.HyperNodeGradientStats) {
	if len(gradientByPlugin) == 0 {
		return [][]*api.HyperNodeInfo{{root}}, nil
	}
	constrained := gradientByPlugin[:0]
	for _, result := range gradientByPlugin {
		if result.Unconstrained {
			if len(result.Gradients) == 0 {
				continue
			}
			klog.ErrorS(nil, "Unconstrained HyperNode gradient contains candidates; treating as rejected", "plugin", result.PluginName, "purpose", purpose)
			result.Unconstrained = false
			result.Gradients = nil
		}
		constrained = append(constrained, result)
	}
	if len(constrained) > 0 {
		return intersectHyperNodeGradients(constrained, purpose)
	}

	// All callbacks permit the input subtree. Expand it once so preferred rules
	// can still score candidates even when no plugin supplies a concrete gradient.
	hyperNodeByName := make(map[string]*api.HyperNodeInfo)
	eligible := sets.New[string]()
	queue := []*api.HyperNodeInfo{root}
	for len(queue) > 0 {
		hn := queue[0]
		queue = queue[1:]
		if hn == nil || eligible.Has(hn.Name) {
			continue
		}
		eligible.Insert(hn.Name)
		hyperNodeByName[hn.Name] = hn
		for child := range hn.Children {
			if childHN := ssn.HyperNodes[child]; childHN != nil {
				queue = append(queue, childHN)
			}
		}
	}
	gradients := rebuildGradientsByTier(hyperNodeByName, eligible, purpose)
	return gradients, &api.HyperNodeGradientStats{
		PluginEligibleByTier: map[string]map[int]int{},
		IntersectedByTier:    hyperNodeCountByTier(gradients),
	}
}

func hyperNodeCountByTier(gradients [][]*api.HyperNodeInfo) map[int]int {
	counts := make(map[int]int)
	for _, layer := range gradients {
		for _, hn := range layer {
			if hn != nil {
				counts[hn.Tier()]++
			}
		}
	}
	return counts
}

func intersectHyperNodeGradients(gradientByPlugin []api.HyperNodePluginGradient, purpose api.SearchPurpose) ([][]*api.HyperNodeInfo, *api.HyperNodeGradientStats) {
	stats := &api.HyperNodeGradientStats{PluginEligibleByTier: make(map[string]map[int]int, len(gradientByPlugin))}
	for _, pluginGradient := range gradientByPlugin {
		stats.PluginEligibleByTier[pluginGradient.PluginName] = hyperNodeCountByTier(pluginGradient.Gradients)
	}

	eligible := api.HyperNodeNamesInGradients(gradientByPlugin[0].Gradients)
	for i := 1; i < len(gradientByPlugin); i++ {
		eligible = eligible.Intersection(api.HyperNodeNamesInGradients(gradientByPlugin[i].Gradients))
	}
	stats.ExcludedByReason = api.ComputePluginExcludedHyperNodes(gradientByPlugin, eligible)
	if eligible.Len() == 0 {
		stats.IntersectedByTier = map[int]int{}
		return [][]*api.HyperNodeInfo{}, stats
	}

	hyperNodeByName := make(map[string]*api.HyperNodeInfo, eligible.Len())
	for _, layer := range gradientByPlugin[0].Gradients {
		for _, hn := range layer {
			if hn != nil && eligible.Has(hn.Name) {
				hyperNodeByName[hn.Name] = hn
			}
		}
	}
	result := rebuildGradientsByTier(hyperNodeByName, eligible, purpose)
	stats.IntersectedByTier = hyperNodeCountByTier(result)
	return result, stats
}

func rebuildGradientsByTier(hyperNodeByName map[string]*api.HyperNodeInfo, eligible sets.Set[string], purpose api.SearchPurpose) [][]*api.HyperNodeInfo {
	byTier := make(map[int][]*api.HyperNodeInfo)
	for name := range eligible {
		if hn := hyperNodeByName[name]; hn != nil {
			byTier[hn.Tier()] = append(byTier[hn.Tier()], hn)
		}
	}
	tiers := make([]int, 0, len(byTier))
	for tier := range byTier {
		tiers = append(tiers, tier)
		sort.Slice(byTier[tier], func(i, j int) bool { return byTier[tier][i].Name < byTier[tier][j].Name })
	}
	sort.Ints(tiers)
	if purpose == api.PurposeEvict {
		sort.Sort(sort.Reverse(sort.IntSlice(tiers)))
	}
	result := make([][]*api.HyperNodeInfo, 0, len(tiers))
	for _, tier := range tiers {
		result = append(result, byTier[tier])
	}
	return result
}

// BuildVictimsPriorityQueue returns a priority queue with victims sorted by:
//  1. If victims belong to the same job, use !ssn.TaskOrderFn.
//  2. If either victim's job is missing, evict orphaned tasks first; if both
//     are orphaned, use !ssn.TaskOrderFn.
//  3. If the preemptor job is missing or victims are in the same queue, compare
//     jobs with JobOrderCompareFn and use !ssn.TaskOrderFn as a tie-break.
//  4. If victims are in different queues and preemptor job exists, use
//     ssn.VictimQueueOrderFn.
func (ssn *Session) BuildVictimsPriorityQueue(victims []*api.TaskInfo, preemptor *api.TaskInfo) *util.PriorityQueue {
	jobThenTaskOrder := func(lvJob, rvJob *api.JobInfo, l, r interface{}) bool {
		if cmp := ssn.JobOrderCompareFn(lvJob, rvJob); cmp != 0 {
			return cmp > 0
		}
		return !ssn.TaskOrderFn(l, r)
	}

	victimsQueue := util.NewPriorityQueue(func(l, r interface{}) bool {
		lv := l.(*api.TaskInfo)
		rv := r.(*api.TaskInfo)
		if lv.Job == rv.Job {
			return !ssn.TaskOrderFn(l, r)
		}

		lvJob, lvJobFound := ssn.Jobs[lv.Job]
		rvJob, rvJobFound := ssn.Jobs[rv.Job]
		preemptorJob, preemptorJobFound := ssn.Jobs[preemptor.Job]

		// Handle orphaned tasks whose PodGroups were deleted after the session
		// snapshot was taken but before BuildVictimsPriorityQueue was called.
		//
		// Race condition scenario:
		// 1. Session captures a snapshot of jobs/tasks at scheduling cycle start
		// 2. PodGroup gets deleted concurrently, removing the job from ssn.Jobs
		// 3. Here, ssn.Jobs[task.Job] returns nil for the orphaned task
		//
		// Sorting priority:
		// - Both jobs missing: compare using TaskOrderFn (creation timestamp)
		// - One job missing: evict orphaned task first (its PodGroup is gone anyway)
		// - Both jobs present: use normal JobOrderFn comparison below
		if !lvJobFound || !rvJobFound {
			if !lvJobFound && !rvJobFound {
				return !ssn.TaskOrderFn(l, r)
			}
			return !lvJobFound
		}

		if !preemptorJobFound {
			return jobThenTaskOrder(lvJob, rvJob, l, r)
		}

		if lvJob.Queue != rvJob.Queue {
			return ssn.VictimQueueOrderFn(ssn.Queues[lvJob.Queue], ssn.Queues[rvJob.Queue], ssn.Queues[preemptorJob.Queue])
		}

		return jobThenTaskOrder(lvJob, rvJob, l, r)
	})
	for _, victim := range victims {
		victimsQueue.Push(victim)
	}
	return victimsQueue
}

// RegisterBinder registers the passed binder to the cache, the binder type can be such as pre-binder, post-binder
func (ssn *Session) RegisterBinder(name string, binder interface{}) {
	ssn.cache.RegisterBinder(name, binder)
}

// AddHintProvider registers events that may make Jobs rejected by a plugin schedulable.
func (ssn *Session) AddHintProvider(pluginName string, p unschedulable.HintProvider) {
	if !ssn.unschedulableJobCacheEnabled {
		return
	}
	ssn.unschedulableJobCache.AddHintProvider(pluginName, p)
}

// applyCachedSkips derives each pending Job's Skip decision from the
// unschedulable-job cache at OpenSession. Jobs without a cached record keep the
// zero-value Skip and are evaluated normally.
func (ssn *Session) applyCachedSkips() {
	if !ssn.unschedulableJobCacheEnabled {
		return
	}
	for _, job := range ssn.Jobs {
		rejections := ssn.unschedulableJobCache.CachedRejections(job)
		if len(rejections) == 0 {
			continue
		}
		job.Skip = unschedulable.ComputeSkip(job, rejections)
		klog.V(4).Infof("Job %s cached skip: enqueue=%v allocate=%v tasks=%d",
			job.UID, job.Skip.Enqueue, job.Skip.Allocate, len(job.Skip.Tasks))
	}
}

// reconcileUnschedulableCache updates the unschedulable-job cache at CloseSession
// to match what actually happened to each Job this session.
func (ssn *Session) reconcileUnschedulableCache() {
	if !ssn.unschedulableJobCacheEnabled {
		return
	}
	for jobID, job := range ssn.Jobs {
		// A pipelined Job must remain visible to allocate/preempt in the next
		// session. Do not cache even if another task was freshly rejected.
		if len(job.TaskStatusIndex[api.Pipelined]) > 0 {
			ssn.unschedulableJobCache.Forget(jobID)
			continue
		}

		// Cache fresh rejections even when an elastic Job is already ready. Its
		// rejected surplus tasks are still redundant work, while ComputeSkip
		// keeps the Job eligible to schedule every other pending task.
		if rejections := ssn.rejectionsForJob(jobID); len(rejections) > 0 {
			scope := ssn.queueScope(job.Queue)
			for i := range rejections {
				rejections[i].Queues = scope
			}
			ssn.unschedulableJobCache.Record(job, rejections)
			continue
		}

		// No fresh rejections: if the Job was skipped this session, leave its
		// existing record untouched. This includes ready elastic Jobs whose
		// surplus rejected tasks were intentionally skipped.
		if job.Skip.Skipped() {
			continue
		}

		ssn.unschedulableJobCache.Forget(jobID)
	}
}

// queueScope returns queueID together with its ancestor queues, walking
// Queue.Spec.Parent through ssn.Queues. Quota-plugin hints use the result to
// scope Queue-local changes. The walk stops at the root, a missing parent, or a
// cycle.
func (ssn *Session) queueScope(queueID api.QueueID) []api.QueueID {
	scope := []api.QueueID{queueID}
	seen := map[api.QueueID]struct{}{queueID: {}}
	current := queueID
	for {
		qi, ok := ssn.Queues[current]
		if !ok || qi.Queue == nil || qi.Queue.Spec.Parent == "" {
			break
		}
		parent := api.QueueID(qi.Queue.Spec.Parent)
		if _, dup := seen[parent]; dup {
			break
		}
		seen[parent] = struct{}{}
		scope = append(scope, parent)
		current = parent
	}
	return scope
}
