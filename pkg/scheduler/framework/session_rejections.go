/*
Copyright 2026 The Volcano Authors.

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
	"sort"

	"k8s.io/apimachinery/pkg/util/sets"

	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/unschedulable"
)

// rejectionKey 唯一标识一次调度拒绝（Rejection）的来源，由插件名和扩展点（Extension Point）组成。
// 相同的 key 会聚合失败的 Task 列表，避免产生重复记录。
type rejectionKey struct {
	plugin string
	source unschedulable.RejectionSource
}

// rejectionAggregate 保存对应 rejectionKey 所记录的 Task 集合与调度提示键（HintKeys）。
type rejectionAggregate struct {
	tasks    sets.Set[api.TaskID]
	hintKeys sets.Set[unschedulable.HintKey] // nil 表示粗粒度回退（coarse fallback）
}

// jobRejectionScope 表示在评估某个 Job 或 SubJob 候选方案时的临时拒绝作用域。
// 因为不可调度缓存是在 Job 级别记录的，SubJob 的拒绝也会归属于所属 Job ID。
type jobRejectionScope struct {
	jobID      api.JobID
	rejections map[rejectionKey]*rejectionAggregate
}

// jobRejectionTracker 负责跟踪管理调度拒绝信息，将评估候选方案时的临时拒绝与当前 Session 最终确认的拒绝隔离。
type jobRejectionTracker struct {
	// recorded 包含已确认的拒绝记录，在 Session 关闭时将同步写回不可调度 Job 缓存
	recorded map[api.JobID]map[rejectionKey]*rejectionAggregate
	// evaluationScopes 评估作用域栈，支持 Job 及其内部嵌套 SubJob 候选方案的层级评估
	evaluationScopes []jobRejectionScope
}

// RejectionTrackingEnabled 检查当前 Session 是否启用了不可调度 Job 拒绝原因跟踪功能。
func (ssn *Session) RejectionTrackingEnabled() bool {
	return ssn.unschedulableJobCacheEnabled
}

// AddRejection 记录当前会话中某个插件通过特定扩展点使 Job 无法调度，并可指定失败的 Task 列表。
// 这些拒绝信息在 CloseSession 时会被持久化到 unschedulableJobCache 中。
func (ssn *Session) AddRejection(jobID api.JobID, plugin string, source unschedulable.RejectionSource, tasks ...api.TaskID) {
	ssn.AddRejectionWithKeys(jobID, plugin, source, nil, tasks...)
}

// AddRejectionWithKeys 记录当前会话中某个插件使 Job 无法调度，除了关联失败的任务外，还携带可用的调度提示键（HintKeys）。
// HintKeys 用于后续在集群发生特定事件（如某节点 Pod 退出释放资源）时，精确唤醒被该事件影响的不可调度作业。
func (ssn *Session) AddRejectionWithKeys(jobID api.JobID, plugin string, source unschedulable.RejectionSource, hintKeys []unschedulable.HintKey, tasks ...api.TaskID) {
	if !ssn.unschedulableJobCacheEnabled {
		return
	}
	ssn.jobRejections.record(jobID, plugin, source, hintKeys, tasks...)
}

// record 将拒绝信息添加到当前处于活跃状态的评估作用域中；若当前没有活跃作用域，则直接加入 Session 最终确认的拒绝集合。
func (tracker *jobRejectionTracker) record(jobID api.JobID, plugin string, source unschedulable.RejectionSource, hintKeys []unschedulable.HintKey, tasks ...api.TaskID) {
	rejectionsByKey := tracker.rejectionsForWrite(jobID)
	key := rejectionKey{plugin: plugin, source: source}
	aggregate, ok := rejectionsByKey[key]
	if !ok {
		aggregate = &rejectionAggregate{tasks: sets.New[api.TaskID]()}
		rejectionsByKey[key] = aggregate
	}
	aggregate.tasks.Insert(tasks...)

	if !ok {
		if len(hintKeys) == 0 {
			return
		}
		aggregate.hintKeys = sets.New[unschedulable.HintKey](hintKeys...)
		if aggregate.hintKeys.Len() > unschedulable.MaxHintKeysPerPluginEvent {
			aggregate.hintKeys = nil
		}
		return
	}

	if aggregate.hintKeys == nil || len(hintKeys) == 0 {
		aggregate.hintKeys = nil
		return
	}
	aggregate.hintKeys.Insert(hintKeys...)
	if aggregate.hintKeys.Len() > unschedulable.MaxHintKeysPerPluginEvent {
		aggregate.hintKeys = nil
	}
}

// rejectionsForWrite 获取写入目标：优先返回最内层匹配 jobID 的评估作用域；若不在任何临时作用域中，则返回 Session 级的确认集合。
func (tracker *jobRejectionTracker) rejectionsForWrite(jobID api.JobID) map[rejectionKey]*rejectionAggregate {
	// Search from the innermost evaluation outwards. When Job and SubJob
	// evaluations are nested, a rejection belongs to the currently active
	// SubJob candidate first.
	for i := len(tracker.evaluationScopes) - 1; i >= 0; i-- {
		scope := &tracker.evaluationScopes[i]
		if scope.jobID != jobID {
			continue
		}
		if scope.rejections == nil {
			scope.rejections = make(map[rejectionKey]*rejectionAggregate)
		}
		return scope.rejections
	}

	// Without an active candidate evaluation, the caller has confirmed the
	// rejection and it belongs to the Session result.
	if tracker.recorded == nil {
		tracker.recorded = make(map[api.JobID]map[rejectionKey]*rejectionAggregate)
	}
	rejectionsByKey := tracker.recorded[jobID]
	if rejectionsByKey == nil {
		rejectionsByKey = make(map[rejectionKey]*rejectionAggregate)
		tracker.recorded[jobID] = rejectionsByKey
	}
	return rejectionsByKey
}

// rejectionsForJob 返回当前会话为指定作业累积的所有已确认拒绝原因。
func (ssn *Session) rejectionsForJob(jobID api.JobID) []unschedulable.Rejection {
	return ssn.jobRejections.list(jobID)
}

// list 返回 jobID 已确认记录的拒绝切片。
func (tracker *jobRejectionTracker) list(jobID api.JobID) []unschedulable.Rejection {
	return buildRejections(tracker.recorded[jobID])
}

// buildRejections 将聚合的拒绝映射结构转换并排序为稳定的 unschedulable.Rejection 列表。
func buildRejections(rejectionsByKey map[rejectionKey]*rejectionAggregate) []unschedulable.Rejection {
	if len(rejectionsByKey) == 0 {
		return nil
	}
	rejections := make([]unschedulable.Rejection, 0, len(rejectionsByKey))
	for key, aggregate := range rejectionsByKey {
		var taskIDs []api.TaskID
		if aggregate.tasks.Len() > 0 {
			taskIDs = sets.List(aggregate.tasks)
			sort.Slice(taskIDs, func(i, j int) bool { return taskIDs[i] < taskIDs[j] })
		}
		var hintKeys []unschedulable.HintKey
		if aggregate.hintKeys != nil {
			hintKeys = sets.List(aggregate.hintKeys)
			sort.Slice(hintKeys, func(i, j int) bool { return hintKeys[i] < hintKeys[j] })
		}
		rejections = append(rejections, unschedulable.Rejection{
			Plugin:   key.plugin,
			Source:   key.source,
			Tasks:    taskIDs,
			HintKeys: hintKeys,
		})
	}
	sort.Slice(rejections, func(i, j int) bool {
		if rejections[i].Plugin != rejections[j].Plugin {
			return rejections[i].Plugin < rejections[j].Plugin
		}
		return rejections[i].Source < rejections[j].Source
	})
	return rejections
}

// collect 在新的临时作用域中执行 evaluate 闭包，并返回该次评估期间产生的全部拒绝原因（执行完后弹出恢复作用域栈）。
func (tracker *jobRejectionTracker) collect(jobID api.JobID, evaluate func()) []unschedulable.Rejection {
	// Remember the previous stack length so the deferred restore removes only
	// the scope created by this call and any nested scopes.
	scopeIndex := len(tracker.evaluationScopes)
	tracker.evaluationScopes = append(tracker.evaluationScopes, jobRejectionScope{jobID: jobID})
	defer tracker.restoreScopes(scopeIndex)

	evaluate()
	// The return value is built before the deferred restore removes this scope.
	return buildRejections(tracker.evaluationScopes[scopeIndex].rejections)
}

// restoreScopes 将评估栈恢复至指定长度。
func (tracker *jobRejectionTracker) restoreScopes(scopeIndex int) {
	tracker.evaluationScopes = tracker.evaluationScopes[:scopeIndex]
}

// CollectJobRejections 在临时隔离的作用域内执行 evaluate，仅收集并返回该闭包内产生的拒绝原因。
// 调度器在尝试试探多个候选拓扑或节点时，可用此方法收集试探过程中的失败原因，若试探成功则丢弃，试探失败则由调用方选择性确认。
func (ssn *Session) CollectJobRejections(jobID api.JobID, evaluate func()) []unschedulable.Rejection {
	if !ssn.unschedulableJobCacheEnabled {
		evaluate()
		return nil
	}

	return ssn.jobRejections.collect(jobID, evaluate)
}
