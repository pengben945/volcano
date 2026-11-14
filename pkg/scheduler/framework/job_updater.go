/*
Copyright 2025 The Volcano Authors.

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
	"math/rand"
	"time"

	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"

	"volcano.sh/apis/pkg/apis/scheduling"
	"volcano.sh/volcano/cmd/scheduler/app/options"
	"volcano.sh/volcano/pkg/scheduler/api"
)

// TimeJitterAfter 判断 new 时间是否晚于 (old + duration + 随机抖动时间 jitter)。
// 用于状态刷新节流或防止大量任务在同一瞬间并发重试引起的惊群效应。
func TimeJitterAfter(new, old time.Time, duration, maxJitter time.Duration) bool {
	var jitter int64
	if maxJitter > 0 {
		jitter = rand.Int63n(int64(maxJitter))
	}
	return new.After(old.Add(duration + time.Duration(jitter)))
}

// JobUpdater 用于在调度周期结束时，多 worker 并发将内存中作业（Job）和 PodGroup 的最新状态同步更新到底层缓存和持久化存储。
type JobUpdater struct {
	ssn      *Session
	jobQueue []*api.JobInfo
}

// NewJobUpdater 构造 JobUpdater，提取当前 Session 中的全部作业列表准备批量更新。
func NewJobUpdater(ssn *Session) *JobUpdater {
	queue := make([]*api.JobInfo, 0, len(ssn.Jobs))
	for _, job := range ssn.Jobs {
		queue = append(queue, job)
	}

	ju := &JobUpdater{
		ssn:      ssn,
		jobQueue: queue,
	}
	return ju
}

// UpdateAll 并行执行所有 Job 的状态写回操作，工作协程数由 options.GetJobUpdaterWorkerNum() 决定。
func (ju *JobUpdater) UpdateAll() {
	workqueue.ParallelizeUntil(context.TODO(), options.GetJobUpdaterWorkerNum(), len(ju.jobQueue), ju.updateJob)
}

// isPodGroupConditionsUpdated 检查新旧 PodGroupCondition 列表是否发生实质性变化。
// 比较时会忽略 LastTransitionTime 和 TransitionID，仅当状态、原因或消息有变化时才判定为已更新。
func isPodGroupConditionsUpdated(newCondition, oldCondition []scheduling.PodGroupCondition) bool {
	if len(newCondition) != len(oldCondition) {
		return true
	}

	for index, newCond := range newCondition {
		oldCond := oldCondition[index]

		newTime := newCond.LastTransitionTime
		oldTime := oldCond.LastTransitionTime

		// if newCond is not new enough, we treat it the same as the old one
		newCond.LastTransitionTime = oldTime

		// comparing should ignore the TransitionID
		newTransitionID := newCond.TransitionID
		newCond.TransitionID = oldCond.TransitionID

		shouldUpdate := !equality.Semantic.DeepEqual(&newCond, &oldCond)

		newCond.LastTransitionTime = newTime
		newCond.TransitionID = newTransitionID
		if shouldUpdate {
			return true
		}
	}

	return false
}

// isPodGroupStatusUpdated 检查 PodGroup 的 Status（除去 Condition 的主体状态以及 Conditions 列表）是否发生变化。
func isPodGroupStatusUpdated(newStatus, oldStatus scheduling.PodGroupStatus) bool {
	newCondition := newStatus.Conditions
	newStatus.Conditions = nil
	oldCondition := oldStatus.Conditions
	oldStatus.Conditions = nil

	return !equality.Semantic.DeepEqual(newStatus, oldStatus) || isPodGroupConditionsUpdated(newCondition, oldCondition)
}

// isJobAllocatedHyperNodeChanged 检查 Job 分配的 HyperNode 注解是否发生了变更。
func (ju *JobUpdater) isJobAllocatedHyperNodeChanged(job *api.JobInfo) bool {
	oldHyperNode := ju.ssn.PodGroupOldState.Annotations[job.UID][api.JobAllocatedHyperNode]
	return oldHyperNode != job.PodGroup.GetAnnotations()[api.JobAllocatedHyperNode]
}

// syncAllocatedHyperNodeAnnotation 将内存中计算出的作业拓扑分配位置（AllocatedHyperNode）
// 同步序列化写入到 PodGroup 的 Annotations 中，以保证跨调度周期的持久化。
func syncAllocatedHyperNodeAnnotation(job *api.JobInfo) {
	if job.PodGroup == nil {
		return
	}
	annotations := job.PodGroup.GetAnnotations()
	current := ""
	if annotations != nil {
		current = annotations[api.JobAllocatedHyperNode]
	}
	if current == job.AllocatedHyperNode {
		return
	}
	if job.AllocatedHyperNode == "" {
		if annotations != nil {
			delete(annotations, api.JobAllocatedHyperNode)
		}
		return
	}
	if annotations == nil {
		annotations = map[string]string{}
		job.PodGroup.SetAnnotations(annotations)
	}
	annotations[api.JobAllocatedHyperNode] = job.AllocatedHyperNode
}

// updateJob 执行单个 Job 的状态比对与持久化更新操作。
func (ju *JobUpdater) updateJob(index int) {
	job := ju.jobQueue[index]
	ssn := ju.ssn

	// 重新计算并赋予 PodGroup 最新的 Status（Phase、Running/Failed 任务计数等）
	job.PodGroup.Status = jobStatus(ssn, job)
	syncAllocatedHyperNodeAnnotation(job)
	oldStatus, found := ssn.PodGroupOldState.Status[job.UID]
	updatePGStatus := !found || isPodGroupStatusUpdated(job.PodGroup.Status, oldStatus)
	updatePGAnnotations := ju.isJobAllocatedHyperNodeChanged(job)
	updateJobInfo := ssn.DirtyJobs.Has(job.UID)
	// 仅在状态、注解或 Job 自身发生变更（Dirty）时才向底层 Cache 触发实际写回
	if _, err := ssn.cache.UpdateJobStatus(job, updatePGStatus, updatePGAnnotations, updateJobInfo); err != nil {
		klog.Errorf("Failed to update job <%s/%s>: %v",
			job.Namespace, job.Name, err)
	}
}
