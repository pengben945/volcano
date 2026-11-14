/*
Copyright 2018 The Kubernetes Authors.
Copyright 2018-2025 The Volcano Authors.

Modifications made by Volcano authors:
- Added comprehensive operation management with save/recover capabilities
- Enhanced with Allocate/UnAllocate and UnPipeline operations
- Added improved error handling and rollback support

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
	"errors"
	"fmt"

	"k8s.io/klog/v2"

	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/metrics"
)

// Operation 表示在调度事务中执行的具体操作类型。
type Operation int8

const (
	// Evict 驱逐操作：标记一个正在运行的 Task 进行驱逐释放节点资源
	Evict = iota
	// Pipeline 流水线/预留操作：在目标节点上有 Task 正在释放（Releasing）时，预先占位等待资源释放
	Pipeline
	// Allocate 分配操作：将 Task 分配到有充足空闲资源的节点上
	Allocate
)

// operation 内部结构，封装具体操作类型、涉及的任务对象以及操作原因。
type operation struct {
	name   Operation
	task   *api.TaskInfo
	reason string
}

// Statement 是 Volcano 调度框架的核心事务抽象。
// 它提供类似数据库事务（ACID 中的 A 和 C）的原子化操作机制：
// 在进行资源试探、抢占或 Gang 批量调度时，所做的 Evict/Pipeline/Allocate 操作先在 Session 内存快照中生效；
// - 如果后续整个 Gang 或调度决策成功，调用 Commit() 将操作真正提交到底层 cache（发起异步绑定或驱逐 API 调用）；
// - 如果中途校验失败（如成团失败），调用 Discard() 逆序回滚所有内存状态变更，恢复如初。
type Statement struct {
	operations []operation
	ssn        *Session
}

// NewStatement 为给定的调度会话创建一个新的事务对象。
func NewStatement(ssn *Session) *Statement {
	return &Statement{
		ssn: ssn,
	}
}

// Operations 返回当前事务内累积的所有操作列表。
func (s *Statement) Operations() []operation {
	return s.operations
}

// HasEvictions 检查当前事务是否包含至少一个驱逐（Evict）操作。
func (s *Statement) HasEvictions() bool {
	for _, op := range s.operations {
		if op.name == Evict {
			return true
		}
	}
	return false
}

// Evict 在事务中执行任务驱逐：
// 1. 将 Job 内该任务的状态置为 Releasing；
// 2. 更新所属 Node 上的任务状态；
// 3. 触发注册的 DeallocateFunc 事件处理器；
// 4. 将驱逐操作记录进事务操作日志。
func (s *Statement) Evict(reclaimee *api.TaskInfo, reason string) {
	// Update status in session
	if job, found := s.ssn.Jobs[reclaimee.Job]; found {
		job.UpdateTaskStatus(reclaimee, api.Releasing)
	} else {
		klog.Errorf("Failed to find Job <%s> in Session <%s> index when evicting.",
			reclaimee.Job, s.ssn.UID)
	}

	// Update task in node.
	if node, found := s.ssn.Nodes[reclaimee.NodeName]; found {
		node.UpdateTask(reclaimee)
	}

	for _, eh := range s.ssn.eventHandlers {
		if eh.DeallocateFunc != nil {
			eventInfo := &Event{
				Task: reclaimee,
			}
			eh.DeallocateFunc(eventInfo)
		}
	}

	s.operations = append(s.operations, operation{
		name:   Evict,
		task:   reclaimee,
		reason: reason,
	})
}

// evict 事务提交时调用：向底层 cache 发送实际驱逐请求；若失败则尝试恢复内存状态。
func (s *Statement) evict(reclaimee *api.TaskInfo, reason string) error {
	if err := s.ssn.cache.Evict(reclaimee, reason); err != nil {
		if e := s.unevict(reclaimee); e != nil {
			klog.Errorf("Faled to unevict task <%v/%v>: %v.", reclaimee.Namespace, reclaimee.Name, e)
		}
		return err
	}

	return nil
}

// unevict 驱逐回滚：将任务状态恢复为 Running，恢复 Node 占用，并以 Restore=true 触发 Allocate 回调。
func (s *Statement) unevict(reclaimee *api.TaskInfo) error {
	// Update status in session
	job, found := s.ssn.Jobs[reclaimee.Job]
	if found {
		job.UpdateTaskStatus(reclaimee, api.Running)
	} else {
		klog.Errorf("Failed to find Job <%s> in Session <%s> index when unevicting.",
			reclaimee.Job, s.ssn.UID)
	}

	// Update task in node.
	if node, found := s.ssn.Nodes[reclaimee.NodeName]; found {
		node.UpdateTask(reclaimee)
	}

	for _, eh := range s.ssn.eventHandlers {
		if eh.AllocateFunc != nil {
			eh.AllocateFunc(&Event{
				Task:    reclaimee,
				Restore: true,
			})
		}
	}

	return nil
}

// Pipeline 在事务中执行流水线预留操作：
// 当抢占或回收导致某些任务被驱逐（正在释放资源）时，待调度任务可先 Pipeline 绑定到该节点上；
// 若执行失败，会自动触发 unPipeline 回滚已完成的中间步骤。
func (s *Statement) Pipeline(task *api.TaskInfo, hostname string, evictionOccurred bool) (err error) {
	defer func() {
		if err == nil {
			return
		}
		if rollbackErr := s.unPipeline(task); rollbackErr != nil {
			klog.Errorf("Failed to rollback pipeline for task <%v/%v> on node <%v> in Session <%v>: %v",
				task.Namespace, task.Name, hostname, s.ssn.UID, rollbackErr)
		}
	}()

	errInfos := make([]error, 0)
	job, found := s.ssn.Jobs[task.Job]
	if found {
		job.UpdateTaskStatus(task, api.Pipelined)
	} else {
		err := fmt.Errorf("Failed to find Job <%s> in Session <%s> index when pipeline.",
			task.Job, s.ssn.UID)
		klog.Errorf("%v", err)
		errInfos = append(errInfos, err)
	}

	task.NodeName = hostname
	task.EvictionOccurred = evictionOccurred

	if node, found := s.ssn.Nodes[hostname]; found {
		if err := node.AddTask(task); err != nil {
			klog.Errorf("Failed to add task <%v/%v> to node <%v> when pipeline in Session <%v>: %v",
				task.Namespace, task.Name, hostname, s.ssn.UID, err)
			errInfos = append(errInfos, err)
		}
		klog.V(3).Infof("After pipelined Task <%v/%v> to Node <%v>: idle <%v>, used <%v>, releasing <%v>",
			task.Namespace, task.Name, node.Name, node.Idle, node.Used, node.Releasing)
	} else {
		err := fmt.Errorf("Failed to find Node <%s> in Session <%s> index when pipeline.",
			hostname, s.ssn.UID)
		klog.Errorf("%v", err)
		errInfos = append(errInfos, err)
	}

	for _, eh := range s.ssn.eventHandlers {
		if eh.AllocateFunc != nil {
			eventInfo := &Event{
				Task: task,
			}
			eh.AllocateFunc(eventInfo)
			if eventInfo.Err != nil {
				klog.Errorf("Failed to exec allocate callback functions for task <%v/%v> to node <%v> when pipeline in Session <%v>: %v",
					task.Namespace, task.Name, hostname, s.ssn.UID, eventInfo.Err)
				errInfos = append(errInfos, eventInfo.Err)
			}
		}
	}

	if len(errInfos) != 0 {
		return fmt.Errorf("Task(%s/%s) pipeline to node(%s) error and errInfos num is %d, pipeline has been rolled back",
			task.Namespace, task.Name, hostname, len(errInfos))
	} else {
		s.operations = append(s.operations, operation{
			name: Pipeline,
			task: task,
		})
	}

	return nil
}

func (s *Statement) pipeline(task *api.TaskInfo) {
}

// UnPipeline 显式回滚指定任务的 Pipeline 状态。
func (s *Statement) UnPipeline(task *api.TaskInfo) error {
	return s.unPipeline(task)
}

// unPipeline 撤销 Pipeline 操作：任务状态重置为 Pending，从节点中移除，并触发 DeallocateFunc 回调。
func (s *Statement) unPipeline(task *api.TaskInfo) error {
	job, found := s.ssn.Jobs[task.Job]
	if found {
		job.UpdateTaskStatus(task, api.Pending)
	} else {
		klog.Errorf("Failed to find Job <%s> in Session <%s> index when unpipeline.", task.Job, s.ssn.UID)
	}

	if node, found := s.ssn.Nodes[task.NodeName]; found {
		node.RemoveTask(task)
		klog.V(3).Infof("After unpipelined Task <%v/%v> to Node <%v>: idle <%v>, used <%v>, releasing <%v>",
			task.Namespace, task.Name, node.Name, node.Idle, node.Used, node.Releasing)
	} else {
		klog.Errorf("Failed to find Node <%s> in Session <%s> index when unpipeline.",
			task.NodeName, s.ssn.UID)
	}

	for _, eh := range s.ssn.eventHandlers {
		if eh.DeallocateFunc != nil {
			eventInfo := &Event{
				Task: task,
			}
			eh.DeallocateFunc(eventInfo)
		}
	}
	task.NodeName = ""
	task.EvictionOccurred = false
	task.JobAllocatedHyperNode = ""

	return nil
}

// Allocate 在事务中尝试将任务分配到指定节点：
// 1. 将任务在 Job 中的状态更新为 Allocated；
// 2. 将任务添加到目标节点的已用资源中；
// 3. 触发注册的 AllocateFunc 事件回调；
// 4. 若出现任何失败，自动触发 unallocate 回滚。
func (s *Statement) Allocate(task *api.TaskInfo, nodeInfo *api.NodeInfo) (err error) {
	defer func() {
		if err != nil {
			if rollbackErr := s.unallocate(task); rollbackErr != nil {
				klog.Errorf("Failed to rollback allocation for Task <%v/%v> on node <%v> in Session <%v>: %v",
					task.Namespace, task.Name, nodeInfo.Name, s.ssn.UID, rollbackErr)
			}
		}
	}()
	errInfos := make([]error, 0)
	hostname := nodeInfo.Name
	task.Pod.Spec.NodeName = hostname

	// Only update status in session
	job, found := s.ssn.Jobs[task.Job]
	if found {
		job.UpdateTaskStatus(task, api.Allocated)
	} else {
		err := fmt.Errorf("Failed to find Job <%s> in Session <%s> index when allocating.",
			task.Job, s.ssn.UID)
		klog.Errorf("%v", err)
		errInfos = append(errInfos, err)
	}

	task.NodeName = hostname
	if node, found := s.ssn.Nodes[hostname]; found {
		if err := node.AddTask(task); err != nil {
			klog.Errorf("Failed to add task <%v/%v> to node <%v> when allocating in Session <%v>: %v",
				task.Namespace, task.Name, hostname, s.ssn.UID, err)
			errInfos = append(errInfos, err)
		}
		klog.V(3).Infof("After allocated Task <%v/%v> to Node <%v>: idle <%v>, used <%v>, releasing <%v>",
			task.Namespace, task.Name, node.Name, node.Idle, node.Used, node.Releasing)
	} else {
		err := fmt.Errorf("Failed to find Node <%s> in Session <%s> index when allocating.",
			hostname, s.ssn.UID)
		klog.Errorf("%v", err)
		errInfos = append(errInfos, err)
	}

	// Callbacks
	for _, eh := range s.ssn.eventHandlers {
		if eh.AllocateFunc != nil {
			eventInfo := &Event{
				Task: task,
			}
			eh.AllocateFunc(eventInfo)
			if eventInfo.Err != nil {
				klog.Errorf("Failed to exec allocate callback functions for task <%v/%v> to node <%v> when allocating in Session <%v>: %v",
					task.Namespace, task.Name, hostname, s.ssn.UID, eventInfo.Err)
				errInfos = append(errInfos, eventInfo.Err)
			}
		}
	}

	if len(errInfos) != 0 {
		err = fmt.Errorf("Task %s/%s allocate to node %s error and errInfos num is %d, allocation has been rolled back",
			task.Namespace, task.Name, hostname, len(errInfos))
		return
	} else {
		// Update status in session
		klog.V(3).Info("Allocating operations ...")
		s.operations = append(s.operations, operation{
			name: Allocate,
			task: task,
		})
	}

	return nil
}

// allocate 提交分配操作：构建 BindContext，加入 cache 异步绑定队列，并将任务状态置为 Binding。
func (s *Statement) allocate(task *api.TaskInfo) error {
	bindContext := s.ssn.CreateBindContext(task)
	if err := s.ssn.cache.AddBindTask(bindContext); err != nil {
		return err
	}

	if job, found := s.ssn.Jobs[task.Job]; found {
		job.UpdateTaskStatus(task, api.Binding)
	} else {
		klog.Errorf("Failed to find Job <%s> in Session <%s> index when binding.",
			task.Job, s.ssn.UID)
		return fmt.Errorf("failed to find job %s", task.Job)
	}

	metrics.UpdateTaskScheduleDuration(metrics.TaskStageAssumed, metrics.Duration(task.Pod.CreationTimestamp.Time))
	return nil
}

// unallocate 撤销任务分配：将任务从已分配节点卸载，重置为 Pending 状态并通知 DeallocateFunc。
func (s *Statement) unallocate(task *api.TaskInfo) error {
	// Update status in session
	job, found := s.ssn.Jobs[task.Job]
	if found {
		job.UpdateTaskStatus(task, api.Pending)
	} else {
		klog.Errorf("Failed to find Job <%s> in Session <%s> index when unallocating.",
			task.Job, s.ssn.UID)
	}

	if node, found := s.ssn.Nodes[task.NodeName]; found {
		klog.V(3).Infof("Remove Task <%v> on node <%v>", task.Name, task.NodeName)
		node.RemoveTask(task)
	}

	for _, eh := range s.ssn.eventHandlers {
		if eh.DeallocateFunc != nil {
			eh.DeallocateFunc(&Event{
				Task: task,
			})
		}
	}
	task.NodeName = ""
	task.JobAllocatedHyperNode = ""

	return nil
}

// Discard 事务回滚：逆序撤销当前事务中所有未提交的操作（Evict 回滚为 unevict，Pipeline 回滚为 unPipeline，Allocate 回滚为 unallocate）。
func (s *Statement) Discard() {
	klog.V(3).Info("Discarding operations ...")
	for i := len(s.operations) - 1; i >= 0; i-- {
		op := s.operations[i]
		op.task.GenerateLastTxContext()
		switch op.name {
		case Evict:
			err := s.unevict(op.task)
			if err != nil {
				klog.Errorf("Failed to unevict task: %s", err.Error())
			}
		case Pipeline:
			err := s.unPipeline(op.task)
			if err != nil {
				klog.Errorf("Failed to unpipeline task: %s", err.Error())
			}
		case Allocate:
			err := s.unallocate(op.task)
			if err != nil {
				klog.Errorf("Failed to unallocate task: %s", err.Error())
			}
		}
	}
	s.operations = nil
}

// Commit 事务提交：将当前事务中的所有操作真正提交生效。
// Evict 向 Kubernetes API 发起 Pod 删除/驱逐；Allocate 发起向 API Server 的 Pod 节点绑定请求。
func (s *Statement) Commit() {
	klog.V(3).Info("Committing operations ...")
	for _, op := range s.operations {
		op.task.ClearLastTxContext()
		switch op.name {
		case Evict:
			err := s.evict(op.task, op.reason)
			if err != nil {
				klog.Errorf("Failed to evict task: %s", err.Error())
			}
		case Pipeline:
			s.pipeline(op.task)
		case Allocate:
			err := s.allocate(op.task)
			if err != nil {
				if e := s.unallocate(op.task); e != nil {
					klog.Errorf("Failed to unallocate task <%v/%v>: %v.", op.task.Namespace, op.task.Name, e)
				}
				klog.Errorf("Failed to allocate task <%v/%v>: %v.", op.task.Namespace, op.task.Name, err)
			}
		}
	}
	s.operations = nil
}

// Merge 将其他 Statement 的操作记录转移并入当前 Statement。
// 由于多个 Statement 共享同一个 Session 快照，内存状态变更已生效，Merge 移交操作的所有权，
// 并清空源 Statement 避免出现重复 Commit 或重复 Discard。
func (s *Statement) Merge(stmts ...*Statement) {
	for _, stmt := range stmts {
		s.operations = append(s.operations, stmt.operations...)
		stmt.operations = nil
	}
}

// SaveOperations 对一组 Statement 中的操作进行深拷贝备份，返回一个保存有操作快照的独立 Statement 对象。
// 可用于多分支决策试探（如多种调度方案比选），选定后再通过 RecoverOperations 恢复。
func SaveOperations(stmts ...*Statement) *Statement {
	stmtTmp := &Statement{}
	for _, stmt := range stmts {
		stmt.outputOperations("Save operations: ", 4)
		for _, op := range stmt.operations {
			task := op.task.Clone()
			task.EvictionOccurred = op.task.EvictionOccurred
			stmtTmp.operations = append(stmtTmp.operations, operation{
				name:   op.name,
				task:   task,
				reason: op.reason,
			})
		}
	}
	return stmtTmp
}

// RecoverOperations 在当前 Statement 上重放先前备份的一组操作。
func (s *Statement) RecoverOperations(stmt *Statement) error {
	if stmt == nil {
		return errors.New("statement is nil")
	}
	s.outputOperations("Recover operations: ", 4)
	for _, op := range stmt.operations {
		switch op.name {
		case Evict:
			s.Evict(op.task, op.reason)
		case Pipeline:
			err := s.Pipeline(op.task, op.task.NodeName, op.task.EvictionOccurred)
			if err != nil {
				klog.Errorf("Failed to pipeline task: %s", err.Error())
				return err
			}
		case Allocate:
			node := s.ssn.Nodes[op.task.NodeName]
			err := s.Allocate(op.task, node)
			if err != nil {
				klog.Errorf("Failed to allocate task <%v/%v>: %v", op.task.Namespace, op.task.Name, err)
				return err
			}
		}
	}
	return nil
}

// outputOperations 输出事务操作的调试日志。
func (s *Statement) outputOperations(msg string, level klog.Level) {
	if !klog.V(level).Enabled() {
		return
	}

	var buffer string
	for _, op := range s.operations {
		switch op.name {
		case Evict:
			buffer += fmt.Sprintf("task %s evict from node %s ", op.task.Name, op.task.NodeName)
		case Pipeline:
			buffer += fmt.Sprintf("task %s pipeline from node %s ", op.task.Name, op.task.NodeName)
		case Allocate:
			buffer += fmt.Sprintf("task %s allocate from node %s ", op.task.Name, op.task.NodeName)
		}
	}
	klog.V(level).Info(msg, buffer)
}
