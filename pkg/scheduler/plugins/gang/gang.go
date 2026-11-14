/*
Copyright 2018 The Kubernetes Authors.
Copyright 2018-2025 The Volcano Authors.

Modifications made by Volcano authors:
- Enhanced gang scheduling validation with task-level validity checks
- Improved preemption logic to respect gang scheduling constraints
- Added support for job starving detection and enhanced pipeline state management

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

package gang

import (
	"fmt"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"

	"volcano.sh/apis/pkg/apis/scheduling"
	"volcano.sh/apis/pkg/apis/scheduling/v1beta1"
	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/framework"
	"volcano.sh/volcano/pkg/scheduler/metrics"
	"volcano.sh/volcano/pkg/scheduler/plugins/util"
)

// PluginName indicates name of volcano scheduler plugin.
// 插件的唯一名称，用于在配置(scheduler conf)和框架(framework)中标识该插件。
const PluginName = "gang"

// gangPlugin 实现了 framework.Plugin 接口，用于实现 Gang 调度（All-or-Nothing 批处理调度算法）。
// Gang 调度要求属于同一个作业的一组 Pod 要么全部（或至少达到 minAvailable 数量）成功调度，要么都不调度，
// 从而避免多任务分布式计算（如 AI 训练、MPI 计算）中因资源互锁导致的死锁和资源浪费。
type gangPlugin struct {
	// Arguments given for the plugin
	// 存储从 scheduler 配置文件中传递给该插件的参数（键值对）。
	pluginArguments framework.Arguments
}

// New return gang plugin
// 插件的构造函数，符合 framework.PluginBuilder 类型定义。
// 在 framework.RegisterPluginBuilder 中注册此方法后，调度器初始化各 Session 时将调用该方法创建插件实例。
func New(arguments framework.Arguments) framework.Plugin {
	return &gangPlugin{pluginArguments: arguments}
}

// Name 返回插件在 Volcano 调度框架中的唯一名称。
func (gp *gangPlugin) Name() string {
	return PluginName
}

// OnSessionOpen 在每次调度周期（Session 打开）时被框架调用。
// 插件通过该方法向当前 Session 注册核心算法回调（Hooks/Extension Points），
// 例如准入校验 (JobValid)、抢占过滤 (Preemptable/Reclaimable)、就绪判定 (JobReady)、
// 预留资源 (JobPipelined)、饥饿检测 (JobStarving) 以及作业排序 (JobOrder) 等。
func (gp *gangPlugin) OnSessionOpen(ssn *framework.Session) {
	// 1. 注册 Job 准入校验函数 (JobValidFn)
	// 在调度动作（如 allocate/enqueue）尝试处理作业之前，验证该 Job 是否符合 Gang 调度要求。
	validJobFn := func(obj interface{}) *api.ValidateResult {
		job, ok := obj.(*api.JobInfo)
		if !ok {
			return &api.ValidateResult{
				Pass:    false,
				Message: fmt.Sprintf("Failed to convert <%v> to *JobInfo", obj),
			}
		}

		// 检查 Task 级别的 Gang 约束是否满足
		if valid := job.CheckTaskValid(); !valid {
			return &api.ValidateResult{
				Pass:    false,
				Reason:  v1beta1.NotEnoughPodsOfTaskReason,
				Message: "Not enough valid pods of each task for gang-scheduling",
			}
		}

		// 检查 SubJob（子作业）级别的 Gang 约束是否满足
		if valid := job.CheckSubJobValid(); !valid {
			return &api.ValidateResult{
				Pass:    false,
				Reason:  v1beta1.NotEnoughPodsOfTaskReason,
				Message: "Not enough valid subGroups of each task for gang-scheduling",
			}
		}

		// 检查作业中有效的 Task 数量（排除已完成、已失败或处于非合法状态的 Task）是否达到最小可用数 (MinAvailable)。
		// 如果总有效 Task 数量不足 MinAvailable，说明即使集群资源充足也绝不可能凑齐一个 Gang，直接拒绝准入。
		vtn := job.ValidTaskNum()
		if vtn < job.MinAvailable {
			return &api.ValidateResult{
				Pass:   false,
				Reason: v1beta1.NotEnoughPodsReason,
				Message: fmt.Sprintf("Not enough valid tasks for gang-scheduling, valid: %d, min: %d",
					vtn, job.MinAvailable),
			}
		}
		return nil
	}

	ssn.AddJobValidFn(gp.Name(), validJobFn)

	// 2. 注册可抢占/可回收判定函数 (PreemptableFn & ReclaimableFn)
	// 用于在抢占 (preempt) 或跨队列回收 (reclaim) 时过滤牺牲者 (victims)。
	// Gang 保护规则：被抢占的 Job，其剩余正常运行的任务数不能低于该 Job 的 MinAvailable。
	// 如果抢占某个 Task 会导致该被抢占 Job 的存活数跌破 MinAvailable，则该 Task 绝对不能作为受害者被驱逐，
	// 从而保护已成功成团运行的 Gang 作业不被破坏。
	preemptableFn := func(preemptor *api.TaskInfo, preemptees []*api.TaskInfo) ([]*api.TaskInfo, int) {
		var victims []*api.TaskInfo
		jobOccupiedMap := map[api.JobID]int32{}

		for _, preemptee := range preemptees {
			job := ssn.Jobs[preemptee.Job]
			if job == nil {
				klog.Warningf("[gang] Skip preemptee <%s/%s>: job <%s> not found in session (orphaned task from deleted PodGroup)",
					preemptee.Namespace, preemptee.Name, preemptee.Job)
				continue
			}

			// 初始化该被抢占 Job 当前已就绪/正在运行的任务数
			if _, found := jobOccupiedMap[job.UID]; !found {
				jobOccupiedMap[job.UID] = job.ReadyTaskNum()
			}

			// 仅当该 Job 剩余存活任务数大于 MinAvailable 时，才允许该 Task 被作为受害者（超出保障配额的部分允许被抢）
			if jobOccupiedMap[job.UID] > job.MinAvailable {
				jobOccupiedMap[job.UID]--
				victims = append(victims, preemptee)
			} else {
				klog.V(4).Infof("Can not preempt task <%v/%v> because job %s ready num(%d) <= MinAvailable(%d) for gang-scheduling",
					preemptee.Namespace, preemptee.Name, job.Name, jobOccupiedMap[job.UID], job.MinAvailable)
			}
		}

		klog.V(4).Infof("Victims from Gang plugins, victims=%+v preemptor=%s", victims, preemptor)

		return victims, util.Permit
	}

	// TODO(k82cn): Support preempt/reclaim batch job.
	// 将该保护策略同时注册到回收和抢占流程中
	ssn.AddReclaimableFn(gp.Name(), preemptableFn)
	ssn.AddPreemptableFn(gp.Name(), preemptableFn)

	// Currently handles GangReclaim and GangPreempt only.
	// Support for legacy task-level preempt/reclaim will be added in the future.
	// 3. 注册统一驱逐判定函数 (UnifiedEvictableFn)
	// 在统一驱逐模型下，Gang 调度通过 bundle 机制整体管理最小配额约束，因此这里放行候选集。
	ssn.AddUnifiedEvictableFn(gp.Name(), func(_ *api.EvictionContext, candidates []*api.TaskInfo) ([]*api.TaskInfo, int) {
		// Gang-aware eviction uses the bundle model (safe/whole split) to manage
		// MinAvailable constraints, so the plugin permits all candidates here.
		return candidates, util.Permit
	})

	// 4. 注册作业排序函数 (JobOrderFn)
	// 决定多个 Job 之间的调度处理先后次序。
	// 排序逻辑：优先处理已经就绪（IsReady 为 true，即已分配/运行任务数已达到 MinAvailable）的 Job。
	// 尽快让已满足 Gang 条件的作业完成后续全部 Task 的绑定和投递，避免资源被其他未满足的作业打断。
	jobOrderFn := func(l, r interface{}) int {
		lv := l.(*api.JobInfo)
		rv := r.(*api.JobInfo)

		lReady := lv.IsReady()
		rReady := rv.IsReady()

		klog.V(4).Infof("Gang JobOrderFn: <%v/%v> is ready: %t, <%v/%v> is ready: %t",
			lv.Namespace, lv.Name, lReady, rv.Namespace, rv.Name, rReady)

		// 两个都 Ready 或都未 Ready 时，本插件不分优劣，返回 0 交给其他排序插件（如 priority/drf）进一步排序
		if lReady && rReady {
			return 0
		}

		// l 已就绪而 r 未就绪，则 l 优先调度 (返回 1)
		if lReady {
			return 1
		}

		// r 已就绪而 l 未就绪，则 r 优先调度 (返回 -1)
		if rReady {
			return -1
		}

		return 0
	}
	ssn.AddJobOrderFn(gp.Name(), jobOrderFn)

	// 5. 注册子作业排序函数 (SubJobOrderFn)
	// 针对层次化作业/SubJob 的优先级排序，逻辑同上，优先调度已经就绪的 SubJob。
	subJobOrderFn := func(l, r interface{}) int {
		lv := l.(*api.SubJobInfo)
		rv := r.(*api.SubJobInfo)

		lReady := lv.IsReady()
		rReady := rv.IsReady()

		klog.V(4).Infof("Gang SubJobOrderFn: <%v> is ready: %t, <%v> is ready: %t",
			lv.UID, lReady, rv.UID, rReady)

		if lReady && rReady {
			return 0
		}

		if lReady {
			return 1
		}

		if rReady {
			return -1
		}

		return 0
	}
	ssn.AddSubJobOrderFn(gp.Name(), subJobOrderFn)

	// 6. 注册作业就绪判定函数 (JobReadyFn)
	// 判定一个 Job 在当前会话阶段是否满足“整体就绪”条件。
	// 判定依据：Job 的 Task 级别约束已满足，且 SubJob 级别约束已满足，
	// 且 Job 当前 Allocated + Ready 的任务总数已达到 MinAvailable (ji.IsReady())。
	// 在 allocate 阶段，如果 Job 尝试分配后仍未达成 JobReady，框架会触发事务回滚，撤销本次调度分配，保障 All-or-Nothing。
	ssn.AddJobReadyFn(gp.Name(), func(obj interface{}) bool {
		ji := obj.(*api.JobInfo)
		if ji.CheckTaskReady() && ji.CheckSubJobReady() && ji.IsReady() {
			return true
		}
		return false
	})

	// 7. 注册子作业就绪判定函数 (SubJobReadyFn)
	ssn.AddSubJobReadyFn(gp.Name(), func(obj interface{}) bool {
		sji := obj.(*api.SubJobInfo)
		return sji.IsReady()
	})

	// 8. 注册流水线/资源预留判定函数 (JobPipelinedFn)
	// Pipelined 是 Volcano 提供的资源预留机制：当集群当前可用资源不足以一次性满足 Job 的 MinAvailable，
	// 但如果等待正在运行的某些任务释放或通过抢占能够满足时，可将任务置为 Pipelined 状态预留节点资源。
	// 当整个 Job 处于 Allocated + Pipelined 状态的任务数满足 MinAvailable 时，返回 Permit 允许预留。
	pipelinedFn := func(obj interface{}) int {
		ji := obj.(*api.JobInfo)
		if ji.CheckTaskPipelined() && ji.CheckSubJobPipelined() && ji.IsPipelined() {
			return util.Permit
		}
		return util.Reject
	}
	ssn.AddJobPipelinedFn(gp.Name(), pipelinedFn)

	// 9. 注册子作业流水线判定函数 (SubJobPipelinedFn)
	ssn.AddSubJobPipelinedFn(gp.Name(), func(obj interface{}) int {
		sji := obj.(*api.SubJobInfo)
		if sji.IsPipelined() {
			return util.Permit
		}
		return util.Reject
	})

	// 10. 注册作业饥饿检测函数 (JobStarvingFn)
	// 判定作业是否处于资源饥饿状态（等待资源超时或无法达到 MinAvailable）。
	// 在抢占动作 (preempt action) 中，只有被判定为处于饥饿状态 (Starving) 的作业才被允许发起抢占。
	jobStarvingFn := func(obj interface{}) bool {
		ji := obj.(*api.JobInfo)
		// In the preemption scenario, the taskMinAvailable configuration is not concerned, only the jobMinAvailable is concerned
		return ji.IsStarving()
	}
	ssn.AddJobStarvingFns(gp.Name(), jobStarvingFn)
}

// OnSessionClose 在调度周期（Session 结束）时被框架统一调用。
// Gang 插件在此阶段负责：
// 1. 检查本次会话中所有 Job 的调度达成情况；
// 2. 将调度的最终结果（满足 Gang 或无法满足 Gang）以 PodGroupCondition 形式写回到 Kubernetes CRD (PodGroup) 中；
// 3. 统计并向 Prometheus 上报调度失败/成功的相关指标。
func (gp *gangPlugin) OnSessionClose(ssn *framework.Session) {
	var unreadyTaskCount int32
	var unScheduleJobCount int
	for _, job := range ssn.Jobs {
		// skip the jobs that have no tasks.
		// 跳过没有任何 Task 的空作业
		if len(job.Tasks) == 0 {
			continue
		}
		// 如果作业未能满足 Gang 约束（未达到 MinAvailable 数量的 Pod 就绪）
		if !job.IsReady() {
			// 若由于历史 Pod 处于 Releasing 且已绑定节点，实际有效运行任务已够，则跳过错误上报
			if shouldSkipUnschedulableForReleasing(job) {
				continue
			}
			// 计算当前会话中已经尝试分配（在事务中成功）加上已经处于 Ready 状态的任务总数
			schedulableTaskNum := func() (num int32) {
				for _, task := range job.TaskStatusIndex[api.Pending] {
					ctx := task.GetTransactionContext()
					if task.LastTransaction != nil {
						ctx = *task.LastTransaction
					}
					if api.AllocatedStatus(ctx.Status) {
						num++
					}
				}
				return num + job.ReadyTaskNum()
			}
			// 距离达成 MinAvailable 还缺少的 Task 数量
			unreadyTaskCount = job.MinAvailable - schedulableTaskNum()
			msg := fmt.Sprintf("%v/%v tasks in gang unschedulable: %v",
				unreadyTaskCount, len(job.Tasks), job.FitError())

			unScheduleJobCount++
			if !ssn.IsJobTerminated(job.UID) {
				metrics.RegisterJobRetries(job.Name)
			}

			// TODO: If the Job is gang-unschedulable due to scheduling gates
			// we need a new message and reason to tell users
			// More detail in design doc pod-scheduling-readiness.md
			// 构造 PodGroupCondition: 设置状态为 Unschedulable，原因一般为资源不足，详细说明缺失的 Pod 数量和不匹配错误
			jc := &scheduling.PodGroupCondition{
				Type:               scheduling.PodGroupUnschedulableType,
				Status:             v1.ConditionTrue,
				LastTransitionTime: metav1.Now(),
				TransitionID:       string(ssn.UID),
				Reason:             v1beta1.NotEnoughResourcesReason,
				Message:            msg,
			}

			// 同步写回 PodGroup CRD 状态
			if err := ssn.UpdatePodGroupCondition(job, jc); err != nil {
				klog.Errorf("Failed to update job <%s/%s> condition: %v",
					job.Namespace, job.Name, err)
			}
		} else {
			// 作业成功满足 MinAvailable，构造 PodGroupScheduled 状态 Condition
			jc := &scheduling.PodGroupCondition{
				Type:               scheduling.PodGroupScheduled,
				Status:             v1.ConditionTrue,
				LastTransitionTime: metav1.Now(),
				TransitionID:       string(ssn.UID),
				Reason:             "tasks in gang are ready to be scheduled",
				Message:            "",
			}

			// 同步写回 PodGroup CRD 状态
			if err := ssn.UpdatePodGroupCondition(job, jc); err != nil {
				klog.Errorf("Failed to update job <%s/%s> condition: %v",
					job.Namespace, job.Name, err)
			}
		}
		// 记录指标：未调度的任务数量
		if !ssn.IsJobTerminated(job.UID) {
			metrics.UpdateUnscheduleTaskCount(job.Name, int(unreadyTaskCount))
		}
		unreadyTaskCount = 0
	}

	// 记录指标：本次 Session 周期内不可调度的 Job 总数
	metrics.UpdateUnscheduleJobCount(unScheduleJobCount)
}

// shouldSkipUnschedulableForReleasing reports whether an unready job has enough
// effective scheduled tasks to satisfy minAvailable. Releasing tasks still
// assigned to nodes count because their Pods continue to occupy gang slots.
// Pending or Pipelined tasks prevent this shortcut because they still require
// scheduling.
// 判定一个未就绪的 Job 是否实际具备足够多的有效已调度 Task 以满足 minAvailable。
// 当某些 Task 处于 Releasing 状态但依然驻留在 Node 上时，它们依然物理占用着 Gang 槽位。
// 如果存在 Pending 或 Pipelined 任务，则不能跳过，因为它们依然需要正常调度处理。
func shouldSkipUnschedulableForReleasing(job *api.JobInfo) bool {
	scheduledReleasing := job.ReleasingScheduledTaskNum()
	if scheduledReleasing == 0 {
		return false
	}

	// Pending or Pipelined tasks still require the normal gang condition so they
	// remain visible as work that needs to be scheduled.
	// 如果作业中还有处于等待 (Pending) 或预留 (Pipelined) 状态的 Task，
	// 说明该作业仍处于调度活跃流程中，需要正常上报 Gang Condition。
	if len(job.TaskStatusIndex[api.Pending]) > 0 || len(job.TaskStatusIndex[api.Pipelined]) > 0 {
		return false
	}

	// 实际有效调度的 Task 数量 = 已调度任务数 + 仍占节点的释放中任务数
	effectiveScheduled := job.ScheduledTaskNum() + scheduledReleasing
	return effectiveScheduled >= job.MinAvailable
}
