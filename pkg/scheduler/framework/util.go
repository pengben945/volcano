/*
Copyright 2022 The Volcano Authors.

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
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/klog/v2"
	fwk "k8s.io/kube-scheduler/framework"
	k8sframework "k8s.io/kubernetes/pkg/scheduler/framework"

	"volcano.sh/volcano/pkg/scheduler/api"
)

// PodFilter 用于过滤 Pod 的判定函数签名。若 Pod 满足过滤条件返回 true，否则返回 false。
type PodFilter func(*v1.Pod) bool

// PodsLister 接口定义了调度器获取 Pod 列表的标准操作，兼容 Kubernetes 调度框架。
type PodsLister interface {
	// List 根据 LabelSelector 返回匹配的所有 Pod 列表
	List(labels.Selector) ([]*v1.Pod, error)
	// FilteredList 返回通过 podFilter 校验且匹配 LabelSelector 的 Pod 列表
	FilteredList(podFilter PodFilter, selector labels.Selector) ([]*v1.Pod, error)
}

// PodLister 是 Volcano 内部对 PodsLister 的实现，主要用于 Predicates（预选过滤）和 NodeOrder（优选打分）插件。
// 它基于当前 Session 快照中的 Tasks 构建并缓存 Pod 副本，避免高频调度评估中重复深拷贝 Pod 造成性能损耗。
type PodLister struct {
	Session *Session

	CachedPods       map[api.TaskID]*v1.Pod
	Tasks            map[api.TaskID]*api.TaskInfo
	TaskWithAffinity map[api.TaskID]*api.TaskInfo
}

// PodAffinityLister 专用于亲和性/反亲和性计算的 Pod 列表器，仅过滤包含亲和性声明的 Pod 以加速计算。
type PodAffinityLister struct {
	pl *PodLister
}

// HaveAffinity 检查 Pod 是否声明了节点亲和性（NodeAffinity）、Pod 亲和性（PodAffinity）或反亲和性（PodAntiAffinity）。
func HaveAffinity(pod *v1.Pod) bool {
	affinity := pod.Spec.Affinity
	return affinity != nil &&
		(affinity.NodeAffinity != nil ||
			affinity.PodAffinity != nil ||
			affinity.PodAntiAffinity != nil)
}

// NewPodLister 基于当前 Session 中的所有 Job 和已分配任务构建 PodLister。
func NewPodLister(ssn *Session) *PodLister {
	pl := &PodLister{
		Session: ssn,

		CachedPods:       make(map[api.TaskID]*v1.Pod),
		Tasks:            make(map[api.TaskID]*api.TaskInfo),
		TaskWithAffinity: make(map[api.TaskID]*api.TaskInfo),
	}

	for _, job := range pl.Session.Jobs {
		for status, tasks := range job.TaskStatusIndex {
			if !api.AllocatedStatus(status) {
				continue
			}

			for _, task := range tasks {
				pl.Tasks[task.UID] = task

				pod := pl.copyTaskPod(task)
				pl.CachedPods[task.UID] = pod

				if HaveAffinity(task.Pod) {
					pl.TaskWithAffinity[task.UID] = task
				}
			}
		}
	}

	return pl
}

// NewPodListerFromNode 基于当前 Session 中的所有 Node 上的已分配任务构建 PodLister。
func NewPodListerFromNode(ssn *Session) *PodLister {
	pl := &PodLister{
		Session:          ssn,
		CachedPods:       make(map[api.TaskID]*v1.Pod),
		Tasks:            make(map[api.TaskID]*api.TaskInfo),
		TaskWithAffinity: make(map[api.TaskID]*api.TaskInfo),
	}

	for _, node := range pl.Session.Nodes {
		for _, task := range node.Tasks {
			if !api.AllocatedStatus(task.Status) && task.Status != api.Releasing {
				continue
			}

			pl.Tasks[task.UID] = task
			pod := pl.copyTaskPod(task)
			pl.CachedPods[task.UID] = pod
			if HaveAffinity(task.Pod) {
				pl.TaskWithAffinity[task.UID] = task
			}
		}
	}

	return pl
}

func (pl *PodLister) copyTaskPod(task *api.TaskInfo) *v1.Pod {
	pod := task.Pod.DeepCopy()
	pod.Spec.NodeName = task.NodeName
	return pod
}

// GetPod 获取带有正确 NodeName 的 Pod 实例；优先从缓存中获取，保持只读以避免并发读写冲突。
func (pl *PodLister) GetPod(task *api.TaskInfo) *v1.Pod {
	if task.NodeName == task.Pod.Spec.NodeName {
		return task.Pod
	}

	pod, found := pl.CachedPods[task.UID]
	if !found {
		// we could not write the copied pod back into cache for read only
		pod = pl.copyTaskPod(task)
		klog.Warningf("DeepCopy for pod %s/%s at PodLister.GetPod is unexpected", pod.Namespace, pod.Name)
	}
	return pod
}

// UpdateTask 在内存中更新任务的 NodeName 并同步其缓存状态（非并发安全，应由调度主协程在绑定/驱逐时调用）。
func (pl *PodLister) UpdateTask(task *api.TaskInfo, nodeName string) *v1.Pod {
	pod, found := pl.CachedPods[task.UID]
	if !found {
		pod = pl.copyTaskPod(task)
		pl.CachedPods[task.UID] = pod
	}
	pod.Spec.NodeName = nodeName

	if !api.AllocatedStatus(task.Status) {
		delete(pl.Tasks, task.UID)
		if HaveAffinity(task.Pod) {
			delete(pl.TaskWithAffinity, task.UID)
		}
	} else {
		pl.Tasks[task.UID] = task
		if HaveAffinity(task.Pod) {
			pl.TaskWithAffinity[task.UID] = task
		}
	}

	return pod
}

// List 列出匹配标签选择器的所有 Pod。
func (pl *PodLister) List(selector labels.Selector) ([]*v1.Pod, error) {
	var pods []*v1.Pod
	for _, task := range pl.Tasks {
		pod := pl.GetPod(task)
		if selector.Matches(labels.Set(pod.Labels)) {
			pods = append(pods, pod)
		}
	}

	return pods, nil
}

// filteredListWithTaskSet 内部辅助方法：在指定 Task 集合上执行过滤器和标签筛选。
func (pl *PodLister) filteredListWithTaskSet(taskSet map[api.TaskID]*api.TaskInfo, podFilter PodFilter, selector labels.Selector) ([]*v1.Pod, error) {
	var pods []*v1.Pod
	for _, task := range taskSet {
		pod := pl.GetPod(task)
		if podFilter(pod) && selector.Matches(labels.Set(pod.Labels)) {
			pods = append(pods, pod)
		}
	}

	return pods, nil
}

// FilteredList 列出满足 podFilter 且匹配标签选择器的 Pod。
func (pl *PodLister) FilteredList(podFilter PodFilter, selector labels.Selector) ([]*v1.Pod, error) {
	return pl.filteredListWithTaskSet(pl.Tasks, podFilter, selector)
}

// AffinityFilteredList 仅针对具备亲和性声明的 Pod 集合执行过滤和标签匹配。
func (pl *PodLister) AffinityFilteredList(podFilter PodFilter, selector labels.Selector) ([]*v1.Pod, error) {
	return pl.filteredListWithTaskSet(pl.TaskWithAffinity, podFilter, selector)
}

// AffinityLister 生成基于当前 PodLister 的 PodAffinityLister。
func (pl *PodLister) AffinityLister() *PodAffinityLister {
	pal := &PodAffinityLister{
		pl: pl,
	}
	return pal
}

// List 列出所有亲和性列表中的 Pod。
func (pal *PodAffinityLister) List(selector labels.Selector) ([]*v1.Pod, error) {
	return pal.pl.List(selector)
}

// FilteredList 列出满足过滤条件的亲和性 Pod。
func (pal *PodAffinityLister) FilteredList(podFilter PodFilter, selector labels.Selector) ([]*v1.Pod, error) {
	return pal.pl.AffinityFilteredList(podFilter, selector)
}

// GenerateNodeMapAndSlice 将 Volcano 的 api.NodeInfo 映射转换为原生 Kubernetes 调度框架所使用的 fwk.NodeInfo 映射。
// 用于桥接原生预选（Predicates）插件（如 NodeResourcesFit, PodTopologySpread 等）。
func GenerateNodeMapAndSlice(nodes map[string]*api.NodeInfo) map[string]fwk.NodeInfo {
	nodeMap := make(map[string]fwk.NodeInfo)
	for _, node := range nodes {
		nodeInfo := k8sframework.NewNodeInfo(node.Pods()...)
		nodeInfo.SetNode(node.Node)
		nodeInfo.ImageStates = node.CloneImageSummary()
		nodeMap[node.Name] = nodeInfo
	}
	return nodeMap
}

// CachedNodeInfo 实现了对单个 Node 信息的快速检索，供打分和预选插件使用。
type CachedNodeInfo struct {
	Session *Session
}

// GetNodeInfo 根据节点名称获取对应的 v1.Node 对象。
func (c *CachedNodeInfo) GetNodeInfo(name string) (*v1.Node, error) {
	node, found := c.Session.Nodes[name]
	if !found {
		return nil, errors.NewNotFound(v1.Resource("node"), name)
	}

	return node.Node, nil
}

// NodeLister 用于在 NodeOrder 打分插件中获取集群所有节点列表。
type NodeLister struct {
	Session *Session
}

// List 获取当前 Session 中所有节点的 v1.Node 列表。
func (nl *NodeLister) List() ([]*v1.Node, error) {
	var nodes []*v1.Node
	for _, node := range nl.Session.Nodes {
		nodes = append(nodes, node.Node)
	}
	return nodes, nil
}
