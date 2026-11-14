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
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"volcano.sh/volcano/pkg/scheduler/api"
)

const (
	// queueDRADeviceClassCountPrefix DRA 设备类数量在资源列表中的命名空间前缀
	queueDRADeviceClassCountPrefix = "deviceclass/"
	// queueDRADeviceClassCapacitySep DRA 设备类容量维度的分隔符
	queueDRADeviceClassCapacitySep = ".deviceclass/"
)

// addTaskDRAAllocatedByQueue 统计指定 Task 占用的 DRA（动态资源分配，Dynamic Resource Allocation）资源，
// 并将其累加到所属队列（Queue）的已分配 DRA 资源集合中。
// 如果同一个 ResourceClaim 被同一个 Job 内的多个 Task 共享引用，通过 resourceClaimRefs 避免重复统计资源配额。
func addTaskDRAAllocatedByQueue(
	allocated map[api.QueueID]map[string]*api.DRAResource,
	resourceClaimRefs map[api.QueueID]map[string]int,
	queueID api.QueueID,
	task *api.TaskInfo,
) {
	if task == nil {
		return
	}

	if allocated[queueID] == nil {
		allocated[queueID] = make(map[string]*api.DRAResource)
	}
	if resourceClaimRefs[queueID] == nil {
		resourceClaimRefs[queueID] = make(map[string]int)
	}

	// 如果没有细分的 Claim 级别 DRA 请求，直接合并 Task 级别的 DRA 资源请求
	if len(task.ResourceClaimDRAResreq) == 0 {
		mergeDRAResourceMap(allocated[queueID], task.DRAResreq)
		return
	}

	// 遍历 Task 关联的所有 ResourceClaim
	for _, claimKey := range task.ResourceClaimKeys {
		claimReq := task.ResourceClaimDRAResreq[claimKey]
		if len(claimReq) == 0 {
			continue
		}
		// 若当前队列此前尚未统计该 Claim（首次引用），则将该 Claim 的资源需求计入队列已分配
		if resourceClaimRefs[queueID][claimKey] == 0 {
			mergeDRAResourceMap(allocated[queueID], claimReq)
		}
		// 增加引用计数，防止同一 Claim 被同一队列多次重复计算
		resourceClaimRefs[queueID][claimKey]++
	}
}

// mergeDRAResourceMap 将源 DRA 资源映射（src）按 deviceClass 累加合并到目标映射（dst）中。
func mergeDRAResourceMap(dst map[string]*api.DRAResource, src map[string]*api.DRAResource) {
	if len(src) == 0 {
		return
	}

	for deviceClass, res := range src {
		if res == nil {
			continue
		}
		if dst[deviceClass] == nil {
			dst[deviceClass] = &api.DRAResource{
				Capacity: make(map[string]resource.Quantity),
			}
		}
		dst[deviceClass].Add(res)
	}
}

// mergeDRAAllocatedIntoResourceList 将 DRA 设备类及容量信息格式化并合并进标准的 Kubernetes v1.ResourceList 中。
// 格式化后的资源名称格式：
// 1. 设备数量: "deviceclass/<deviceClass>"
// 2. 具体维度容量: "<dim>.deviceclass/<deviceClass>"
func mergeDRAAllocatedIntoResourceList(
	allocated v1.ResourceList,
	draAllocated map[string]*api.DRAResource,
) v1.ResourceList {
	if allocated == nil {
		allocated = make(v1.ResourceList)
	}

	for deviceClass, res := range draAllocated {
		if res == nil {
			continue
		}

		if res.Count > 0 {
			allocated[v1.ResourceName(queueDRADeviceClassCountPrefix+deviceClass)] = *resource.NewQuantity(res.Count, resource.DecimalSI)
		}

		for dim, qty := range res.Capacity {
			if qty.IsZero() {
				continue
			}
			allocated[v1.ResourceName(dim+queueDRADeviceClassCapacitySep+deviceClass)] = qty.DeepCopy()
		}
	}

	return allocated
}
