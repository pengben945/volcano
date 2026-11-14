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

import "volcano.sh/volcano/pkg/scheduler/api"

// AddHyperNodeCandidateFn 注册可选的、仅用于资源分配阶段的 HyperNode 候选域过滤回调函数。
// HyperNode 是超节点（如机架、交换机、机房等层级聚合拓扑），该函数允许插件介入候选超节点的筛选决策。
func (ssn *Session) AddHyperNodeCandidateFn(name string, fn api.HyperNodeCandidateFn) {
	if ssn.hyperNodeCandidateFns == nil {
		ssn.hyperNodeCandidateFns = make(map[string]api.HyperNodeCandidateFn)
	}
	ssn.hyperNodeCandidateFns[name] = fn
}

// HyperNodeContainsNomination 检查所有启用了梯度（Gradient）计算的插件是否共同允许一个覆盖 pinned 拓扑域的候选域。
// 用于在复杂拓扑（机架/机群/超节点）调度中，校验将 Job 或 SubJob 调度固定（pinned）在特定超节点拓扑域时，
// 是否符合各拓扑插件的亲和与约束要求。
func (ssn *Session) HyperNodeContainsNomination(job *api.JobInfo, subJob *api.SubJobInfo, root *api.HyperNodeInfo, pinned string) bool {
	// 获取固定域（pinned）包含的真实物理节点集合
	nodes := ssn.RealNodesSet[pinned]
	if root == nil || len(nodes) == 0 || ssn.HyperNodes[pinned] == nil {
		return false
	}
	// 在 HyperNode 索引中查找所有包含这些节点的候选域
	candidates := ssn.HyperNodeIndex().DomainsContaining(nodes)
	if len(candidates) == 0 {
		return false
	}

	var results []api.HyperNodePluginGradient
	// 遍历 Session 每一层级配置的插件，收集启用了超节点梯度的插件约束
	for _, tier := range ssn.Tiers {
		for _, plugin := range tier.Plugins {
			if !isEnabled(plugin.EnabledHyperNodeGradient) {
				continue
			}
			jobFn, jobFound := ssn.hyperNodeGradientForJobFns[plugin.Name]
			subJobFn, subJobFound := ssn.hyperNodeGradientForSubJobFns[plugin.Name]
			if (subJob == nil && !jobFound) || (subJob != nil && !subJobFound) {
				continue
			}
			var result api.HyperNodeGradientResult
			// 优先使用插件定制的 Candidate 评估函数，否则退化为使用 Job/SubJob 梯度计算函数
			if fn := ssn.hyperNodeCandidateFns[plugin.Name]; fn != nil {
				result = fn(job, subJob, root, candidates)
			} else if subJob == nil {
				result = jobFn(job, root, api.PurposeAllocate)
			} else {
				result = subJobFn(subJob, root, api.PurposeAllocate)
			}
			if result.Unconstrained && len(result.Gradients) == 0 {
				continue
			}
			if result.Unconstrained {
				return false
			}
			results = append(results, api.HyperNodePluginGradient{PluginName: plugin.Name, Gradients: result.Gradients})
		}
	}
	// 如果没有任何插件施加约束，直接校验根域是否包含这组物理节点即可
	if len(results) == 0 {
		return ssn.RealNodesSet[root.Name].IsSuperset(nodes)
	}
	// 计算所有有约束插件允许的超节点名称的交集（合意拓扑域）
	allowed := api.HyperNodeNamesInGradients(results[0].Gradients)
	for _, result := range results[1:] {
		allowed = allowed.Intersection(api.HyperNodeNamesInGradients(result.Gradients))
	}
	// 检查是否有任何候选超节点落在共同允许的交集中
	for _, candidate := range candidates {
		if allowed.Has(candidate.Name) {
			return true
		}
	}
	return false
}
