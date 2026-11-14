# 02. 调度引擎 (Scheduler)

## 模块路径
- 主入口：[`cmd/scheduler/`](file:///Users/Edison/volcano/cmd/scheduler)
- 核心实现：[`pkg/scheduler/`](file:///Users/Edison/volcano/pkg/scheduler)

## 基础结构
1. **Cache 层**：[`pkg/scheduler/cache`](file:///Users/Edison/volcano/pkg/scheduler/cache) —— 缓存集群节点、队列、任务等状态。
2. **Framework 框架**：[`pkg/scheduler/framework`](file:///Users/Edison/volcano/pkg/scheduler/framework) —— 定义 Session、Action 与 Plugin 接口。
3. **Actions 动作集合**：[`pkg/scheduler/actions`](file:///Users/Edison/volcano/pkg/scheduler/actions) —— enqueue、allocate、preempt、reclaim、backfill 等。
4. **Plugins 插件集合**：[`pkg/scheduler/plugins`](file:///Users/Edison/volcano/pkg/scheduler/plugins) —— gang、proportion、drf、deviceshare、numaaware 等。

---
*(待后续逐步深度分析 Session 机制、动作执行流水线与核心算法插件)*
