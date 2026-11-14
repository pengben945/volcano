# 03. 控制器模块 (Controllers)

## 模块路径
- 主入口：[`cmd/controller-manager/`](file:///Users/Edison/volcano/cmd/controller-manager)
- 核心实现：[`pkg/controllers/`](file:///Users/Edison/volcano/pkg/controllers)

## 核心控制器
1. **Job Controller**：[`pkg/controllers/job`](file:///Users/Edison/volcano/pkg/controllers/job) —— Volcano Job 状态机、Task 依赖与生命周期。
2. **PodGroup Controller**：[`pkg/controllers/podgroup`](file:///Users/Edison/volcano/pkg/controllers/podgroup) —— 管理成组调度的状态流转。
3. **Queue Controller**：[`pkg/controllers/queue`](file:///Users/Edison/volcano/pkg/controllers/queue) —— 租户队列状态与资源配额维护。

---
*(待后续逐步深度分析各个 Controller 的 Reconcile 循环与事件监听)*
