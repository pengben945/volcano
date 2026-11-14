# 04. 准入控制 (Admission)

## 模块路径
- 主入口：[`cmd/admission/`](file:///Users/Edison/volcano/cmd/admission)
- 核心实现：[`pkg/webhooks/`](file:///Users/Edison/volcano/pkg/webhooks)

## 核心功能
1. **Mutating Webhook**：自动为 Pod 注入默认的 PodGroup、配置默认队列等。
2. **Validating Webhook**：校验 Job、Queue、PodGroup 的规格合法性（如 `minAvailable` 是否合规）。

---
*(待后续逐步深度分析 Webhook 注册流程与拦截处理细节)*
