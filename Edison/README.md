# Volcano 学习与 AI 计算平台研发求职知识库

> 本目录为学习 Volcano 源码、业务逻辑、架构设计以及备战 **AI 计算平台 / AI Infra 研发岗位** 的专属工作区。
> 后续我们将针对每个核心模块逐步展开深度剖析。

---

## 📂 模块文档规划

| 文档 | 模块 | 核心源码路径 | 说明 |
| :--- | :--- | :--- | :--- |
| [01_project_overview.md](file:///Users/Edison/volcano/Edison/01_project_overview.md) | **项目概览与整体架构** | [`cmd/`](file:///Users/Edison/volcano/cmd) | 项目定位、组件构成、技术选型 |
| [02_scheduler.md](file:///Users/Edison/volcano/Edison/02_scheduler.md) | **调度引擎 (Scheduler)** | [`pkg/scheduler`](file:///Users/Edison/volcano/pkg/scheduler) | Session、Actions、Plugins（待深度分析） |
| [03_controllers.md](file:///Users/Edison/volcano/Edison/03_controllers.md) | **控制器 (Controllers)** | [`pkg/controllers`](file:///Users/Edison/volcano/pkg/controllers) | Job、PodGroup、Queue 控制器（待深度分析） |
| [04_admission.md](file:///Users/Edison/volcano/Edison/04_admission.md) | **准入控制 (Admission)** | [`pkg/webhooks`](file:///Users/Edison/volcano/pkg/webhooks) | 校验与突变 Webhook（待深度分析） |
| [05_crds.md](file:///Users/Edison/volcano/Edison/05_crds.md) | **核心 CRD 定义** | [`staging/src/volcano.sh/apis`](file:///Users/Edison/volcano/staging/src/volcano.sh/apis) | PodGroup、Queue、Job 等数据结构（待深度分析） |
| [06_ai_infra_topics.md](file:///Users/Edison/volcano/Edison/06_ai_infra_topics.md) | **AI 平台求职专题** | - | Gang 调度、异构拓扑、多租户配额、面试题汇总 |
