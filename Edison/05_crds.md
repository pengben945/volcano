# 05. 核心 CRD 定义

## 源码路径
- 接口定义：[`staging/src/volcano.sh/apis/pkg/apis/`](file:///Users/Edison/volcano/staging/src/volcano.sh/apis/pkg/apis)

## 核心资源类型
1. **PodGroup (`scheduling.volcano.sh/v1beta1`)**：定义成组调度元信息（`minAvailable`、`queue`、`priorityClassName`）。
2. **Queue (`scheduling.volcano.sh/v1beta1`)**：定义租户资源配额（`weight`、`capability`、`guarantee`、`reclaimable`）。
3. **Job (`batch.volcano.sh/v1alpha1`)**：面向复杂批量任务的 Job 规格定义。

---
*(待后续逐步深度分析各个结构体字段含义与业务关联)*
