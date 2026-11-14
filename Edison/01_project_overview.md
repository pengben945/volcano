# 01. 项目概览与整体架构

## 1. 项目简介
- **项目名称**：Volcano（CNCF 孵化项目）
- **定位**：云原生批量计算与高性能 AI/大数据调度系统。
- **核心场景**：
  - 分布式深度学习训练（PyTorch、TensorFlow、DeepSpeed、Megatron-LM、Ray）
  - 大数据分析与批处理（Spark、Flink）
  - 智算中心异构算力调度与资源切分（GPU / NPU / NUMA）

## 2. 核心架构组件
- **volcano-scheduler**：核心调度引擎，负责批处理调度、Gang 调度、多租户队列资源分配等。
- **volcano-controller-manager**：资源控制器，管理 VolcanoJob、PodGroup、Queue 的生命周期。
- **volcano-admission**：准入控制 Webhook，拦截并校验/补全 PodGroup、Job 等资源配置。
- **volcanoctl**：命令行管理工具。

## 3. 技术栈与框架选型
- **开发语言**：Go (`go 1.26`)
- **分布式与编排底层**：Kubernetes 原生生态（`client-go`、`k8s.io/apimachinery`、`controller-runtime`）
- **命令行框架**：`cobra` + `pflag`
- **日志与监控**：`klog/v2` + `Prometheus client_golang`
- **测试框架**：`ginkgo/v2` + `gomega` + `testify`

---
*(注：后续将按计划深入分析各组件交互时序与网络通信机制)*
