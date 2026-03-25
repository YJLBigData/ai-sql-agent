# SQL Agent Copilot

一个面向电商数据开发的本地 SQL AI 工程，核心目标是:

- 使用本地 MySQL 业务库 `sql_agent`
- 使用本地私有知识库辅助写 SQL
- 支持 `本地模型 / 阿里百炼 / DeepSeek / 双引擎(本地+在线)` 路由
- 支持结构化知识层、混合检索、rerank、安全分级和评测回归
- 支持直接初始化 7 张测试表和演示数据

## 项目结构

```text
AI_SQL_Agent
├─ knowledge
│  ├─ business_rules
│  ├─ metrics
│  ├─ schema_docs
│  ├─ structured
│  └─ sql_examples
├─ sql
│  ├─ create_tables.sql
│  └─ sanity_checks.sql
├─ src/sql_ai_copilot
│  ├─ agent
│  ├─ cli
│  ├─ config
│  ├─ database
│  ├─ knowledge
│  ├─ llm
│  ├─ security
│  ├─ governance
│  └─ app.py
├─ web
│  └─ templates
│     └─ index.html
├─ .env
├─ .env.example
├─ requirements.txt
└─ README.md
```

## 整体方案

### 1. 数据层

- 本地数据库: MySQL `sql_agent`
- 表模型:
  - `dim_user`
  - `dim_product`
  - `dim_store`
  - `fct_order_main`
  - `fct_order_item`
  - `fct_refund_main`
  - `fct_refund_item`

### 2. 知识层

当前知识层由 2 部分组成:

- 文档知识:
  - `schema_docs`: 表结构说明
  - `metrics`: 指标口径
  - `business_rules`: 业务规则
  - `sql_examples`: SQL 样例
- 结构化知识:
  - 指标字典
  - 维度字典
  - 同义词词典
  - 表关系图谱
  - 字段业务释义
  - 高质量 SQL 样例元数据

结构化知识和文档知识会一起参与:

- 语义识别
- 召回 + rerank
- 脱敏后的在线规划
- 本地 SQL 生成

### 3. 检索层

- 混合检索
  - 关键词
  - 语义规则
  - 向量召回
- 本地 rerank
- schema linking
- 术语归一

### 4. 生成层

- 单引擎
  - 本地 `Ollama/qwen3:8b`
  - 百炼 `qwen3-max`
  - DeepSeek `deepseek-reasoner`
- 双引擎
  - 在线模型负责高智力规划和结构化任务 JSON
  - 本地模型负责私有语义补全、SQL 草稿与解释

### 5. 安全层

- `S0`: 公开知识，可发在线
- `S1`: 内部知识，脱敏后可发在线
- `S2`: 敏感知识，只能本地处理
- SQL 校验和自动修复闭环

### 6. 治理层

- Prompt trace
- Token 成本
- 失败样本沉淀
- 100 条默认评测集生成能力
- CLI 回归评测

## 启动方式

### 1. 安装依赖

```bash
./.venv/bin/pip install -r requirements.txt
```

### 2. 初始化数据库和演示数据

```bash
PYTHONPATH=src ./.venv/bin/python -m sql_ai_copilot.cli.main init-db
```

### 3. 启动前端页面

```bash
PYTHONPATH=src ./.venv/bin/python -m sql_ai_copilot.app
```

然后访问:

- [http://127.0.0.1:8502](http://127.0.0.1:8502)

### 4. 运行评测

```bash
PYTHONPATH=src ./.venv/bin/python -m sql_ai_copilot.cli.main evaluate --provider local --engine-mode single --local-model qwen3:8b --limit 20
```

## PyCharm 一键运行

项目已经写入了共享运行配置，直接在 PyCharm 右上角选择即可:

- `SQL Agent One Click`
  - 自动检查 `sql_agent` 数据库和 7 张表
  - 如果没有数据会自动初始化
  - 然后启动前端页面
- `SQL Agent Rebuild DB`
  - 强制重建演示数据

如果 PyCharm 没自动识别运行项，重新打开项目一次即可。

## 示例问题

- 统计近30天各渠道蒙牛 GMV、退款金额和退款率
- 统计 2025 年 1 月特仑苏在华东地区各门店销售额 Top 20
- 统计纯甄系列各 SKU 的退款金额、退款件数和退款率
- 统计会员用户和非会员用户的订单量、客单价、净销售额

## 终极版主链路

```text
用户提问
-> Semantic Layer
-> Retrieval Layer
-> Security Router
-> Planner Layer
   -> 模板优先
   -> 本地模型次之
   -> 在线模型兜底
-> SQL Validator
-> SQL Executor
-> Governance
```

当前项目已经具备这条链路的第一版工程实现。

后续如果继续扩展，建议优先补:

1. 更完整的业务知识资产
2. 更强的本地评测集
3. 本地模型专题化微调或蒸馏
4. 更多高频专题模板
