# SQL Agent Copilot

一个面向电商数据开发的本地 SQL AI 工程，核心目标是:

- 使用本地 MySQL 业务库 `sql_agent`
- 使用本地私有知识库辅助写 SQL
- 支持前端直接切换 `阿里百炼 / DeepSeek`
- 支持直接初始化 7 张测试表和演示数据

## 项目结构

```text
AI_SQL_Agent
├─ knowledge
│  ├─ business_rules
│  ├─ metrics
│  ├─ schema_docs
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

当前你还没有完整知识库，因此先用 4 类文档搭一个第一版私有语义层:

- `schema_docs`: 表结构说明
- `metrics`: 指标口径
- `business_rules`: 业务规则
- `sql_examples`: SQL 样例

后续你可以继续往 `knowledge/` 里补:

- 飞鹤业务口径文档
- 活动规则
- 财务结算口径
- 组织权限规则
- 历史人工优质 SQL

### 3. 检索层

- 本地轻量检索，不依赖外部向量库
- 先根据问题命中相关文档
- 再把命中的知识 + 实时 Schema 一起给模型

### 4. 生成层

- 统一走 OpenAI Compatible 接口
- 百炼默认模型: `qwen3-max`
- DeepSeek 默认模型: `deepseek-reasoner`

### 5. 安全层

- 只允许 `SELECT / WITH`
- 通过 `EXPLAIN` 做 SQL 预校验
- 禁止生成和执行 DDL / DML

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

## 标准化落地建议

如果你后续要把这套方案从“测试库”升级到“飞鹤正式场景”，建议按下面顺序做:

1. 先固化指标口径
2. 再沉淀核心表的字段解释
3. 再沉淀常见主题 SQL 样例
4. 最后再补活动、结算、库存、供应链等业务规则

这样 AI 才能真正从“会写 SQL”进化成“懂你们公司口径的 SQL Copilot”。
