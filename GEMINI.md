# Gemini

## 项目：SeekDB - AI原生混合搜索数据库

本项目是由 OceanBase 开发的基于 C++ 的数据库 **SeekDB**。它是 2025 年数据库竞赛的主题，该竞赛旨在优化其搜索功能并在此基础上构建 AI 应用。

SeekDB 被设计为一个轻量级、嵌入式、AI 原生搜索数据库。它将矢量、全文、JSON、关系型和 GIS 数据处理整合到一个引擎中。

竞赛分为两大挑战：

1. **内核挑战**：优化标量过滤全文搜索查询的性能。

2. **AI 应用挑战**：使用 SeekDB 构建一个基于 PDF 文档知识库的检索增强生成 (RAG) 应用，以回答问题。

## 构建和运行

本项目使用 `CMake` 作为构建系统，并使用便捷的包装脚本 `build.sh` 来管理构建过程。

### 1. 初始设置（依赖项）

首次构建之前，您必须初始化项目以下载并设置所有必要的依赖项。这可以通过在构建命令中添加 `--init` 标志来完成。

```bash
# 此命令将下载依赖项并准备构建环境

bash build.sh release --init

```

### 2. 编译数据库

主要的构建脚本是 `build.sh`。建议比赛使用 `release` 构建类型。

```bash
# 编译项目（初始化后）：

# -jN 标志指定并行编译作业的数量（例如，-j4）

bash build.sh release --make -j4

```

完整的首次构建命令如下所示：

```bash

bash build.sh release --init --make -j4

```

主可执行文件将位于 `build_release/src/observer/observer`。

### 3. 部署本地集群

该项目包含一个部署脚本 `obd.sh`，用于运行本地单节点集群以进行测试。

```bash
# 1. 使用提供的配置部署集群

./tools/deploy/obd.sh deploy -c /data/obcluster.yaml

# 2. 连接到数据库

./deps/3rd/u01/obclient/bin/obclient -h127.0.0.1 -P2881 -uroot -Dtest -A

# 3. 停止/重启/销毁集群

./tools/deploy/obd.sh stop obcluster

./tools/deploy/obd.sh restart obcluster

./tools/deploy/obd.sh destroy obcluster

```
*注意：`obcluster.yaml` 配置文件的详细信息请参阅主 `README.md` 文件。*

### 4. 一键编译并启动集群

```bash

bash ./compile_and_restart.sh

```

### 5. 运行测试

测试使用连接到已部署数据库的外部脚本和应用程序执行。

#### 内核性能测试

- **测试脚本**：位于单独的仓库中：[https://github.com/oceanbase/ob-mldr-test](https://github.com/oceanbase/ob-mldr-test)。

- **流程**：

1. 使用 `test_insert_fast.py` 加载数据并构建索引。

2. 使用 `get_search_rrf_oceanbase.py` 运行查询性能测试。

3. 集成脚本 `mldr_data_test.py` 可以运行完整的流程（1 次预热 + 3 次测试运行）。

- **示例命令**：

```bash

python3.9 mldr_data_test.py --lang en --query_types bm25

```

#### AI 应用 (RAG) 测试

- **应用代码**：位于 `rag/` 目录中。

- **依赖项**：通过 `pip install -r rag/requirements.txt` 安装。

- **流程**：`rag/main.py` 脚本执行整个 RAG 流程，从处理文档到生成答案。

- **示例命令**：

```bash

cd rag && python3.10 main.py

```

## 开发约定

* **语言**：核心数据库使用 **C++** 编写。测试层和应用层使用 **Python** 编写。

* **构建系统**：**CMake** 是主要的构建系统。

* **依赖项**：第三方库通过 `deps/init/dep_create.sh` 脚本进行管理。不建议直接修改 `deps` 目录。

* **贡献指南**：该项目包含 `CONTRIBUTING.md` 和 `CODE_OF_CONDUCT.md` 文件，表明其已制定贡献指南。

* **代码风格**：虽然在初始分析中并未明确定义，但鉴于该项目拥有庞大且成熟的 C++ 代码库，遵循现有的代码风格至关重要。