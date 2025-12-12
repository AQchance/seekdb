# Gemini Code Assistant Context: OceanBase SeekDB

This document provides context for the Gemini Code Assistant regarding the `seekdb` project, a fork of OceanBase developed for the 2025 Database Competition.

## Project Overview

**OceanBase SeekDB** is a lightweight, embedded, AI-native search database. It's designed to unify vector, full-text, JSON, and relational data processing within a single engine. The project is the basis for two competition tracks:

1.  **Kernel Track:** Optimizing the performance (QPS) of scalar-filtered, full-text search queries on a single-node `seekdb` instance.
2.  **AI Application Track:** Building a Retrieval-Augmented Generation (RAG) system using `seekdb` to answer questions from a knowledge base of PDF documents.

The core database engine is written in **C++**, while testing, evaluation, and the RAG application are implemented in **Python**.

## Core Technologies

*   **Database Core:** C++
*   **Build System:** CMake, custom `build.sh` script
*   **Testing & AI Applications:** Python
*   **Dependencies:** The project manages its dependencies, including `hyperscan`, within the `deps/` directory.

## Building and Running

The project uses a `build.sh` script to wrap the CMake build process.

### 1. Initial Setup (First time only)

Before the first build, you must initialize the dependencies. This script downloads and prepares all necessary libraries and tools.

```bash
# This command will fetch all dependencies. It may take some time.
bash build.sh --init
```

### 2. Building the Database (Kernel)

To build the `seekdb` observer (the main database process), use the `build.sh` script. Several build types are available (`debug`, `release`, `errsim`, etc.). A `release` build is recommended for performance testing.

```bash
# Example: Perform a release build using all available CPU cores.
# The --make flag triggers the compilation.
bash build.sh release --make

# For a faster build, you can specify the number of cores, e.g., -j16
bash build.sh release --make -j16
```

The main executable will be located at `build_release/src/observer/observer`.

### 3. Deploying a Test Cluster

The project includes a deployment tool `obd` for setting up a local test cluster.

```bash
# 1. Deploy a single-node cluster using the provided configuration
./tools/deploy/obd.sh deploy -c /path/to/your/obcluster.yaml

# 2. Connect to the cluster
./deps/3rd/u01/obclient/bin/obclient -h127.0.0.1 -P2881 -uroot -Dtest -A

# 3. Stop/Destroy the cluster
# ./tools/deploy/obd.sh stop obcluster
# ./tools/deploy/obd.sh destroy obcluster
```
A sample `obcluster.yaml` is provided in the `README.md`.

### 4. Running Tests

#### Kernel Performance Tests

The kernel performance tests are run using Python scripts from the `ob-mldr-test` repository (expected to be cloned separately).

```bash
# These commands are run from the ob-mldr-test directory

# 1. Load data and build indexes (pre-heating)
python3 test_insert_fast.py --lang en

# 2. Run query tests and evaluate recall
python3 get_search_rrf_oceanbase.py --languages en --query_types bm25
python3 evaluate_results_oceanbase.py --languages en --metrics recall@10
```

#### AI Application (RAG) Tests

The RAG application is located in the `rag/` directory and has its own execution and evaluation scripts.

```bash
cd rag

# 1. Install Python dependencies
pip install -r requirements.txt

# 2. Run the RAG main process
python3 main.py --dataset="./data/dataset/" --questions="./data/questions.json" --output="./data/output.json"

# 3. Evaluate the generated answers
python3 eval.py --output ./data/output.json --answer ./data/answer.json
```

## Development Conventions

*   **Code Style:** The project follows the OceanBase coding style. Refer to existing code for conventions.
*   **Build System:** All build logic is managed through `CMakeLists.txt` files. The `build.sh` script is the primary entry point for developers.
*   **Dependencies:** Dependencies are managed via `./deps/init/dep_create.sh`. Do not add system-wide dependencies.
*   **Testing:**
    *   C++ unit tests are located in the `unittest/` directory and can be built by enabling the `OB_BUILD_UNITTEST` CMake option.
    *   Python is used for integration and performance testing.
*   **Contribution:** See `CONTRIBUTING.md` for guidelines. The competition rules impose strict constraints on modifications, especially to build flags and dependencies.
