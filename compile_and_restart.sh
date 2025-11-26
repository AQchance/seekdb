#!/bin/bash
set -e

# 1. 接受参数（默认 release）
BUILD_TYPE="${1:-release}"

# 只允许 debug 或 release
if [[ "$BUILD_TYPE" != "release" && "$BUILD_TYPE" != "debug" ]]; then
  echo "Usage: $0 [release|debug]"
  exit 1
fi

echo ">>> Build type: $BUILD_TYPE"

# 2. 停止集群
./tools/deploy/obd.sh stop -n obcluster

# 3. 编译
bash build.sh "$BUILD_TYPE" --init --make -j30

# 4. 拷贝 observer
SRC_BIN="build_${BUILD_TYPE}/src/observer/observer"
DEST_BIN="data/obcluster/bin/observer"

if [[ ! -f "$SRC_BIN" ]]; then
  echo "Error: binary not found at $SRC_BIN"
  exit 1
fi

echo ">>> Copying $SRC_BIN to $DEST_BIN"
cp "$SRC_BIN" "$DEST_BIN"

# 5. 启动集群
./tools/deploy/obd.sh start -n obcluster

echo ">>> Done!"
