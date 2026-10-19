#!/usr/bin/env bash
# 构建控制台前端 dist 到 deploy/admin-dist/，供 Dockerfile.admin COPY 进镜像。
#
# 为什么单独一个脚本而不是把 npm 塞进 Dockerfile：
#   1) 镜像里的构建机要装 Node + 拉全量 npm 依赖，镜像层凭空大一大截；
#   2) 本网络内 npm/Docker registry 时常不可达，构建期依赖越少越稳。
# 代价是 dist 不在镜像里现算 —— 所以这个脚本是 Dockerfile.admin 的前置条件，
# 少跑一步就会得到一个只有 REST、打不开页面的空壳控制台。
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
FRONTEND_DIR="$REPO_ROOT/z-mq-admin"
OUT_DIR="$REPO_ROOT/deploy/admin-dist"

cd "$FRONTEND_DIR"

if [ ! -d node_modules ]; then
    echo "==> node_modules 不存在，先装依赖"
    npm install --no-audit --no-fund
fi

echo "==> vite build"
npm run build

if [ ! -f "$FRONTEND_DIR/dist/index.html" ]; then
    echo "构建结束但没看到 dist/index.html，构建其实没成功" >&2
    exit 1
fi

rm -rf "$OUT_DIR"
mkdir -p "$OUT_DIR"
cp -R "$FRONTEND_DIR/dist/." "$OUT_DIR/"

echo "==> dist 已就位: $OUT_DIR"
