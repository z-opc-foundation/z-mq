#!/usr/bin/env bash
# z-mq 三进程镜像构建（lead 005 §1.3 bin/build-images.sh）
#
# 构建上下文必须是**本仓根目录**，不是 deploy/ —— 两份 Dockerfile 都要 COPY 根 pom 与模块源码。
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

PLATFORM="${PLATFORM:-}"
PLATFORM_ARG=()
if [[ -n "$PLATFORM" ]]; then
  PLATFORM_ARG=(--platform "$PLATFORM")
  BUILD_CMD=(docker buildx build "${PLATFORM_ARG[@]}" --load)
else
  BUILD_CMD=(docker build)
fi

NS_TAG="${NS_TAG:-local}"
BROKER_TAG="${BROKER_TAG:-local}"
ADMIN_TAG="${ADMIN_TAG:-local}"

# 官方 Docker Hub 在本网络不可达（auth.docker.io i/o timeout），镜像站前缀在这里统一注入，
# 不写死在 Dockerfile 的 FROM 里。
REGISTRY_PREFIX="${REGISTRY_PREFIX-docker.m.daocloud.io/library/}"
BASE_ARGS=(
  --build-arg "BUILDER_BASE=${REGISTRY_PREFIX}maven:3.9.9-eclipse-temurin-8"
  --build-arg "RUNTIME_BASE=${REGISTRY_PREFIX}eclipse-temurin:8-jre"
)

echo "==> building z-mq-nameserver:${NS_TAG}"
"${BUILD_CMD[@]}" "${BASE_ARGS[@]}" -f deploy/Dockerfile.nameserver -t "z-mq-nameserver:${NS_TAG}" .

echo "==> building z-mq-broker:${BROKER_TAG}"
"${BUILD_CMD[@]}" "${BASE_ARGS[@]}" -f deploy/Dockerfile.broker -t "z-mq-broker:${BROKER_TAG}" .

# 控制台镜像的 COPY 里有 deploy/admin-dist/，缺目录这一步会直接失败。
# 提前跑一次构建脚本，让失败点在这里而不是 docker build 的第 3 层。
if [ "${SKIP_ADMIN:-}" != "1" ]; then
  echo "==> building z-mq-admin dist"
  bash deploy/bin/build-admin-dist.sh
fi

echo "==> building z-mq-broker-admin:${ADMIN_TAG}"
"${BUILD_CMD[@]}" "${BASE_ARGS[@]}" -f deploy/Dockerfile.admin -t "z-mq-broker-admin:${ADMIN_TAG}" .

echo "==> done"
docker images | grep -E '^z-mq-(nameserver|broker|broker-admin)' || true
