# z-mq 部署快捷入口（lead 005 §1.3）
#
# 所有镜像构建的上下文都在 make 里被锚到仓库根（REPO_ROOT），不要从 deploy/ 里直接
# `docker build .` —— 那会因为拿不到根 pom 而在 builder 阶段就解析不到 parent。

REPO_ROOT := $(abspath $(dir $(lastword $(MAKEFILE_LIST)))/..)
NS_IMAGE   ?= z-mq-nameserver:local
BROKER_IMAGE ?= z-mq-broker:local
K8S_DIR    := $(REPO_ROOT)/deploy/k8s

.PHONY: help build admin-dist images compose-up compose-down k8s-apply k8s-status k8s-clean

help:
	@echo "make build       - 跑本仓 reactor 编译（出 exec jar）"
	@echo "make admin-dist  - 构建控制台前端 dist 到 deploy/admin-dist/"
	@echo "make images      - 构建 NameServer + Broker + Admin 三个镜像"
	@echo "make compose-up  - 本地 compose 起两个进程"
	@echo "make k8s-apply   - 顺序 apply deploy/k8s/*.yaml"
	@echo "make k8s-status  - 看两个 StatefulSet 状态"
	@echo "make k8s-clean   - 删掉 k8s 里的 z-mq namespace"

build:
	cd $(REPO_ROOT) && mvn -pl z-mq-nameserver,z-mq-broker,z-mq-broker-admin -am package -DskipTests

# 控制台 dist 必须先落到 deploy/admin-dist/，Dockerfile.admin 才 COPY 得到
admin-dist:
	cd $(REPO_ROOT) && bash deploy/bin/build-admin-dist.sh

images:
	cd $(REPO_ROOT) && bash deploy/bin/build-images.sh

compose-up:
	cd $(REPO_ROOT) && docker compose -f deploy/docker-compose.yml up -d --build

compose-down:
	cd $(REPO_ROOT) && docker compose -f deploy/docker-compose.yml down

k8s-apply:
	kubectl apply -f $(K8S_DIR)/00-namespace.yaml
	kubectl apply -f $(K8S_DIR)/01-statefulset-nameserver.yaml
	kubectl apply -f $(K8S_DIR)/02-statefulset-broker.yaml
	kubectl apply -f $(K8S_DIR)/03-services.yaml
	kubectl apply -f $(K8S_DIR)/04-deployment-admin.yaml

k8s-status:
	kubectl -n z-mq get sts,deploy,pods,svc

k8s-clean:
	kubectl delete namespace z-mq --ignore-not-found
