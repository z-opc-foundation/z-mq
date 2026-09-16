# Z-MQ Console

> Z-MQ 控制台 — 集群 / Topic / Producer / Consumer 可视化管理 (对标 Apache RocketMQ Console)

## 一句话定位

基于 Vue 3 + Vite + Element Plus 的现代化管理控制台, 提供 7 大核心功能:

- **仪表盘** — 集群 TPS / 消息数 / 存储实时统计
- **集群** — Broker 节点 / Master-Slave / 入出 TPS / 心跳监控
- **Topic 管理** — 创建 / 查询 / 删除 Topic, 读/写队列配置
- **消息查询** — 按 Topic + queueId + offset 查询消息内容
- **Consumer Group** — 订阅关系 / 消费进度 / 消息积压
- **延迟消息** — RocketMQ 18 级延迟投递 (1s / 5s / ... / 2h)
- **系统设置** — Broker Admin 地址 / 默认队列数

## 技术栈

| 模块 | 选型 | 用途 |
|------|------|------|
| 框架 | Vue 3.4 | Composition API + `<script setup>` |
| 构建 | Vite 5 | 快速冷启动 / HMR |
| 路由 | Vue Router 4 | Hash 模式路由 (无需后端 rewrite) |
| 状态 | Pinia 2 | 集群 / Topic 全局状态 |
| UI 库 | Element Plus 2.7 | 中文友好的桌面端组件库 |
| 图表 | ECharts 5 | 仪表盘趋势图 (预留) |
| HTTP | Axios 1.7 | 后端 REST API 调用 |
| 语言 | TypeScript 5.4 | 全量类型化 |

## 项目结构

```
z-mq-console/
├── package.json             # 依赖与脚本
├── vite.config.ts           # Vite 配置 (含 /api proxy)
├── tsconfig.json            # TypeScript 主配置
├── tsconfig.node.json       # Node 端 TS 配置
├── index.html               # 入口 HTML
├── public/
│   └── zmq-logo.svg         # 站点图标
├── src/
│   ├── main.ts              # 应用启动入口
│   ├── App.vue              # 顶层布局 (侧边栏 + 头部 + 主区域)
│   ├── env.d.ts             # 环境变量类型
│   ├── router/index.ts      # 路由配置 (Hash 模式)
│   ├── stores/cluster.ts    # Pinia 全局状态
│   ├── api/index.ts         # 后端 API 客户端 + 类型定义 + Mock 数据
│   ├── assets/main.css      # 全局样式
│   └── views/
│       ├── DashboardView.vue    # 仪表盘
│       ├── ClusterView.vue      # 集群节点
│       ├── TopicsView.vue       # Topic 管理
│       ├── MessagesView.vue     # 消息查询
│       ├── ConsumersView.vue    # Consumer Group
│       ├── DelayView.vue        # 延迟消息
│       └── SettingsView.vue     # 系统设置
```

## 快速开始

### 安装依赖

```bash
cd z-mq-console
npm install   # 或 pnpm install / yarn
```

### 启动开发服务器

```bash
npm run dev
# 默认端口 8081, 自动打开浏览器
# 访问 http://localhost:8081
```

开发服务器会把 `/api/*` 请求代理到 `http://localhost:9090` (z-mq-broker-admin)。

如需代理到其他地址, 可设置环境变量:

```bash
VITE_ZMQ_ADMIN_URL=http://your-broker-host:9090 npm run dev
```

### 构建生产包

```bash
npm run build
# 输出到 dist/, 默认 ~1.5MB (gzip 后 ~500KB)
```

### 本地预览生产构建

```bash
npm run preview
# 默认端口 8081
```

### 类型检查

```bash
npm run type-check
# vue-tsc --noEmit, 全量类型校验
```

## 后端集成

控制台通过 REST API 与 z-mq-broker-admin 通信. 当前 MVP 在 `src/api/index.ts` 提供 Mock 数据,
后端 REST 接口上线后只需替换为真实实现:

| API | 方法 | 路径 | 说明 |
|-----|------|------|------|
| 集群概览 | GET | `/api/cluster/overview` | Broker/Topic/Consumer 数, TPS 等 |
| Broker 列表 | GET | `/api/cluster/brokers` | Master/Slave / 地址 / TPS |
| Topic 列表 | GET | `/api/topics` | 所有 Topic 配置 |
| 创建 Topic | POST | `/api/topics` | `{ topic, readQueueNums, writeQueueNums }` |
| 删除 Topic | DELETE | `/api/topics/{topic}` | |
| 消息查询 | POST | `/api/messages/query` | `{ topic, queueId, offset, maxNum }` |
| Consumer Group | GET | `/api/consumer/groups` | Group + 订阅 + 积压 |

## 部署

### 静态部署 (Nginx)

```bash
npm run build
# 把 dist/ 上传到 nginx 服务器
```

Nginx 配置示例:

```nginx
server {
  listen 8081;
  server_name _;
  root /opt/zmq-console/dist;
  index index.html;

  # SPA 路由 fallback (Hash 模式不需要, 但 History 模式需要)
  location / {
    try_files $uri $uri/ /index.html;
  }

  # 后端 API 反向代理
  location /api/ {
    proxy_pass http://localhost:9090/;
    proxy_set_header Host $host;
    proxy_set_header X-Real-IP $remote_addr;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
  }
}
```

### Docker 部署 (可选)

```dockerfile
FROM node:20-alpine AS builder
WORKDIR /app
COPY package*.json ./
RUN npm ci
COPY . .
RUN npm run build

FROM nginx:alpine
COPY --from=builder /app/dist /usr/share/nginx/html
COPY nginx.conf /etc/nginx/conf.d/default.conf
EXPOSE 8081
```

## 与后端 z-mq 的关系

```
┌─────────────────┐       ┌─────────────────────┐
│   z-mq-console  │       │  z-mq-broker-admin  │
│   (Vue 3)       │ ─────▶│  (REST API :9090)   │
│   port 8081     │  /api │                     │
└─────────────────┘       └─────────────────────┘
                                    │
                          ┌─────────┼─────────┐
                          ▼         ▼         ▼
                      NameServer  Broker    Consumer
                      :9876       :10911    (Java)
```

## 复用 z-opc 公共规范

- **z-util**: 日期/字符串/集合工具类 (`io.github.yuku123:z-util-core`) - 在 `TopicConfig`、`MessageQueue` 等数据类中已使用
- **z-vector 模式**: API 与实现分离、Page 化存储、统一的 README 规范
- **Git 风格提交图**: 提交信息遵循 `<type>(<scope>): <subject>` 格式

## 版本历史

| 版本 | 日期 | 主要变化 |
|------|------|---------|
| 1.0.0 | 2026-09-08 | 初始版本: 7 个核心页面 + Mock 数据 |

## 维护

- 模块维护人: z-mq-console 团队
- 反馈渠道: GitLab Issues
- 文档: 本 README + 代码内注释