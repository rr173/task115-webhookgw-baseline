# task115-webhookgw

Webhook 投递网关。为命名事件（如 `order.created`）维护订阅端点，按事件类型匹配订阅，并通过带 HMAC 签名的 HTTP POST 将事件投递到各端点；内置指数退避重试、每端点限流、失败死信队列与重启恢复。所有状态（订阅、事件、投递尝试、死信）持久化到 SQLite。

## 主要输入与输出

- 输入：HTTP JSON 请求（`/subscriptions`、`/events`、`/attempts`、`/deadletters` 等），含 `name`、`endpoint`、`events`、`payload` 等字段。
- 输出：JSON 响应（订阅对象、事件回执、投递尝试列表、死信队列、按状态聚合的统计指标等）。投递请求头携带 `X-Webhook-Signature`（HMAC-SHA256）、`X-Webhook-Event`、`X-Webhook-Attempt`。

## 本地命令

```bash
go build ./...        # 编译
go run . --smoke-test # 自检（不依赖外部服务、不依赖真实时间睡眠）
go run .              # 启动 HTTP 服务（默认 :8080，SQLite 文件 webhookgw.db）
go test ./...         # 测试
```

## Docker 构建

构建脚本 `build_benzhi_docker.sh` 接收两个参数：

1. 镜像名（默认 `my-project`）
2. 目标平台（默认 `linux/amd64`）

```bash
# amd64
bash ./build_benzhi_docker.sh go-task-benzhi:amd64 linux/amd64
docker run -it go-task-benzhi:amd64
# arm64
bash ./build_benzhi_docker.sh go-task-benzhi:arm64 linux/arm64
docker run -it go-task-benzhi:arm64
```

进入容器后可用 `go version` 确认工具链版本为 `go1.26.3`。

## 双架构主镜像

主 `Dockerfile` 为多阶段构建（`golang:1.26.3-bookworm` 构建 + `alpine:3.20` 运行，`CGO_ENABLED=0`）：

```bash
docker buildx build --platform linux/amd64 --load -t go-task-check:amd64 .
docker run --rm go-task-check:amd64 --smoke-test
docker buildx build --platform linux/arm64 --load -t go-task-check:arm64 .
docker run --rm go-task-check:arm64 --smoke-test
```

## 技术栈

- Go `1.26.3`（`GOTOOLCHAIN=local`）
- SQLite 引擎 `3.46.1`，纯 Go 驱动 `modernc.org/sqlite v1.35.0`（`CGO_ENABLED=0`）
- 依赖下载：`GOPROXY=https://goproxy.cn,direct`、`GOSUMDB=sum.golang.google.cn`
