# task115-webhookgw

Webhook 投递网关（Go Bugfix 标注训练题基线）。

订阅事件 → 匹配 → HMAC 签名 HTTP 投递 → 退避重试 → 死信队列 → 重启恢复，状态持久化于 SQLite。

本地运行：

```bash
go run . --smoke-test   # 自检
go run .               # 启动服务（:8080）
go test ./...          # 测试
```
