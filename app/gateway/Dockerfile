
# 使用轻量级 alpine 基础镜像
FROM alpine:latest

# 安装必要依赖（如 CA 证书、时区）
RUN apk --no-cache add ca-certificates tzdata

# 创建非 root 用户（安全最佳实践）
RUN adduser -D -s /bin/sh appuser
USER appuser

WORKDIR /app

# 从构建上下文复制二进制（GoReleaser 会自动放在这里）
COPY linux/amd64/server .
EXPOSE 8080
EXPOSE 7070
# 启动命令
ENTRYPOINT ["./server"]