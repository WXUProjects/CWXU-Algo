# GoAlgo Backend

[GoAlgo](https://algo.zhiyuansofts.cn) 后端：校队训练数据与管理平台。

前身为**无锡学院算法协会内部监测平台**，在[刘伟教授](https://wlwxy.cwxu.edu.cn/info/1226/1915.htm)指导下，由[伞恩晨](https://github.com/srcenchen)、[宗柏屹](https://github.com/AoralsFout)、[张万里](https://github.com/hyhgfrgh/)设计实现，后扩展为面向多校队的 **GoAlgo**。

> 线上入口：https://algo.zhiyuansofts.cn  
> 关于我们：https://algo.zhiyuansofts.cn/about  
> 交流群 QQ：`925338346`

## 产品定位

汇总多 OJ 提交与比赛，服务个人进度与队内管理（**不是又一个 OJ**）：

| 能力 | 说明 |
|------|------|
| 能力分析 | 按题型、知识点、难度汇总熟悉程度 |
| 多平台同步 | Codeforces、AtCoder、洛谷、牛客、LeetCode、QOJ 等 |
| 多组织隔离 | 默认公共域；校队独立成员、分组、公告与后台 |
| 角色权限 | 站点管理员 / 组织管理员 / 教练 / 队长 / 成员 |
| 题库与提交 | 浏览、筛选、题面、提交历史与复盘 |
| 比赛与动态 | 比赛列表、站内榜、全站/队内提交动态 |
| 日报周报 | 按日/周汇总训练情况（含 AI 总结） |

院校合作可开通独立组织空间（品牌、识别码加入、同步策略等）。联系：微信 `srcenchen` · 邮箱 `srcenchen@gmail.com`。

## 架构概览

```
浏览器  ──/api/{user|core|agent}/*──►  Nginx
                                        │
                                        ▼
                                   Gateway (HTTP 边缘)
                              JWT · CORS · 熔断 · 路由
                           ┌────────────┼────────────┐
                           ▼            ▼            ▼
                         User       Core Data      Agent
                      认证/组织/角色  提交/爬虫/统计   AI 总结
                           │            │            │
                     PostgreSQL ◄───────┴────────────┘
                     Redis · RabbitMQ · Consul
```

### 微服务

| 服务 | 目录 | 职责 |
|------|------|------|
| **gateway** | `app/gateway` | HTTP 网关：路由、JWT、CORS、熔断、代理 |
| **user** | `app/user` | 认证、资料、分组、角色、组织/租户、博客权限 |
| **core_data** | `app/core_data` | 提交日志、OJ 爬虫、统计/热力图、比赛、公告、题库流水线、定时任务 |
| **agent** | `app/agent` | AI 训练总结 / 日报周报（基于 core 数据） |
| **common** | `app/common` | 共享：配置、DB/Redis、发现、事件、鉴权、邮件、限流、指标 |

### 对外 API

| 前缀 | 服务 |
|------|------|
| `/api/user/*` | user |
| `/api/core/*` | core_data |
| `/api/agent/*` | agent |

网关内部为 `/v1/{user|core|agent}/*`，线上由 Nginx 将 `/api` 映射到网关。

- 鉴权：`Authorization: Bearer <jwtToken>`
- 密码：客户端 SHA256 后传输
- 契约文档（与前端共享）：仓库根 `shared/api.md`、`shared/api.ts`
- 线上：`https://algo.zhiyuansofts.cn/api/{user|core|agent}/`

### 技术栈

| 层级 | 选型 |
|------|------|
| 语言 | Go 1.25+ |
| 框架 | [go-kratos/kratos](https://go-kratos.dev) v2 |
| 数据库 | PostgreSQL + GORM |
| 缓存 | Redis |
| 消息队列 | RabbitMQ |
| 服务发现 | Consul |
| 依赖注入 | Google Wire |
| API | Protobuf / gRPC + HTTP |
| 鉴权 | JWT |
| 定时任务 | robfig/cron |
| AI | OpenAI 兼容 / 火山引擎 SDK |

### 目录结构

```
cwxu-algo/
├── api/                 # Protobuf 定义（agent / core / user）
├── app/
│   ├── common/          # 跨服务共享
│   ├── gateway/
│   ├── user/
│   ├── core_data/
│   │   └── internal/spider/   # OJ 爬虫插件
│   └── agent/
├── bin/                 # make build 产物
├── scripts/
├── third_party/
├── Makefile
└── openapi.yaml
```

各服务遵循 Kratos 标准分层：

```
cmd/          # 入口 + wire
internal/
  biz/        # 业务用例
  data/       # 模型与数据访问
  server/     # HTTP / gRPC 装配
  service/    # 接口实现
configs/      # config.yaml
```

### 爬虫与事件流

1. 业务侧向 RabbitMQ 队列 `spider` 投递拉取事件  
2. `core_data` Consumer → `SpiderUseCase.LoadData()`  
3. 按平台插件（`SubmitLogFetcher` + `Provider`，`spider.Register()`）抓取  
4. UPSERT 写入提交表，并按模式失效 Redis 缓存  

已接入平台示例：NowCoder、AtCoder、Codeforces、LuoGu、LeetCode、QOJ。新增平台：在 `app/core_data/internal/spider/platform/` 实现接口并 `Register()`。

### 多租户（GoAlgo）

- 账号全局唯一；默认组织 **公共域**（不可退出）
- 单域名多租户；识别码加入校队；站点管理员 vs 组织角色可叠加  
- 规格见 monorepo：`Business/goalgo-framework.md`

## 本地开发

```bash
# 一次性工具链
make init

# 生成 API / 配置 / Wire
make all
# 或分步：make api && make config && make generate

# 编译全部服务到 ./bin/
make build
```

各服务配置见 `app/<service>/configs/config.yaml`（HTTP/gRPC、Consul、AMQP、PostgreSQL、Redis 等）。**勿提交生产密钥。**

改 API 流程：

1. 修改 `api/**/*.proto`
2. `make api`
3. 同步 monorepo 的 `shared/api.md` 与 `shared/api.ts`
4. 对真实环境做一次请求验证

Wire：改 `cmd/*/wire.go` 后执行 `make generate`。

## 相关仓库与前端

| 组件 | 说明 |
|------|------|
| 本仓库 | Go 微服务后端 |
| 新前端 | monorepo `newUI-20260715`（React 19 + Vite + TypeScript + shadcn） |
| 旧前端 | 已弃用，勿再维护 |
| 产品/商业化 | monorepo `Business/` |

## 致谢

感谢刘伟教授的指导，以及无锡学院算法协会同学的使用与反馈。

## License

见 [LICENSE](./LICENSE)。
