# bitstorm-svr-xtimer 定时微服务

基于 Spring Boot + MySQL + Redis 的分布式定时任务触发服务，对外提供「创建定时器 -> 到点 HTTP 回调业务方」的能力。业务方无需引入任何 SDK，任意语言的服务只要提供一个 HTTP 接口即可接入。

## 1. 设计目标

- **秒级精度**：触发不依赖扫表轮询，而是把 cron 提前展开成执行时机，基于 Redis ZSet 做时间轮式读取。
- **业务零侵入**：通过 HTTP 回调通知业务方，接入方无需引入依赖、无需注册执行器。
- **可水平扩展**：按「分钟 + 分桶（bucket）」切分，多实例通过 Redis 分布式锁抢占分片，加机器即可扩吞吐。
- **不丢任务**：执行时机同时落 MySQL（唯一键 `timer_id + run_timer`）与 Redis，Redis 数据可回源 DB。

## 2. 整体架构

```
                        ┌──────────────────────────────────────────────┐
   业务方 --创建定时器--> │  XtimerWebController / XTimerFeignController  │
                        └───────────────────┬──────────────────────────┘
                                            │ 写 xtimer 表（status=0 新建）
                                            ▼
   ┌─────────────┐   cron 展开未来 2 小时    ┌───────────────────────────┐
   │  Migrator   │ ───────────────────────> │ timer_task 表 + Redis ZSet │
   │ (@Scheduled)│                          └───────────────────────────┘
   └─────────────┘
                              ┌─────────────────────────────────────────┐
   ┌─────────────┐  每秒扫描   │ 抢分布式锁（分钟+bucket）后处理该分片     │
   │  Scheduler  │ ─────────> │ time_bucket_lock_{yyyy-MM-dd HH:mm}_{i} │
   │ (@Scheduled)│            └─────────────────────────────────────────┘
   └─────────────┘                            │
                                              ▼
                                   ┌────────────────────┐  每秒 zrangeByScore
                                   │   TriggerWorker    │ ─────────────┐
                                   └────────────────────┘              │
                                                                       ▼
                                   ┌────────────────────┐  线程池异步派发
                                   │   ExecutorWorker   │ <── TriggerPoolTask
                                   └─────────┬──────────┘
                                             │ POST 回调业务方 + 更新任务状态
                                             ▼
                                        业务方 HTTP 接口
```

核心模块划分：

| 模块 | 职责 |
| --- | --- |
| `service/migrator/MigratorWorker` | 定时把激活状态定时器的 cron 展开成具体执行时机，写入 `timer_task` 与 Redis ZSet |
| `service/scheduler/SchedulerWorker` | 每秒遍历所有 bucket，处理「当前分钟」与「上一分钟」两个批次 |
| `service/scheduler/SchedulerTask` | 对 `(分钟, bucketId)` 抢分布式锁，抢到后调用 Trigger |
| `service/trigger/TriggerWorker` | 逐秒推进时间窗口，捞取该秒内待触发的任务 |
| `service/trigger/TriggerTimerTask` | 按 `zrangeGapSeconds` 分段查询并派发任务 |
| `service/trigger/TriggerPoolTask` | 通过 `triggerPool` 线程池异步执行 |
| `service/executor/ExecutorWorker` | 查库校验、HTTP 回调、回写执行结果 |

## 3. 运行链路细节

### 3.1 迁移（Migrator）

1. `@Scheduled(fixedRate = 10s)` 触发，先抢 `migrator_lock_{yyyy-MM-dd HH}`，同一小时只会有一个实例执行。
2. 查询所有 `status = Enable` 的定时器，用 Quartz `CronExpression` 展开 `[now, now + 2 * migrateStepMinutes]` 之间的执行时机。
3. 执行时机批量写入 `timer_task`（唯一键去重，重复迁移不会产生重复任务）。
4. 同时写入 Redis ZSet：key 为 `{yyyy-MM-dd HH:mm}_{timerId % bucketsNum}`，member 为 `{timerId}_{runTimer}`，score 为 `runTimer`（毫秒时间戳）。

### 3.2 调度（Scheduler）

`SchedulerWorker` 每秒执行一次，对每个 bucket 提交两个异步任务：**当前分钟**和**上一分钟**。

重复执行上一分钟是为了兜底重试：分片执行成功后会把锁的过期时间延长（`successExpireSeconds`），下次「重复上一分钟」就抢不到锁，避免重复触发；若上一轮执行失败，锁未被延长，则可以重试。锁本身只加不解，靠过期时间控制频率。

### 3.3 触发（Trigger）

`TriggerWorker.work` 以 1 秒为步长（`trigger.zrangeGapSeconds`）推进窗口，直到覆盖完整个分钟。每个窗口从 ZSet 里 `rangeByScore(start, end - 1)` 取出任务，交给 `triggerPool` 线程池异步执行。

读取时优先走 Redis；Redis 抛异常时回退到 `timer_task` 表按 `run_timer` 区间 + `status = NotRun` 查询，作为缓存不可用时的兜底。

### 3.4 执行（Executor）

1. 按 `timerId + runTimer` 查 `timer_task`，若状态不是「待执行」则直接返回，避免重复回调。
2. 查定时器，若非激活状态（用户已关闭）则跳过。
3. 通过 `RestTemplate` 按配置的 `method / url / header / body` 回调业务方。
4. 回写任务状态（成功 / 失败）、响应内容与耗时。

## 4. 数据模型

`xtimer`（定时器定义）

| 字段 | 说明 |
| --- | --- |
| `timer_id` | 主键 |
| `app` | 所属业务方标识，用于多租户隔离 |
| `name` | 定时器名称 |
| `status` | 0 新建，1 激活，2 未激活 |
| `cron` | cron 表达式 |
| `notify_http_param` | 回调配置 JSON（method / url / header / body） |

`timer_task`（执行时机，即「任务实例」）

| 字段 | 说明 |
| --- | --- |
| `task_id` | 主键 |
| `timer_id` | 所属定时器 |
| `run_timer` | 计划执行时间（毫秒时间戳） |
| `status` | 执行状态（NotRun / Succeed / Failed，见 `TaskStatus`） |
| `output` | 回调响应内容或失败原因 |
| `cost_time` | 实际触发与计划时间的偏差 |

唯一键 `idx_timer_id_run_timer (timer_id, run_timer)` 保证同一时刻的任务只会存在一条。

建表脚本：[`db/bitstorm-svr-xtimer.sql`](db/bitstorm-svr-xtimer.sql)

## 5. Redis Key 一览

| Key | 类型 | 用途 |
| --- | --- | --- |
| `{yyyy-MM-dd HH:mm}_{bucketId}` | ZSet | 时间轮，member 为 `timerId_unix`，score 为执行时间戳 |
| `time_bucket_lock_{yyyy-MM-dd HH:mm}_{bucketId}` | String | 分片调度锁，防止多实例重复触发同一分钟分片 |
| `migrator_lock_{yyyy-MM-dd HH}` | String | 迁移锁，保证同一小时只有一个实例执行迁移 |
| `enable_timer_lock_{app}` | String | 激活/停用的操作限频锁 |
| `create_timer_lock_{app}` | String | 创建/删除的操作限频锁 |

## 6. 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/xtimer/createTimer` | 创建定时器，返回 `timerId` |
| GET | `/xtimer/enableTimer?app=&timerId=` | 激活定时器（激活后会立即迁移执行时机） |
| POST | `/xtimer/callback` | 本地回调测试接口，打印收到的回调内容 |

同时通过 Feign 客户端 `XTimerClient` 暴露同名能力，供其他微服务调用。

请求示例：

```json
POST /xtimer/createTimer
{
  "app": "demo-app",
  "name": "每分钟回调一次",
  "cron": "0 * * * * ?",
  "notifyHTTPParam": {
    "method": "POST",
    "url": "http://127.0.0.1:8082/xtimer/callback",
    "header": {},
    "body": "{\"bizId\":1}"
  }
}
```

## 7. 配置说明

```yaml
scheduler:
  bucketsNum: 5            # 横向分桶数，决定并行粒度
  tryLockSeconds: 70       # 分片锁过期时间，需大于一分钟
  successExpireSeconds: 130 # 成功后延长锁时间，用于避免上一分钟被重复执行
  pool: { corePoolSize: 10, maxPoolSize: 100, queueCapacity: 99999 }

trigger:
  zrangeGapSeconds: 1      # 每次从 ZSet 取多少秒的任务
  pool: { corePoolSize: 10, maxPoolSize: 100, queueCapacity: 99999 }

migrator:
  migrateStepMinutes: 60   # 迁移步长，实际展开未来 2 倍时长
  migrateTryLockMinutes: 1 # 迁移锁过期时间（分钟）
```

线程池见 `common/pool/AsyncPool`：`schedulerPool` 负责分片调度，`triggerPool` 负责任务派发，均使用 `CallerRunsPolicy`。

## 8. 本地启动

1. 导入建表脚本：`db/bitstorm-svr-xtimer.sql`
2. 启动 MySQL、Redis、Nacos（默认 `127.0.0.1:8848`）
3. 按实际情况修改 `src/main/resources/application.yml` 中的数据源、Redis、Nacos 连接信息
4. 启动 `XTimerApplication`（默认端口 `8082`）

## 9. 已知限制

- **Redis 是调度链路的强依赖**：调度入口需要先获取分布式锁，Redis 不可用时锁操作会抛异常，导致新任务无法触发；已在执行的任务因只依赖 MySQL 与 HTTP，可以正常执行完。
- **执行侧防重依赖状态判断**：当前是「查状态 -> 执行 -> 更新状态」，在高并发或多实例重复触发的极端场景下存在并发窗口，可通过状态 CAS 更新加固。
- **功能面偏薄**：暂未提供失败重试策略、告警、任务查询/删除的 HTTP 入口与可视化控制台。
