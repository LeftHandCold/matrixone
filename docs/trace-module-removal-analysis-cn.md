# MatrixOne trace 模块性能与移除可行性评审

> 评审日期：2026-07-17
> 代码基线：`main@6b1ad0c32b2463ed8ffaabcd3999a17e4dd64c1d`
> 评审范围：`pkg/util/trace`、`pkg/util/trace/impl/motrace` 及其直接生产依赖
> 不在本评审范围：`pkg/txn/trace`、Go `runtime/trace`

## 1. 执行摘要

本次 profile 确实暴露了一个真实且严重的高并发热点：
`trace.IsMOCtledSpan` 使用全局独占 `sync.Mutex` 保护一个几乎只读、仅含 4
类受控 Span 的状态表。每次 `MOTracer.Start` 都会读取该表，真实 Span 在
`End -> NeedRecord` 时还会再次读取。短查询、高并发和多层 Span 嵌套会放大这把锁的竞争。

但是，现有材料不能支持“trace 占用 50%～60% CPU”或“删除 trace 后可直接回收
50%～60% 性能”的结论：

1. 图 1 是 mutex delay 视角，小时数是多个 goroutine 的累计等待时间，不是 CPU 时间，
   也不是请求墙钟时间。
2. 图 2、图 3 是 CPU profile，trace 并未表现为主要 CPU flat hotspot。
3. 图 1 中 frontend/compile/txn 函数是 `trace.Start` 的调用方，不能据此认定存在另一把
   “frontend 分布锁”。
4. 图 1 显示的 `66.2%` 更像是 `MOTracer.Start` 子路径；右侧
   `MOSpan.End -> NeedRecord` 也落在同一个 `IsMOCtledSpan` 锁上。因此 trace 锁在
   `sync.Mutex.Unlock` 应用层分支中的实际占比可能高于图 4 标注的 53%～60%。

最终结论：

- **图 4 的热点方向正确，但 profile 类型和调用栈归因不够准确。**
- **当前不能直接删除整个 trace/motrace 模块。**它同时承载 Span、SQL Statement、
  日志、错误、CU/执行统计、系统表和 RPC 上下文关联。
- **代码默认值应改为 `DisableSpan=true`，而不是要求每个环境修改配置文件。**正式启动
  入口通过默认构造器自动关闭 Span，使 `trace.Start` 走 Noop 快路径；同时保持
  `DisableTrace=false`、`DisableError=false`，继续保留 Statement、日志和错误 exporter。
- **无锁改造不是本次默认关闭 Span 的上线前置条件。**只有未来需要在生产重新启用 Span，
  或诊断 opt-in 场景也必须承受高并发负载时，才需要将受控 Span 状态改为原子不可变快照。
- 如果产品最终确认不需要 Span，应该先拆分 `motrace` 中的 Statement/Statistic/Exporter，
  保留 Noop API 和 RPC wire compatibility，再分阶段退役 Span；不应直接删除目录。

本次需求的能力优先级是：StatementInfo、日志和错误必须保留；CU exporter 是否保留不影响
本次决策。由于 CU 当前与 StatementInfo 共用统计和上报链路，默认关闭 Span 时继续保留它
成本更低、风险更小，没有必要为了本次优化额外拆除。

## 2. 证据范围与限制

### 2.1 输入材料

本评审结合以下信息：

- 图 1：以 `sync.(*Mutex).Unlock` 为底部热点、包含
  `trace.IsMOCtledSpan`、`MOTracer.Start`、`MOSpan.End/NeedRecord` 的火焰图；
  可见选中分支 `274.69 hrs (66.2%)`。
- 图 2：总量约 `1602.14s` 的 CPU flame graph。
- 图 3：总量约 `1606.8s` 的 CPU flame graph。
- 图 4：point_select、read_only、insert 三种 workload 的汇总结论。
- 当前目录 MatrixOne main 源码。

### 2.2 当前无法完全验证的部分

当前没有三组 workload 的原始 pprof 文件、采集参数、build ID、压测持续时间、并发度和
完成请求数。因此：

- 可以从图 1 和源码确认具体锁及其调用路径；
- 不能仅从截图精确复算图 4 中每一行的 83.6%/75.9%/70.0% 等数字；
- 不能把 mutex delay 份额换算成 QPS、p99 或 CPU 改善比例；
- 不能确认截图二进制和当前 `main@6b1ad0c32b` 完全一致；
- 不能确认三个 workload 是否使用相同 mutex profile rate、持续时间和并发度。

后续性能决策必须保留并使用原始 profile，而不是只依据截图。

## 3. 对图 4 的重新评审

### 3.1 mutex profile 与 CPU profile 不能混用

Go mutex profile 默认关注锁竞争造成的累计 delay。一个 goroutine 等待 1 秒，100 个
goroutine 同时等待，可以累计出约 100 秒样本。因此图中的 `274.69 hrs` 可以远大于
压测实际运行时间。

mutex profile 中：

- `sync.(*Mutex).Unlock` 是竞争样本的归因点，不表示 Unlock 指令自身执行了几百小时；
- 百分比表示该调用栈占 mutex delay 样本的比例；
- 未发生锁等待的 CPU、I/O、执行和规划时间不在这个分母中；
- 不同锁的等待可能并发重叠，不能简单相加后从请求延迟中减去。

图 2、图 3 才是 CPU 视角。图中 frontend 顶层调用链很宽，是因为 SQL 的规划、执行、
扫描、输出等工作都从 frontend 入口进入，属于 cumulative time；它不等于 frontend
函数自身的 flat CPU。

### 3.2 图 1 的正确调用栈归因

源码与图 1 对应的主调用路径为：

```text
frontend / compile / txn 请求路径
  -> trace.Start
    -> motrace.(*MOTracer).Start
      -> trace.IsMOCtledSpan
        -> sync.(*Mutex).Unlock
```

结束路径为：

```text
motrace.(*MOSpan).End
  -> motrace.(*MOSpan).NeedRecord
    -> trace.IsMOCtledSpan
      -> sync.(*Mutex).Unlock
```

对应源码：

- `pkg/util/trace/impl/motrace/mo_trace.go:73`：`MOTracer.Start`
- `pkg/util/trace/impl/motrace/mo_trace.go:89`：Start 阶段查询受控 Span 状态
- `pkg/util/trace/impl/motrace/mo_trace.go:320`：`MOSpan.End`
- `pkg/util/trace/impl/motrace/mo_trace.go:398`：`NeedRecord`
- `pkg/util/trace/config.go:209`：`IsMOCtledSpan`

因此图中 `commitUnsafe`、`doComQuery`、`executeStmt`、`Compile.Run` 等函数说明“哪些
请求路径频繁触发了 trace 锁”，不说明这些函数持有另一把 frontend 锁。

如果图 4 的“frontend 分布锁 17%～23%”来自这些上游函数宽度，那么这是调用方归因
错误。只有原始 profile 中存在另一条不经过 `trace.IsMOCtledSpan`、能够落到具体
frontend mutex 字段的 Unlock 栈，才能单独认定 frontend 锁。

### 3.3 对图 4 各列的修订建议

| 图 4 表述 | 评审结果 | 建议改写 |
|---|---|---|
| `sync.Mutex.Unlock` 70%～83.6% | 数字可能正确，但只属于 mutex delay 分母 | 应写为“应用 `sync.Mutex` 归因的 mutex delay 样本份额” |
| `runtime.unlock` 16%～28.9% | 可能是 runtime 内部锁等待份额 | 不应与 CPU 占比混用 |
| “应用层主导” | 结论过宽 | 改为“已采样 mutex delay 中应用层锁主导” |
| “trace 锁 53%～60%” | 方向正确，但可能低估 | 分别统计 Start、End/NeedRecord 后再合并为同一把锁 |
| “frontend 分布锁 17%～23%” | 当前截图不支持 | 用具体 mutex 字段和 Unlock 栈证明，否则归入 trace 调用方 |

推荐的最终表述是：

> 三类 workload 的 mutex profile 均显示应用层 mutex delay 占主要部分，其中
> `trace.IsMOCtledSpan` 的全局状态锁是明确且跨 workload 一致的热点。该热点会影响
> 高并发下的吞吐和尾延迟，但仅凭 mutex profile 不能推导 CPU 或端到端性能提升比例。

## 4. 源码根因分析

### 4.1 高读低写状态使用全局独占锁

`pkg/util/trace/config.go:178` 定义：

```go
var MOCtledSpanEnableConfig struct {
    sync.Mutex
    NameToKind  map[string]SpanKind
    KindToState map[SpanKind]*MoCtledState
}
```

受控状态只有 4 类：

- `s3 -> SpanKindRemoteFSVis`
- `local -> SpanKindLocalFSVis`
- `statement -> SpanKindStatement`
- `tnrpc -> SpanKindTNRPCHandle`

状态更新来自 `mo_ctl TraceSpan`，属于极低频控制面操作；状态读取发生在请求热路径，
属于极高频数据面操作。当前实现却让所有读取使用独占 `Lock()`。

这会产生三个问题：

1. 所有请求 goroutine 在同一 cache line 和同一 mutex 上串行竞争；
2. 默认 `SpanKindInternal` 不在受控表中，但每次 Start/End 仍需加锁完成一次 map miss；
3. 受控但默认关闭的 Statement/S3/Local/TNRPC Span 仍要先进入热路径加锁，才能知道
   自己应该返回 `NoopSpan`。

简单改成 `RWMutex` 只能缓解，不能消除读路径上的原子 cache-line 竞争。由于状态集合小、
写入极少，正确模型应是“写时复制、读时无锁”的不可变快照，或者按 Kind 使用原子状态。

### 4.2 Start 热路径存在可避免的工作

当前 `MOTracer.Start` 顺序为：

1. `provider.IsEnable()`，内部取得 `tracerProviderConfig.mux.RLock`；
2. 从 pool 获取 `MOSpan`；
3. 初始化名称、时间和全部 options；
4. 调用 `IsMOCtledSpan` 获取全局 Mutex；
5. 如果受控 Kind 关闭，再释放 Span 并返回 Noop；
6. 否则生成 ID、创建带 Span 的 context。

`trace.WithKind` 已返回零分配 `KindOption`，见 `pkg/util/trace/config.go:391`。因此可以在
分配 `MOSpan` 前扫描 opts，仅提取 Kind 并完成无锁状态判断。受控且关闭时应直接返回
`ctx, NoopSpan{}`。

### 4.3 End 阶段重复读取同一状态

真实 `MOSpan.End` 会调用 `NeedRecord`，后者再次查询 `IsMOCtledSpan`。因此普通
未受控 Span 通常至少造成两次全局锁访问：Start 一次、End 一次。

如果控制状态要求 Span 生命周期内保持一致，可以在 Start 时把受控状态/阈值保存进
Span，End 不再读取全局状态。如果现有语义要求 mo_ctl 在 Span 执行中途改变状态并影响
End，则仍可在 End 读取一次原子快照。无论哪种语义，都不应使用全局 Mutex 读锁。

该语义需要产品和测试明确，不能在实现时默默改变。

### 4.4 `NeedRecord` 的 Duration 顺序问题

当前实现先处理 mo_ctl 受控分支：

```go
if has, state, threshold := trace.IsMOCtledSpan(s.Kind); has {
    return state && (s.Duration >= threshold), nil
}

s.Duration = s.EndTime.Sub(s.StartTime)
```

生产 `End()` 在调用 `NeedRecord()` 前只设置 `EndTime`，没有设置 `Duration`。因此
受控 Span 使用正 threshold 时可能用零值 Duration 判断，导致永不记录。

现有 `TestMOSpan_NeedRecord` 在正 threshold 场景中手动设置 `Duration` 后再调用
`NeedRecord`，没有覆盖真实 `End()` 顺序。修复热锁时应同时：

1. 在所有分支前计算 `Duration = EndTime - StartTime`；
2. 添加调用真实 `End()` 的正阈值测试；
3. 使用 mock SpanProcessor 验证是否真正 OnEnd，而不只直接调用 `NeedRecord()`。

### 4.5 provider enable 仍有一把高频 RWMutex

`tracerProviderConfig.IsEnable()` 每次使用 `RLock`，而 enable 只在初始化和 Shutdown
阶段改变。这也符合 `atomic.Bool` 模型。修复 `IsMOCtledSpan` 后，应重新采集 profile，
确认 provider RWMutex 是否成为下一个热点。

## 5. 当前 MO 对 trace/motrace 的真实依赖

基于当前非测试、非 example Go 源码静态统计：

- 58 个文件直接导入 `pkg/util/trace`，分布在 23 个源码目录；
- 32 个文件包含约 66 个 `trace.Start` 调用点；
- 10 个文件直接导入 `pkg/util/trace/impl/motrace`；
- 30 个文件直接导入 `pkg/util/trace/impl/motrace/statistic`。

因此“MO 没有使用这个模块”与源码和 profile 均不一致。更准确的说法可能是：

> 产品当前未充分消费 `span_info`，或者默认阈值/控制开关导致 Span 实际落库较少；
> 但 Span API、状态检查和 motrace 其他能力仍在请求和启动热路径中运行。

### 5.1 启动与配置

调整后的代码默认值为：

- `disable-trace` 默认 `false`；
- `disable-span` 默认 `true`；
- `disable-error` 默认 `false`。

`cmd/mo-service/main.go:469` 的 `initTraceMetric` 初始化 exporter、collector 和 motrace。
`pkg/util/trace/impl/motrace/trace.go:59` 将两个开关独立应用：

- `EnableTracer(!DisableTrace)` 控制整个 motrace provider；
- `WithSpanDisable(DisableSpan)` 只控制是否安装真实 Span tracer。

### 5.2 motrace 不等于 Span

`motrace.Init` 和 exporter 同时负责：

| 能力 | 主要代码 | 关闭/删除影响 |
|---|---|---|
| Span | `mo_trace.go` | `span_info`、TraceID/SpanID、慢/挂 Span profile |
| SQL Statement | `report_statement.go` | `system.statement_info`、SQL 状态、耗时、计划、流量 |
| 执行统计/CU | `statistic/`、`cu.go` | StatsArray、CU 计算、Statement/CU metrics |
| 日志导出 | `report_log.go` | `system.rawlog`、`system.log_info`，本地普通日志本身不一定停止 |
| 错误导出 | `report_error.go` | `system.error_info`，错误普通日志仍可输出 |
| 系统表/视图 | `schema.go` | statement/rawlog/span/log/error/hotspot 表和视图 |
| RPC 关联 | `pkg/common/morpc/codec_header.go` | SpanContext wire header、跨服务关联和滚动升级兼容 |
| 动态控制 | `cmd_tracespan.go` | `mo_ctl TraceSpan` |

### 5.3 StatementInfo 是运行中的生产功能

`pkg/frontend/mysql_cmd_executor.go:209` 在 provider 启用时为 SQL 创建
`motrace.StatementInfo`。`report_statement.go:672` 在语句结束时：

- 记录成功/失败状态；
- 计算 duration、结果行数、网络流量；
- 提取执行计划统计；
- 更新 statement counter/duration；
- 更新 CU counter；
- 将记录送入全局 BatchProcessor。

因此设置 `disable-trace=true` 或删除 motrace，不只是“没有 Span”，还会停止上述
StatementInfo 采集和其中的部分 metrics 更新。

### 5.4 系统表和产品兼容依赖

`pkg/util/trace/impl/motrace/schema.go` 定义并注册：

- `system.statement_info`
- `system.rawlog`
- `system.log_info`
- `system.error_info`
- `system.span_info`
- `system.sql_statement_hotspot`

bootstrap 版本升级、租户 schema、权限规则、SQL planner 特判和 distributed tests 均引用
这些对象。即使不再写新数据，也不能未经迁移直接删除表定义或历史数据。

### 5.5 RPC wire compatibility

`pkg/common/morpc/codec_header.go:59` 在 RPC header 中编码固定大小的 SpanContext，并在
接收端恢复到 context。直接移除 codec 或改变 header 布局会影响滚动升级期间新旧节点
互通。

如果最终停用 Span，应保留 wire slot 并发送空 SpanContext，直到协议版本完成显式迁移。

## 6. 五种选择的影响矩阵

| 方案 | 性能预期 | 功能保留 | 实施风险 | 建议 |
|---|---|---|---|---|
| A. 保持现状 | 高并发继续受全局锁影响 | 全部保留 | 性能风险高 | 不建议长期保持 |
| B. 代码默认 `DisableSpan=true` | 无需修改部署配置，直接绕过 Span 热锁 | Statement、日志、错误保留；CU 随现有 Statement 链路保留 | 丢失 Span/关联/profile，升级需重启 | **推荐作为新默认** |
| C. Span 保持启用，修成无锁快照 | 消除明确锁热点，保留大部分能力 | 全部保留 | 需处理并发语义和测试 | **推荐长期方案** |
| D. `disable-trace=true` | 同时消除 Span 和 motrace exporter 开销 | 普通日志仍可能输出，但 Statement/DB 日志/错误/CU 采集停止 | 生产可观测性风险高 | 只作隔离环境上界实验 |
| E. 删除整个目录 | 编译期消除代码，但需要大规模重构 | 大量能力需要迁移或消失 | 极高，含协议和 schema 风险 | 当前不建议 |

### 6.1 代码默认关闭 Span 的精确边界

默认行为由 `config.NewObservabilityParameters()` 在代码中提供：

```go
DisableTrace: false,
DisableSpan:  true,
DisableError: false,
```

因此未提供 observability 配置、使用动态生成配置、或配置文件中没有 `disable-span` 的环境，
都不需要增加或修改配置文件。`mo-service` 和 embed 正式启动入口都会先创建带代码默认值的
配置对象，再将外部 TOML 覆盖到该对象上。

为诊断临时重新启用 Span 时，仍可显式 opt-in：

```toml
[observability]
disable-span = false
```

在 `mo-service -launch` 单进程同时启动 Log/TN/CN 的模式下，motrace 是进程级单例，
第一个启动的服务（当前 launch 顺序通常是 Log）决定该进程的 tracer。诊断 opt-in 必须让
首个服务的有效配置包含 `disable-span=false`；只修改后启动的 CN 配置不会重新初始化 tracer。
独立进程部署则应在需要启用 Span 的相应进程上显式设置。这个注意事项只影响诊断 opt-in，
不影响无配置时默认关闭 Span。

旧版 camelCase `disableSpan=false` 无法表达“覆盖新的 true 默认值”，因为旧 bool 字段不能
区分“未配置”和“显式 false”。需要临时启用 Span 的环境应使用当前 kebab-case 字段。

该配置下：

- `trace.DefaultTracer` 保持 `NoopTracer`；
- `trace.Start` 不创建 `MOSpan`、不访问 `IsMOCtledSpan`；
- provider 仍然 enable，`initExporter` 仍注册并启动 Statement/Log/Error collector；
- `StatementInfo` 仍被创建和上报；
- `span_info` 不再产生新 Span；
- 请求级 TraceID/SpanID 关联会减少或消失；
- 基于 Hung/Long Span 的自动 profile 不再触发；
- `mo_ctl TraceSpan` 对业务 Span 不再产生实际效果。

默认值和显式 opt-in 都在启动初始化时生效，切换需要重启相应服务。

### 6.2 `disable-trace=true` 的精确边界

该配置将 provider 设置为 disabled，`initExporter` 直接返回。frontend 创建
StatementInfo 前也会因 `GetTracerProvider().IsEnable()==false` 返回；日志/错误 reporter
在进入 BatchProcessor 前同样会退出。

因此它适合用来测量“整个 motrace/observability pipeline 的性能上界”，不适合作为未经
产品确认的生产修复。

## 7. 现有微基准结果与边界

在 `darwin/arm64`、Apple M2 Pro、当前代码基线上运行现有 benchmark：

| Benchmark | 结果 | 分配 |
|---|---:|---:|
| `NoopTracer.Start` | 约 `0.295 ns/op` | `0 B/op, 0 allocs/op` |
| `NoopTracer.Debug` | 约 `0.295 ns/op` | `0 B/op, 0 allocs/op` |
| `MOTracer Start+End`，无 opts | 约 `237 ns/op` | `384 B/op, 3 allocs/op` |
| `MOTracer Start+End`，受控 opts | 约 `416 ns/op` | `837 B/op, 8 allocs/op` |

命令：

```bash
.agents/skills/mo-dev/scripts/mo-cgo-test \
  -run '^$' \
  -bench 'Benchmark(NoopTracer_(Start|Debug)|MOTracer_WithOpts_vs_WithoutOpts)$' \
  -benchmem -benchtime=1s -count=3 \
  ./pkg/util/trace ./pkg/util/trace/impl/motrace
```

这些结果只能用于验证 Noop 快路径和启用态的基础成本，不能直接预测 SQL 提升：

- Noop concrete call 很可能被编译器完全内联；真实 `trace.Start -> atomic.Value -> interface`
  会有额外但仍很小的开销；
- 现有启用态 benchmark 使用合成阈值配置，分配数不等同于生产配置；
- benchmark 是串行的，没有复现全局 Mutex 的并发放大；
- 图 1 的主要问题恰好是串行微基准无法表现的竞争成本。

## 8. 推荐实施计划

### 阶段 0：补齐性能证据

目标：把“mutex 样本份额”转化为可做决策的吞吐和尾延迟证据。

1. 保存每个 workload 的原始 CPU、mutex、block、alloc profile。
2. 记录二进制 commit/build ID、Go 版本、配置、并发度、持续时间、完成请求数。
3. 所有组使用相同 mutex/block profile rate。
4. point_select 先完成 cache warm-up，确保 8 GB cache 条件一致。
5. 每组至少重复 3 次，报告中位数和波动范围。
6. 使用原始 profile 分别核对：

```bash
go tool pprof -top -sample_index=delay mutex.pb.gz
go tool pprof -top -cum -sample_index=delay mutex.pb.gz
go tool pprof -top -sample_index=contentions mutex.pb.gz
go tool pprof -top cpu.pb.gz
```

7. 使用 focus/list 确认 `IsMOCtledSpan` 的 Start 和 End 分支，并确认是否真的存在另一把
   frontend mutex。

产出：修订后的图 4，列名必须明确为 CPU、mutex delay 或 contentions，不能混用。

### 阶段 1：落地代码默认值并实施严格 A/B

当前代码改动已经完成以下边界：

- `config.NewObservabilityParameters()` 默认设置 `DisableSpan=true`；
- `DisableTrace=false`、`DisableError=false` 保持不变；
- 没有增加或修改部署配置文件；
- `mo-service` 和 embed 启动入口都通过默认构造器获得该值；
- 显式使用当前字段 `disable-span=false` 仍可在诊断环境重新启用 Span；
- 回归测试直接验证 `StatementInfo`、`MOZapLog` 和 `MOErrorHolder` collector 在
  `disableSpan=true` 时仍被注册。

建议分组：

| 组 | 配置/代码 | 用途 |
|---|---|---|
| A | 旧代码默认 Span enabled | 确认现状可复现 |
| B | 新代码默认 `DisableSpan=true`，不修改部署配置 | 验证新产品默认值和整个 Span 路径的性能上界 |
| C | Span 启用 + 无锁状态修复 | 测量只消除热点锁、保留功能后的收益 |
| D | `disable-trace=true`，仅隔离环境 | 测量 Statement/Log/Error exporter 的额外上界 |

必须采集：

- QPS/TPS、完成请求总数；
- p50/p95/p99/max；
- CPU 利用率和 CPU/op；
- mutex delay/op、contentions/op；
- alloc bytes/op、allocs/op、GC CPU、GC 次数；
- `statement_info` 上报量、collector queue/flush 状态；
- 错误率和超时率。

解释方式：

- B-A：代码默认关闭完整 Span 路径的影响；
- C-A：明确热锁修复且功能保留的收益；
- B-C：去锁后剩余的 Span ID/context/record 开销；
- D-B：Statement、日志、错误和 exporter 的额外开销。

当前改动已完成的本地验证：

- `pkg/config` 全包测试通过；
- `pkg/util/trace/impl/motrace` 全包测试通过；
- `cmd/mo-service` 默认值和显式 opt-in 定向测试通过；
- 新增 exporter 回归测试，确认关闭 Span 时 Statement、日志和错误 collector 仍注册；
- `pkg/config` 构建通过；相关三个包 `go vet` 通过；
- `git diff --check` 通过。

这些验证证明配置传播和 collector 注册边界成立，但不能替代 workload 级严格 A/B。

#### 50 机器端到端验证（2026-07-17）

在 `mo@10.222.1.50` 上使用独立 worktree
`/home/mo/matrixone-disable-span` 检出 `codex/default-disable-span@966cba0de4`，执行
`make build` 成功。二进制 `-version` 显示 branch 和 commit 与本次改动一致。

默认关闭组直接使用仓库 `etc/launch/launch.toml`，其中没有 `disable-span` 配置。单机
Log/TN/CN 启动成功后，执行唯一标记的 DDL/DML/DQL 和一条预期失败 SQL，等待 exporter
flush，系统表计数如下：

| 表 | 操作前 | 操作后 | 结论 |
|---|---:|---:|---|
| `system.statement_info` | 200 | 580 | StatementInfo 持续写入；成功 insert 和失败 SQL 均可查到 |
| `system.rawlog` | 1554 | 2068 | 日志 exporter 持续写入 |
| `system.error_info` | 0 | 1 | 预期失败 SQL 以错误码 20303 写入 |
| `system.span_info` | 0 | 0 | 默认未产生 Span 记录 |

mutex profile 使用 `-mutex-profile-fraction=1`，两组都执行相同的 1,000,000 次主键
point select、128 并发：

| 组 | Span 状态 | mysqlslap 单次耗时 | mutex profile 中 `IsMOCtledSpan` |
|---|---|---:|---:|
| 默认组 | 配置文件无 `disable-span`，代码默认关闭 | 11.629s | 精确 focus 无样本；profile 总 delay 10.27s |
| 显式启用组 | `/tmp` 中首个 Log service 配置 `disable-span=false` | 12.013s | 7046.69ms，占 profile 总 delay 37.00% |

显式启用组的锁栈同时包含 `MOTracer.Start`、`MOSpan.End/NeedRecord`、frontend/compile/txn
调用方，与图 1 的归因一致；默认组在存在其他 mutex 样本的前提下完全没有该栈，证明新默认
确实切断了目标热点，而不是因为 profiler 未启用。

上述耗时各只有一次运行，不能把 11.629s 与 12.013s 的差值当作正式性能收益；正式发布
仍需按照本节前述方法至少重复三次并报告中位数、波动、QPS/p99 和 CPU/op。远端 profile
保留为 `/tmp/mo-span-default-disabled-mutex-1m.pb.gz` 和
`/tmp/mo-span-enabled-global-mutex-1m.pb.gz`。

### 阶段 2：根据 A/B 结果决定是否继续优化

如果新默认值通过功能与性能验收，Span 继续保持默认关闭，则本阶段不阻塞发布。
只有未来要在高并发生产环境重新启用 Span，才实施下面的代码级修复。

#### 2.1 使用不可变原子快照

建议模型：

```go
type moCtledSpanSnapshot struct {
    states [7]MoCtledState
    exists [7]bool
}

var moCtledSnapshot atomic.Pointer[moCtledSpanSnapshot]
var moCtledWriteMu sync.Mutex
```

读路径：

1. `snapshot := moCtledSnapshot.Load()`；
2. 校验 Kind 下标；
3. 直接返回值，不加锁、不访问可变 map、不返回内部指针。

写路径：

1. writer mutex 串行化 `mo_ctl` 更新；
2. clone 旧 snapshot；
3. 修改 clone；
4. 原子 Store 新 snapshot。

`SpanKind` 当前为连续的 0～6，数组索引可行；实现仍需对未知/负 Kind 做边界检查。

相比 `RWMutex`，该模型能完全消除请求读路径中的 mutex profile 栈。

#### 2.2 Start 前置快速判断

在 `newMOSpan()` 前：

1. provider atomic enable fast path；
2. 扫描 opts 提取 `trace.KindOption`；
3. 从原子 snapshot 查询状态；
4. 受控且关闭时直接返回 Noop；
5. 只有实际启用时才创建和初始化 Span。

需要保证 option 只 Apply 一次，避免回退到历史上“为了检查 Kind 分配完整 SpanConfig”的问题。

#### 2.3 修复 Duration

在 `NeedRecord` 任何分支前计算 Duration，并增加真实 End 路径测试。

#### 2.4 provider enable 原子化

将 `tracerProviderConfig.enable` 替换为 `atomic.Bool`。Shutdown 仍可原子设置 false，避免每个
Start/Statement 检查 provider RWMutex。

#### 2.5 是否缓存 Start 时控制状态

需要明确以下语义之一：

- **快照语义**：Span 是否记录及 threshold 由 Start 时状态决定；End 不再查询全局状态。
- **动态语义**：运行中的 Span 会受中途 `mo_ctl` 修改影响；End 再读取一次原子快照。

当前代码倾向动态语义，因为 End 会重新查询。修复时建议先保持动态语义，降低行为变化；
如要改为快照语义，应作为单独产品变更评审。

### 阶段 3：可选无锁实现的测试与验收

#### 单元测试

- `IsMOCtledSpan`：所有 Kind、unknown Kind、初始化前后、enable/disable、threshold；
- 并发 Set/Is，使用 `-race`；
- 多 writer 更新不得丢失；
- controlled-disabled Start 对非 nil ctx 原样返回 NoopSpan，nil ctx 仍按现有语义归一化；
- options 只 Apply 一次；
- 正 threshold 通过真实 `End()` 触发/不触发 processor；
- Shutdown 与并发 Start；
- snapshot 初始化和重复初始化行为。

#### Benchmark

- `BenchmarkIsMOCtledSpan` 串行和 `RunParallel`；
- `BenchmarkMOTracerStartParallel`：Internal、Statement disabled、Statement enabled；
- `BenchmarkMOTracerStartEndParallel`；
- Noop、原实现、无锁实现对比；
- `-cpu=1,4,8,12` 验证扩展性。

#### 包级验证

trace/motrace 为 CGo-transitive，使用仓库确定性 wrapper：

```bash
.agents/skills/mo-dev/scripts/mo-cgo-test \
  -count=1 -timeout=120s \
  ./pkg/util/trace/... ./pkg/util/export/...
```

再运行依赖回归：

- `pkg/frontend` 相关 StatementInfo 测试；
- `pkg/common/morpc` codec 测试；
- `pkg/sql/plan/function/ctl` TraceSpan 测试；
- system.statement_info/log/error/span distributed cases；
- 相同 workload 的 A/B profile。

#### 性能验收标准

- `trace.IsMOCtledSpan -> sync.Mutex.Unlock` 从 mutex profile 中消失；
- 不出现新的集中式锁热点；
- C 组相对 A 组 QPS/p99 有稳定改善，且功能完全保留；
- C 组 `statement_info`、日志、错误、CU 数据与 A 组语义一致；
- CPU 和内存无显著回退；
- race、单测、依赖测试通过。

### 阶段 4：如果仍希望退役 Span

只有在阶段 2 修复后仍确认 Span 的产品价值低于维护成本，才进入退役设计：

1. 统计 `system.span_info` 的真实查询者、数据量和运维使用场景；
2. 确认云平台、Support、SRE 是否依赖 TraceID、Hung Span profile 或 `mo_ctl TraceSpan`；
3. 将 `motrace/statistic` 移到独立稳定包，避免 SQL 执行层依赖 Span 实现目录；
4. 将 `StatementInfo`、CU、日志、错误 exporter 拆为独立 observability pipeline；
5. 保留 `pkg/util/trace` API 的 Noop 实现，使数十个调用文件不必同步大改；
6. 保留 RPC header 的空 SpanContext slot，保证混合版本互通；
7. 停止写新 span，但保留历史 `system.span_info` 和 schema 兼容期；
8. 经过至少一个明确 deprecation 周期后，再在大版本迁移中删除 Span schema/API。

这是一项架构拆分和产品兼容工作，不是简单删除目录。

## 9. 发布与回滚计划

### 9.1 默认值灰度实验

- 使用新默认值构建先灰度单个可替换 CN 或独立压测集群，不修改其配置文件；
- 二进制升级需要重启；
- 确认 SQL、StatementInfo、日志、错误和 CU exporter 正常；
- 如果诊断能力不足或数据链路异常，可回滚旧二进制，或显式设置
  `disable-span=false` 并重启，临时恢复 Span。

### 9.2 无锁修复

- 先 canary 单 CN，保持 RPC wire 和 schema 不变；
- 观察 mutex、CPU、错误、collector、statement_info 数据；
- 再按 CN 批次滚动；
- TN/Log 节点随后升级；
- 回滚仅回滚实现，不需要 schema 或协议回滚。

## 10. 待确认问题

在做删除类决策前，需要明确：

1. 图 4 三组数据对应的原始 mutex profile 在哪里？
2. profile 采集时的 commit/build ID 是否为当前 main？
3. “frontend 分布锁”具体指哪个 mutex 字段，原始 Unlock 栈是什么？
4. 生产是否查询 `system.span_info`，查询频率和使用者是谁？
5. Support/SRE 是否使用 TraceID 关联 log/error/RPC？
6. 是否使用 `mo_ctl TraceSpan` 和 Hung Span 自动 profile？
7. `statement_info` 和 CU 是否参与云平台查询、容量分析或成本核算？
8. 期望的 mo_ctl 语义是 Start 快照还是 End 动态读取？
9. 是否允许新代码默认关闭 Span 的构建在目标环境做可回滚 A/B？

## 11. 最终决策建议

当前建议决策为：

1. **接受“`IsMOCtledSpan` 全局锁是明确热点”的判断。**
2. **否决“trace 占 50%～60% CPU”及“直接删除无影响”的推导。**
3. **将 `DisableSpan=true` 设为代码默认值，不要求部署环境修改配置文件，并进行严格 A/B。**
4. **验收必须确认 StatementInfo、日志和错误记录语义及数据量不回退；CU 不作为本次决策门槛。**
5. **无锁 Span 改造降为条件项。**只有未来需要重新启用 Span 或诊断场景存在高并发要求时再实施。
6. **整个 trace/motrace 模块当前不得直接删除。**如需退役，只能在完成能力拆分、消费者
   审计、协议兼容和 schema deprecation 后分阶段进行。
