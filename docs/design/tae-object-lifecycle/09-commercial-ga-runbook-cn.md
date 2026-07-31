# TAE Object Lifecycle Commercial GA 运行与放量手册

本文只描述已经进入代码的 Phase 1 能力。它不把 1/10 TiB 或 30 天 elapsed soak
写成“单测已证明”，也不允许绕过 Stage、升级和 coexistence 门禁。

## 1. 上线前硬条件

上线前必须同时满足：

1. 所有 CN/TN 都包含 Lifecycle tagged-entry 安全解析代码；旧节点仍在集群时只能
   Export-only，不能开启 retirement。
2. ARCHIVE 使用专用 S3-compatible Bucket/Container，Versioning 关闭，并配置
   incomplete multipart 自动回收。
3. Stage 只使用 workload identity、IAM Role 或部署管理的 credential handle；
   禁止把 inline secret 当作长期归档凭据。Phase 1实际可解析的handle仅为`default`、
   `role-arn:<arn>`和`shared-profile:<profile>`；release allowlist与
   `SET LIFECYCLE`会拒绝其他字符串。新增部署alias必须先实现对应FileService resolver，
   不能先绑定再让后台任务永久失败。
4. 完成 Whole、Rewrite、Restore、Purge、DROP、CN/TN restart、commit response lost
   的故障注入矩阵。
5. 公共 Merge #26376 修复在目标版本中，或者认证上限证明不会触发公共 Merge OOM。
6. 未绑定表的 DML、查询、Merge、checkpoint、GC、logtail 对照指标满足发布阈值。

## 2. Release gate 与 Kill switch

升级默认写入：

```json
{"archive_stages":[]}
```

并保持 `enabled=false`。`scope_spec` 中每个 Archive Stage 必须固定：

```json
{
  "account_id": 17,
  "stage_id": 12,
  "canonical_url": "s3://archive-bucket/mo/history",
  "provider": "amazon",
  "endpoint": "https://s3.example.com",
  "region": "me-south-1",
  "credential_handle": "role-arn:arn:aws:iam::17:role/mo-archive",
  "storage_class": "STANDARD",
  "encryption_identity": "kms/archive",
  "versioning_disabled": true,
  "abort_incomplete_multipart": true
}
```

Phase 1的通用FileService没有逐PUT选择Storage Class或KMS key的参数，因此认证只允许
`storage_class=STANDARD`（或省略，等价于Provider默认STANDARD）。`encryption_identity`
表示专用Bucket/Container上由Provider policy强制的默认加密身份，认证必须用实际PUT/HEAD和
Provider审计日志证明；它不是Lifecycle在每次PUT中注入的请求header。其他Storage Class
需要先作为公共FileService能力实现，不能只改认证JSON后宣称生效。

变更 release row 是 system-account 运维操作，必须走配置审计。先写完整
`scope_spec`，再开启 `enabled`。

紧急停止时将 `enabled=false`。Kill switch 的语义是：

- 立即停止新 Binding、Discovery、Archive/TTL child 和 finalization；
- 已存在的 Cleanup Root reconciliation、Provider cleanup、Restore 超时隐藏表清理和
  终态元数据回收继续运行；
- 不删除 `COMMIT_UNKNOWN` Root，不猜测普通 MO 事务终态。

Phase 1物理Backup不复制Archive Payload。`enabled=true`期间`BACKUP`会显式拒绝；需要
Backup时先关闭release gate，等待所有FINALIZING/COMMIT_UNKNOWN与在途child按运维流程
收敛，再执行Backup。DR目标也不承诺`RESTORE ARCHIVE`可用。

## 3. 分阶段放量

严格按以下顺序，每阶段至少覆盖一次 Merge 竞争、并发 DELETE、CN/TN restart 和
Provider 429/超时：

1. Export/readback，不退休源 Object；
2. Whole DELETE；
3. Whole ARCHIVE；
4. 单源 Mixed Rewrite；
5. Restore/Purge；
6. 50 → 200 → 500 → 1000 张绑定表。

Gate F 的 TTL small-Mixed `Relation.Delete` 是可关闭优化。未单独认证时，Mixed TTL
继续走单源 Rewrite 或 `MIXED_LAYOUT_BLOCKED`，不阻塞核心 GA。

任何阶段出现以下情况立即停止扩大：

- Dataset 可见但源 Object 未退休，或源已退休但 Dataset 不可见；
- Restore hash、row count、schema 或 AUTO_INCREMENT 水位不一致；
- Cleanup 删除 `PUBLISHED`/`COMMIT_UNKNOWN` Payload；
- unknown Root、cleanup backlog、Restore staging 或 Rewrite source-byte budget达到上限；
- 未绑定表吞吐/P99、Merge backlog、checkpoint、GC、logtail 超过批准阈值；
- 普通 MO panic、数据错误或不可恢复 OOM。

## 4. 认证执行

使用 [certification harness](../../../tools/lifecycle-certification/README.md)
生成可重放 SQL、前后指标和机器可读 evidence。必须分别执行：

- 常见单表 1 TiB；
- 单表 10 TiB 认证边界；
- 50/200/500/1000 Binding coexistence；
- 最大 Object、超大 varlen、高压缩率 Row Group；
- 30 天真实 elapsed soak。

`MO_LIFECYCLE_DRY_RUN=1` 只证明计划可生成，不是容量证据。30 天 soak 每日保留：

- evidence JSON 和 SQL log；
- `mo_lifecycle_*` 指标；
- CN/TN restart、panic、OOM、goroutine 和 heap 证据；
- Merge/checkpoint/GC/logtail backlog；
- Provider request、429、multipart、LIST/Delete 和存储账单；
- 未绑定业务基线与 active coexistence 对照。

正式容量执行必须设置`MO_LIFECYCLE_WAIT_SECONDS`并以
`retirement_completed=true`、`remaining_active_rows=0`作为本轮完成证据；超时的
evidence保留但判定失败。部署级CN/TN/Provider故障通过
`MO_LIFECYCLE_FAULT_HOOK`在`after-load`、`after-bind`和`before-verify`阶段执行，
hook输出与SQL、指标一起归档。没有fault hook或等价平台混沌记录，不能勾选restart和
response-lost门禁。

Lifecycle production Task直接复用MO已有`pkg/util/fault`控制面，不建设第二套注入服务。
稳定fault point名称统一为：

```text
tae-object-lifecycle/<point>
```

其中`<point>`包括`after-root-register`、`after-protection`、
`before-source-read`、Payload/Manifest PUT与readback前后、Rewrite staging前后、
final commit前后、Cleanup LIST/Delete前后，以及Restore initialize/chunk/publish前后。
默认未启用MO fault injection时只是一次立即返回的公共Trigger检查；认证环境可使用公共
fault action的ECHO/RETURN、SLEEP/WAIT或PANIC，再由
`MO_LIFECYCLE_FAULT_HOOK`负责启用、观察和撤销，禁止在共享生产集群遗留fault point。

## 5. 告警与处置

重点告警：

- `COMMIT_UNKNOWN` Root > 0 或持续增长；
- Cleanup Root backlog/age 超阈值；
- `resource_rejections_total` 持续增长；
- Restore FAILED/超时或 staging bytes接近上限；
- Rewrite amplification/blocking持续增长；
- Dataset readback/Restore digest mismatch；
- Provider versioning或credential配置漂移。

处理原则：

- `COMMIT_UNKNOWN` 不删文件、不重启相同 source retirement，先查 matching
  Dataset/TTL Receipt；
- Provider 失败只暂停 Lifecycle，不同步阻塞 DROP；
- Stage 漂移暂停新 ARCHIVE，已发布 Dataset保持可见并告警；
- 资源上限只暂停 Lifecycle child，不提高普通事务/Merge风险；
- 需要修复普通事务、Merge、WAL、Replay或GC时进入对应公共模块，不在 Lifecycle
  私建补偿协议。

## 6. GA 签字

最终签字必须明确列出：

- 实际执行的 commit SHA、配置、provider和容量；
- Gate A–I 测试结果；
- 1/10 TiB与30天soak证据路径；
- 50→1000放量每阶段结果；
- 尚未执行或被豁免的项目；
- owner review结论。

没有真实外部证据时，状态只能是 `Conditional Go`，不能写成 Commercial GA 已完成。
