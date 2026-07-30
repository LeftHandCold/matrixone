# 04 Cleanup Root、所有权与Reconciliation详细设计

## 1. Owner模型

| 资源 | final结果前 | commit成功后 | 删除Owner |
|---|---|---|---|
| Archive Payload/Manifest | Root | Dataset控制可见性，Root保留物理清理职责 | Root Sweeper |
| external booking | Root | 临时，无长期业务引用 | Root Sweeper |
| live TAE staging文件 | Root | 现有TAE Catalog/WAL/GC | commit前Root；commit后TAE |
| source TAE Object | 现有TAE + SyncProtection租约 | DropIntent后现有TAE | 现有GC |
| Restore staging表 | Restore Attempt | 发布后普通Catalog | Restore worker/普通DROP |

Dataset Purge只改变Dataset状态并触发Root，不直接调用Provider。

Whole TTL和TTL小Mixed没有本表管理的物理副作用，不创建Root；其事务结果由普通MO和
TTL Receipt/exact source重扫处理。以下状态机只适用于Archive或Rewrite attempt。

## 2. Root状态机

```text
REGISTERED
  -> UPLOADING
  -> VERIFIED
  -> FINALIZING

REGISTERED/UPLOADING/VERIFIED
  -> attempt abort/timeout/lease loss/owner missing
  -> DELETE_PENDING

FINALIZING
  -> matching Dataset/TTL Receipt
  -> PUBLISHED

FINALIZING
  -> known abort and no matching publication
  -> DELETE_PENDING

FINALIZING
  -> result unknown
  -> COMMIT_UNKNOWN

COMMIT_UNKNOWN
  -> matching Dataset/TTL Receipt
  -> PUBLISHED

COMMIT_UNKNOWN
  -> authoritative ordinary-MO abort result and no publication
  -> DELETE_PENDING

PUBLISHED
  -> Archive Dataset Purge or owner missing
  -> DELETE_PENDING

PUBLISHED
  -> TTL Rewrite没有长期Payload且TAE已接管live Object
  -> DELETE_PENDING

DELETE_PENDING
  -> Dataset已无有效restore lease / cleanup_after reached
  -> DELETING
  -> CLEANED
```

若普通MO无法提供权威abort查询，`COMMIT_UNKNOWN`不能因timeout自动转删除；暂停该表并人工
处理。不为此建设Lifecycle Terminal Journal。

任一物理表存在`COMMIT_UNKNOWN` Root时，Scheduler暂停该Binding全部新retirement。这样
无需在Root保存逐Object集合或建设overlap index；对账收敛后才恢复。

## 3. Root-before-side-effect

Root必须在以下任何动作前durable：

- CreateMultipartUpload/PUT；
- live TAE staging Object写入；
- external booking写入。

Root预先保存Archive namespace/prefix、TAE namespace/segment/booking prefix和ordinal
upper bound；不适用于当前mode的namespace字段为NULL：

| mode | Archive namespace | TAE staging/booking namespace |
|---|---|---|
| ARCHIVE_WHOLE | 必须 | 不需要 |
| ARCHIVE_REWRITE | 必须 | 必须 |
| TTL_REWRITE | 不需要 | 必须 |

单个文件key可在Manifest中枚举；Manifest尚未生成时prefix LIST兜底。

## 4. 不可变写入

每次PUT使用`write-id`不可变key。禁止同一key由两个worker写：

```text
payload-000001-<write-id>.parquet
manifest-<sha256>.json
booking-000001-<write-id>
```

旧worker和新worker不能接管同一attempt。lease/SyncProtection丢失后：

```text
old Root -> DELETE_PENDING
new attempt -> new Root + new prefix
```

Root `VERIFIED -> FINALIZING`使用
`(root_id, attempt_id, executor_epoch, state_version)`CAS，只管理本Root。

## 5. Worker crash与租约

- REGISTERED无副作用：仍进入DELETE_PENDING，由统一Sweeper确认无副作用后CLEANED；
- UPLOADING：新worker不继续写，进入DELETE_PENDING；
- VERIFIED：只有原worker在有效epoch内可CAS FINALIZING；
- FINALIZING/COMMIT_UNKNOWN：禁止接管、禁止Sweeper；
- PUBLISHED：Archive由Dataset/Purge事件驱动，TTL Rewrite可接管临时资源清理；
- DELETE_PENDING/DELETING：Sweeper lease可接管，Delete幂等。

所有worker在Provider/Rewrite操作间续约`worker_lease_deadline`，单次I/O不得超过发布配置的
最大I/O deadline。lease过期后只有状态允许的清理动作可以接管，不能继续Upload或Final。
进入DELETE_PENDING后仍要等待最大I/O deadline并执行迟到PUT quiescence协议。

## 6. 提交结果解释顺序

任何final error、EOB或response lost后：

```text
1. consistent read matching Dataset/TTL Receipt(root_id, attempt_id)
   -> PUBLISHED
2. ordinary transaction明确aborted且无matching publication
   -> DELETE_PENDING
3. 其他
   -> COMMIT_UNKNOWN
```

不能仅凭source已Drop推断成功，也不能仅凭EOB推断失败。

## 7. PUBLISHED到Purge

Archive commit成功后：

- Dataset成为Restore/保留策略入口；
- Root继续持有Payload删除能力；
- live TAE staging转交TAE；
- booking在安全窗口后由Root删除；
- Root保留到Dataset Purge或owner消失。

Root不需要逐资源子状态，只保存`temporary_cleanup_done`：

- `ARCHIVE_WHOLE`没有booking/live staging；
- `ARCHIVE_REWRITE`在PUBLISHED期间先清理booking并置位，但保留Payload；
- `TTL_REWRITE`在matching TTL Receipt证明提交后，TAE接管live Object，Root进入
  `DELETE_PENDING`，只清理booking和未被TAE接管的临时namespace。

Dataset CAS `PUBLISHED -> DELETE_PENDING`后，控制面把对应Root推进
`PUBLISHED -> DELETE_PENDING`。两步不要求跨账户2PC：

- Dataset先变更但Root未变更：Reconciler重放触发；
- Root先收到触发：必须再次确认Dataset不再PUBLISHED且lease已关闭；
- 删除前每次都校验root/dataset identity。

## 8. Delete协议

进入Root `DELETING`后：

- 禁止新增Restore/引用；
- 删除Archive Manifest列出的不可变key；
- Manifest不存在则LIST root prefix；
- 删除booking prefix和未发布TAE staging范围；
- 重复Delete/NotFound视为幂等；
- 所有namespace分别确认。

Phase 1只支持部署认证的非Versioned专用Archive target，因此Root按key删除当前且唯一的
物理对象版本。通用FileService不扩展version ID API。若运维破坏认证合同开启Versioning，
该Stage必须由运维撤销认证并暂停新Archive；Root保持`DELETING`并记录`last_error`，不能
仅因HEAD/LIST看不到Delete Marker后的current version就标记`CLEANED`。遗留历史版本由
Provider运维工具清理并重新认证后再收敛Root。

不能删除TAE已经接管的live Object。`state=PUBLISHED`或matching Dataset/TTL Receipt已经
证明提交时，TAE segment只用于identity核对，不进入Root Delete集合。

## 9. 迟到PUT与quiescence

一次LIST为空不能CLEANED：

```text
LIST/Delete
-> wait provider consistency + max I/O deadline
-> LIST again
-> if object found: Delete and reset quiescence_since
-> only continuous empty window allows CLEANED
```

Stage必须配置incomplete multipart生命周期规则。运维认证记录rule ID/天数；无规则Stage
不允许Bind。

## 10. Cleanup失败

Provider权限、credential或网络错误：

- Root进入/保持`DELETE_PENDING`或`DELETING`；
- 指数退避但有最大间隔；
- 记录last_error和失败次数；
- credential handle可重新解析轮换凭据；
- backlog rows/bytes达到hard cap暂停新Archive；
- 不阻塞普通DROP和普通MO。

## 11. 元数据GC

Root `CLEANED`后保留审计窗口。只有：

- quiescence完成；
- 无Dataset/Restore引用；
- 无COMMIT_UNKNOWN；
- 审计窗口结束；

才物理删除Root行。Reconciler按分片分页，不全表高频扫描。
