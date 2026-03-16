# 复现 incrservice asyncAllocate 卡死 Bug

## Bug 描述

`CREATE TABLE` 在并发 DDL 场景下，触发 `ErrTxnNeedRetryWithDefChanged` 后重试，
重试过程中 `maybeCreateAutoIncrement` → `incrservice.Create()` → `newColumnCache` →
`preAllocate` → `asyncAllocate` 的 RPC 调用到 TN 永远不返回，导致：

- txn 泄漏，锁永远不释放
- leak checker 每 20s 报 ERROR 但无法强制 abort
- 后续所有对同一表的 DDL 全部阻塞
- 只有客户端断开连接（context cancel）才能释放

## 完整 Bug 路径

```
CREATE TABLE staff_info (id INT AUTO_INCREMENT PRIMARY KEY, ...)
  │
  ├─ lockMoTable() → ErrTxnNeedRetryWithDefChanged（并发 DDL 改了表定义）
  │
  ├─ Compile.Run retry → prepareRetry(defChanged=true) → 重建 plan
  │
  ├─ 重试 CreateTable:
  │   ├─ lockMoTable() → 成功（冲突已解决）
  │   ├─ dbSource.Create() → 成功
  │   └─ maybeCreateAutoIncrement()
  │       └─ incrservice.Create()
  │           └─ newTableCache() → newColumnCache()
  │               ├─ preAllocate() → asyncAllocate() [发 RPC 到 TN]
  │               └─ waitPrevAllocatingLocked() → 永远阻塞 ← BUG
  │
  └─ txn 泄漏，锁不释放，级联阻塞
```

## 复现方式

### 方式 1: Go 单元测试（最简单，无需集群）

```bash
cd matrixone
go test -v -run TestCreateTable_IncrserviceHangAfterRetry ./pkg/sql/compile/ -count=1
```

通过 gostub 替换 `maybeCreateAutoIncrement` 模拟 asyncAllocate 卡死，
验证 context cancel 能正确解除阻塞。

### 方式 2: 故障注入 + 真实 MO（推荐，完整复现生产路径）

需要两个故障注入点配合：
1. `lock_mo_table_def_changed` — 在 `lockMoTable` 中注入，触发 retry 路径
2. `incrservice_allocate_hang` — 在 `preAllocate` 中注入 sleep，模拟 TN 卡住

编译包含故障注入代码的 MO 后（单机版或分布式均可）：

```bash
# 一键复现：
bash scripts/repro_incrservice_hang/repro_with_fault_inject.sh [HOST] [PORT] [USER] [PASS]
```

或手动操作：

```bash
# 1. 启用 fault injection 框架（默认关闭）
SELECT enable_fault_injection();

# 2. 注入故障点 1: lockMoTable 返回 ErrTxnNeedRetryWithDefChanged
#    freq "1:1::" = 只在第 1 次调用时触发，retry 时不再触发
#    action=echo: 只让 TriggerFault 返回 ok=true，不阻塞
SELECT fault_inject('all.', 'ADD_FAULT_POINT', 'lock_mo_table_def_changed#1:1:::#echo#0##false');

# 3. 注入故障点 2: incrservice preAllocate 卡住
#    freq ":::" = 每次都触发
#    action=sleep, iarg=300: 阻塞 300 秒
SELECT fault_inject('all.', 'ADD_FAULT_POINT', 'incrservice_allocate_hang#:::#sleep#300##false');

# 4. 在另一个 session 执行 CREATE TABLE（会卡住 ~300 秒）
CREATE DATABASE IF NOT EXISTS test_hang;
CREATE TABLE test_hang.staff_info (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(100));

# 5. 观察 MO 日志：
#    - "FAULT INJECTION: incrservice_allocate_hang triggered"
#    - 每 20s: "found leak txn"
#    - Compile.Run 的 retry 日志

# 6. 清理
SELECT fault_inject('all.', 'REMOVE_FAULT_POINT', 'lock_mo_table_def_changed');
SELECT fault_inject('all.', 'REMOVE_FAULT_POINT', 'incrservice_allocate_hang');
SELECT disable_fault_injection();
DROP DATABASE IF EXISTS test_hang;
```

> **注意**：`mo_ctl('cn', 'AddFaultPoint', ...)` 在单机版不可用（报 `not supported`），
> 因为 `mo_ctl` 的 `AddFaultPoint` 只走 TN handler。必须用 `fault_inject()` 函数。

### 方式 3: 并发 DDL 脚本（触发 retry 路径，TN 卡死靠运气）

```bash
bash scripts/repro_incrservice_hang/repro_concurrent_ddl.sh [HOST] [PORT] [USER] [PASS]
```

多个 session 并发 DROP+CREATE 同名表，触发 `ErrTxnNeedRetryWithDefChanged`。
TN 卡死部分依赖运气，建议配合方式 2 的 incrservice_allocate_hang 一起使用。

## 故障注入点说明

### lock_mo_table_def_changed（ddl.go）

位置：`pkg/sql/compile/ddl.go` 的 `lockMoTable` 函数开头

```go
if _, _, ok := fault.TriggerFault("lock_mo_table_def_changed"); ok {
    return moerr.NewTxnNeedRetryWithDefChangedNoCtx()
}
```

效果：模拟并发 DDL 导致的 lock 冲突，触发 `Compile.Run` 的 retry 路径。

### incrservice_allocate_hang（column_cache.go）

位置：`pkg/incrservice/column_cache.go` 的 `preAllocate` 函数中

```go
if iarg, _, ok := fault.TriggerFault("incrservice_allocate_hang"); ok {
    // sleep action 已在 TriggerFault 内部执行
}
```

效果：在 `asyncAllocate` 之前阻塞，模拟 TN 不响应。由于 `preAllocate` 持有
`col.Lock()`，后续 `waitPrevAllocatingLocked` 会被阻塞。

## fault_inject 参数格式

```
fault_inject('service.pods', 'command', 'name#freq#action#iarg#sarg#constant')
```

- service: `all` / `cn` / `tn`
- command: `ADD_FAULT_POINT` / `REMOVE_FAULT_POINT` / `ENABLE_FAULT_INJECTION` / ...
- freq: `start:end:skip:prob`（留空用默认值：`1:MaxInt:1:1.0`，即 `":::"` = 每次触发）
- action: `echo`（返回值）/ `sleep`（阻塞 iarg 秒）/ `wait`（永远阻塞直到 notify）/ `panic` / ...
- constant: `true`（不可覆盖）/ `false`（可覆盖）

## 文件清单

```
scripts/repro_incrservice_hang/
├── README.md                       # 本文件
├── repro_with_fault_inject.sh      # 故障注入复现脚本（完整路径）
└── repro_concurrent_ddl.sh         # 并发 DDL 复现脚本

pkg/sql/compile/
├── ddl.go                          # lockMoTable 故障注入点
└── ddl_incrservice_hang_test.go    # Go 单元测试

pkg/incrservice/
└── column_cache.go                 # preAllocate 故障注入点
```

## 根因分析

1. `asyncAllocate` 发起 RPC 到 TN 分配自增值范围，无超时保护
2. 如果 TN 慢/卡住，`waitPrevAllocatingLocked` 永远阻塞
3. CN 侧 leak checker 只打日志，无法强制 abort txn
4. 锁永远不释放，级联阻塞后续 DDL
