# 复现 incrservice asyncAllocate 卡死 Bug

## Bug 描述

`CREATE TABLE` 在并发 DDL 场景下，触发 `ErrTxnNeedRetryWithDefChanged` 后重试，
重试过程中 `maybeCreateAutoIncrement` → `incrservice.Create()` → `newColumnCache` →
`preAllocate` → `asyncAllocate` 的 RPC 调用到 TN 永远不返回，导致：

- txn 泄漏，锁永远不释放
- leak checker 每 20s 报 ERROR 但无法强制 abort
- 后续所有对同一表的 DDL 全部阻塞
- 只有客户端断开连接（context cancel）才能释放

## 复现方式

### 方式 1: Go 单元测试（推荐，无需集群）

```bash
cd matrixone
go test -v -run TestCreateTable_IncrserviceHangAfterRetry ./pkg/sql/compile/ -count=1
```

通过 stub `maybeCreateAutoIncrement` 模拟 asyncAllocate 卡死，
验证 context cancel 能正确解除阻塞。

### 方式 2: 故障注入 + 真实集群（推荐，最接近生产场景）

已在 `pkg/incrservice/column_cache.go` 的 `preAllocate` 中添加了故障注入点。
编译包含此修改的 MO 后：

```bash
# 启动 MO 集群后执行：
bash scripts/repro_incrservice_hang/repro_with_fault_inject.sh [HOST] [PORT] [USER] [PASS]
```

或手动操作：

```bash
# 1. 启用故障注入（在 MO SQL 客户端中执行）
#    sleep 300 = 模拟 TN 卡住 300 秒
SELECT mo_ctl('cn', 'AddFaultPoint', 'incrservice_allocate_hang.:::.sleep.300.');

# 2. 在另一个 session 执行 CREATE TABLE（会卡住 300 秒）
CREATE TABLE test_db.staff_info (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(100));

# 3. 观察 MO 日志
#    - "FAULT INJECTION: incrservice_allocate_hang triggered"
#    - 每 20s: "ERROR cn-service found leak txn"

# 4. 移除故障点
SELECT mo_ctl('cn', 'RemoveFaultPoint', 'incrservice_allocate_hang');
```

### 方式 3: 并发 DDL 脚本（需要真实集群，触发 retry 路径）

```bash
bash scripts/repro_incrservice_hang/repro_concurrent_ddl.sh [HOST] [PORT] [USER] [PASS]
```

多个 session 并发 DROP+CREATE 同名表，触发 `ErrTxnNeedRetryWithDefChanged`。
TN 卡死部分依赖运气，建议配合方式 2 使用。

## 文件清单

```
scripts/repro_incrservice_hang/
├── README.md                       # 本文件
├── repro_with_fault_inject.sh      # 故障注入复现脚本
└── repro_concurrent_ddl.sh         # 并发 DDL 复现脚本

pkg/sql/compile/
└── ddl_incrservice_hang_test.go    # Go 单元测试

pkg/incrservice/
└── column_cache.go                 # 已添加故障注入点 (fault.TriggerFault)
```

## 根因分析

见之前的对话分析。核心问题：

1. `asyncAllocate` 发起 RPC 到 TN 分配自增值范围，无超时保护
2. 如果 TN 慢/卡住，`waitPrevAllocatingLocked` 永远阻塞
3. CN 侧 leak checker 只打日志，无法强制 abort txn
4. 锁永远不释放，级联阻塞后续 DDL
