-- ============================================================================
-- 复现 incrservice hang 问题（单机版）
-- 
-- 背景：
--   生产环境中，CREATE TABLE (带 auto_increment 列) 在以下条件下卡住数分钟到数十分钟：
--   1. lockMoTable 遇到 ErrTxnNeedRetryWithDefChanged（并发 DDL 导致 table def 变更）
--   2. prepareRetry 回滚 workspace 写入，但不释放 lockService 锁，也不清理 incrservice 内存状态
--   3. 重试时 maybeCreateAutoIncrement 再次进入 incrservice，store.Allocate 的
--      SELECT ... FOR UPDATE 在 lockService 中等待上一次未释放的锁
--   4. doAllocate 的 3 分钟 defaultAllocateTimeout 超时后才返回错误
--
-- 本脚本用两个 fault point 模拟这个过程：
--   - lock_mo_table_def_changed: 在 lockMoTable 中触发一次 ErrTxnNeedRetryWithDefChanged
--     freq="1:1:::" 表示只在第 1 次调用时触发，之后不再触发（模拟并发 DDL 只冲突一次）
--   - incrservice_allocate_hang: 在 doAllocate 中 sleep 300 秒，模拟锁等待
--     freq=":::" 表示每次调用都触发
--
-- 执行流程：
--   第一次 CREATE TABLE:
--     → lockMoTable 触发 lock_mo_table_def_changed → 返回 ErrTxnNeedRetryWithDefChanged
--     → Compile.Run 进入 retry 路径 → prepareRetry(defChanged=true)
--     → RollbackLastStatement（回滚 workspace 写入，但不释放锁/不清理 incrservice）
--     → IncrStatementID → 重建 plan（新 table-id）
--
--   重试 CREATE TABLE:
--     → lockMoTable 不再触发（fault 已自动移除）→ 正常拿锁
--     → dbSource.Create 成功（新 table-id）
--     → maybeCreateAutoIncrement → incrservice.Create → INSERT 成功
--     → preAllocate → asyncAllocate → doAllocate
--     → 触发 incrservice_allocate_hang → time.Sleep(300s)
--     → 观察：CREATE TABLE 卡住
--
-- 预期结果：
--   CREATE TABLE 应该在 ~3 分钟后超时返回错误（defaultAllocateTimeout = 3min）
--   如果超过 5 分钟仍然卡住，说明超时机制存在 bug
--
-- 注意：
--   fault.TriggerFault 的 sleep action 使用 time.Sleep，不受 context cancellation 影响
--   这精确模拟了生产环境中 lockService.Lock 等待不响应 context 取消的行为
-- ============================================================================

-- 准备：创建测试数据库
DROP DATABASE IF EXISTS test_incr_hang;
CREATE DATABASE test_incr_hang;
USE test_incr_hang;

-- 步骤 1：启用 fault injection
SELECT enable_fault_injection();

-- 步骤 2：注入 fault point
-- 参数格式：name#freq#action#iarg#sarg#constant
-- freq 格式：start:end:skip:prob（空值使用默认值）
SELECT fault_inject('all.', 'ADD_FAULT_POINT', 'lock_mo_table_def_changed#1:1:::#echo#0##false');
SELECT fault_inject('all.', 'ADD_FAULT_POINT', 'incrservice_allocate_hang#:::#sleep#300##false');

-- 步骤 3：确认 fault point 已注入
SELECT fault_inject('all.', 'LIST_FAULT_POINT', '');

-- 步骤 4：执行 CREATE TABLE（会触发 fault point 链）
-- 记录开始时间，然后观察卡住多久
SELECT NOW() AS '开始时间';

-- ⚠️ 这条语句会卡住！预期 ~3 分钟后超时返回错误
-- 如果超过 5 分钟还没返回，说明超时机制有 bug
CREATE TABLE test_hang (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100)
);

SELECT NOW() AS '结束时间';

-- 步骤 5：检查结果
SHOW TABLES;

-- 步骤 6：清理
-- DROP DATABASE IF EXISTS test_incr_hang;
