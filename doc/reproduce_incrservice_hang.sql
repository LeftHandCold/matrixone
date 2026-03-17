-- ============================================================================
-- 复现 incrservice hang 问题
--
-- 包含两个场景：
--   场景 A：单机版（单 CN，两个 session）
--   场景 B：多 CN 版（两个 CN，并发 CREATE 同名表 + retry 注入）
--
-- 背景：
--   生产环境中，txn 08f4 的 CREATE TABLE 卡了 63 分钟，只有 leak detection 日志，
--   没有 context deadline exceeded。根因是 allocator.run() 单线程被阻塞。
--
-- ============================================================================

-- ============================================================================
-- 场景 A：单机版复现（精确复现 63 分钟 hang）
--
-- 原理：
--   allocator.run() 是单线程串行处理 channel 中的 action。
--   用 incrservice_store_allocate_hang 让第一个 CREATE TABLE 的 store.Allocate
--   在 ExecTxn 内部 sleep，阻塞 run() goroutine。
--   第二个 session 的 CREATE TABLE 的 asyncAllocate action 排在 channel 里，
--   永远不会被处理 —— 这就是 63 分钟 hang 的复现。
-- ============================================================================

-- Session 1: 执行以下所有命令
-- ============================================================================

DROP DATABASE IF EXISTS test_incr_hang;
CREATE DATABASE test_incr_hang;
USE test_incr_hang;

SELECT enable_fault_injection();

-- 注入 fault point（只触发一次）
SELECT fault_inject('all.', 'ADD_FAULT_POINT', 'incrservice_store_allocate_hang#1:1::#sleep#3600##false');
SELECT fault_inject('all.', 'LIST_FAULT_POINT', '');

SELECT NOW() AS 'Session1 开始时间';

-- ⚠️ 卡住！store.Allocate 内部 sleep 3600 秒，run() goroutine 被阻塞
CREATE TABLE t1 (id BIGINT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(100));

-- Session 2: 在另一个 mysql 客户端执行（Session 1 卡住后立即执行）
-- ============================================================================
-- USE test_incr_hang;
-- SELECT NOW() AS 'Session2 开始时间';
--
-- -- ⚠️ 无限期卡住！action 在 channel 排队，没有超时
-- CREATE TABLE t2 (id BIGINT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(100));
--
-- SELECT NOW() AS 'Session2 结束时间';

-- 预期：
--   Session 1: 卡住 ~3600 秒
--   Session 2: 无限期卡住，没有 timeout 错误，只有 leak detection 日志

-- 清理：
--   DROP DATABASE IF EXISTS test_incr_hang;
--   SELECT fault_inject('all.', 'REMOVE_FAULT_POINT', 'incrservice_store_allocate_hang');


-- ============================================================================
-- 场景 B：多 CN 版复现（retry + FOR UPDATE 锁 + 并发 CREATE 同名表）
--
-- 环境：
--   CN1: localhost:16001
--   CN2: localhost:16002
--   docker-compose: etc/docker-multi-cn-local-disk/
--
-- 原理：
--   CN1 执行 CREATE TABLE，lock_mo_table_def_changed 触发 retry。
--   retry 后 store.Allocate 的 SELECT ... FOR UPDATE 成功，
--   incrservice_after_for_update 让它 sleep 600 秒持有行锁。
--   同时 run() goroutine 被阻塞。
--
--   同一 CN1 上的第二个 session 执行 CREATE TABLE（不同表名），
--   asyncAllocate action 排在 channel 里，永远不会被处理 → 63 分钟 hang。
--
--   CN2 执行 CREATE TABLE 同名表，lockMoTable 等待 CN1 的排他锁 → 阻塞。
-- ============================================================================

-- Terminal 1: 连接 CN1 (mysql -h 127.0.0.1 -P 16001 -u root -p111)
-- ============================================================================

-- DROP DATABASE IF EXISTS test_hang;
-- CREATE DATABASE test_hang;
-- USE test_hang;
--
-- SELECT enable_fault_injection();
--
-- -- 注入两个 fault points:
-- -- 1. lock_mo_table_def_changed: 第一次调用触发 retry
-- SELECT fault_inject('all.', 'ADD_FAULT_POINT', 'lock_mo_table_def_changed#1:1::#echo#0##false');
-- -- 2. incrservice_after_for_update: FOR UPDATE 成功后 sleep 600 秒
-- SELECT fault_inject('all.', 'ADD_FAULT_POINT', 'incrservice_after_for_update#:::#sleep#600##false');
--
-- SELECT fault_inject('all.', 'LIST_FAULT_POINT', '');
-- SELECT NOW() AS 'CN1-T1 开始';
--
-- -- ⚠️ 卡住：retry 后 store.Allocate 持有 FOR UPDATE 锁 sleep 600s
-- -- run() goroutine 被阻塞
-- CREATE TABLE t1 (id BIGINT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(100));

-- Terminal 2: 连接同一个 CN1 (mysql -h 127.0.0.1 -P 16001 -u root -p111)
-- ============================================================================

-- USE test_hang;
-- SELECT NOW() AS 'CN1-T2 开始';
--
-- -- ⚠️ 无限期卡住！
-- -- lockMoTable 成功（不同表名）→ dbSource.Create → maybeCreateAutoIncrement
-- -- → incrservice.Create → store.Create(INSERT) → newTableCache → preAllocate
-- -- → asyncAllocate 发送 action 到 channel → run() goroutine 被 T1 阻塞
-- -- → action 在 channel 排队 → waitPrevAllocatingLocked 等待 → 永远不会完成
-- --
-- -- 这就是 63 分钟 hang 的精确复现！
-- CREATE TABLE t2 (id BIGINT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(100));

-- Terminal 3: 连接 CN2 (mysql -h 127.0.0.1 -P 16002 -u root -p111)
-- ============================================================================

-- USE test_hang;
--
-- -- 方式 1: 创建同名表 → lockMoTable 等待 CN1 的排他锁 → 阻塞
-- CREATE TABLE t1 (id BIGINT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(100));
--
-- -- 方式 2: 创建不同表 → 应该成功（CN2 有自己的 allocator）
-- CREATE TABLE t3 (id BIGINT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(100));

-- 验证：
--   1. Terminal 2 卡住超过 5 分钟（超过 defaultAllocateTimeout 的 3 分钟）→ 成功复现
--   2. 检查日志：Terminal 2 只有 leak detection，没有 timeout → 和生产一致
--   3. Terminal 3 方式 2 立即成功 → 证明是 CN1 allocator 问题
--   4. Terminal 3 方式 1 阻塞 → 证明 lockMoTable 跨 CN 互斥

-- 清理：
--   DROP DATABASE IF EXISTS test_hang;
--   SELECT fault_inject('all.', 'REMOVE_FAULT_POINT', 'lock_mo_table_def_changed');
--   SELECT fault_inject('all.', 'REMOVE_FAULT_POINT', 'incrservice_after_for_update');
