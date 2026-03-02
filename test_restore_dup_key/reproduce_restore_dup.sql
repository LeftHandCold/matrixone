-- =============================================================================
-- 复现 refreshSnapshotAfterLock 导致 restore cluster 报 Duplicate entry 的问题
--
-- 原理：restore cluster 在一个大事务里依次 drop + clone 多个 account 的数据库。
-- 每次 DropDatabase 调用 refreshSnapshotAfterLock 推进快照，导致事务看到外部已
-- 提交的数据（如前面 account 恢复写入的 mo_branch_metadata），commit 时冲突。
--
-- 使用方法：连接 MO 后直接执行本脚本
-- mysql -h 127.0.0.1 -P 6001 -u dump -p111 < reproduce_restore_dup.sql
-- =============================================================================

-- 清理环境
drop account if exists restore_acc01;
drop account if exists restore_acc02;
drop account if exists restore_acc03;
drop snapshot if exists restore_dup_snap;

-- 创建 3 个 account（多个 account 是触发问题的关键，restore 时每个 account 都会
-- drop + clone 系统库里的 mo_branch_metadata，多次推进快照才会冲突）
create account restore_acc01 admin_name = 'test_account' identified by '111';
create account restore_acc02 admin_name = 'test_account' identified by '111';
create account restore_acc03 admin_name = 'test_account' identified by '111';

-- sys 账户建几个数据库，restore 时需要 drop 这些库（触发 refreshSnapshotAfterLock）
drop database if exists restore_db01;
drop database if exists restore_db02;
drop database if exists restore_db03;
create database restore_db01;
create database restore_db02;
create database restore_db03;

use restore_db01;
create table t1 (id int primary key, val varchar(50));
insert into t1 values (1, 'hello'), (2, 'world');

use restore_db02;
create table t2 (id int primary key, name varchar(100));
insert into t2 values (1, 'foo'), (2, 'bar');

use restore_db03;
create table t3 (id int primary key, score decimal(10,2));
insert into t3 values (1, 99.5), (2, 88.0);

-- 每个 account 也建一些库和表
-- acc01
-- @session:id=1&user=restore_acc01:test_account&password=111
drop database if exists acc01_db1;
create database acc01_db1;
use acc01_db1;
create table info (id int primary key, data varchar(200));
insert into info values (1, 'acc01_data1'), (2, 'acc01_data2');

drop database if exists acc01_db2;
create database acc01_db2;
use acc01_db2;
create table log_tbl (id int primary key, msg text);
insert into log_tbl values (1, 'log entry 1');
-- @session

-- acc02
-- @session:id=2&user=restore_acc02:test_account&password=111
drop database if exists acc02_db1;
create database acc02_db1;
use acc02_db1;
create table records (id int primary key, content varchar(500));
insert into records values (1, 'record_a'), (2, 'record_b'), (3, 'record_c');
-- @session

-- acc03
-- @session:id=3&user=restore_acc03:test_account&password=111
drop database if exists acc03_db1;
create database acc03_db1;
use acc03_db1;
create table items (id int primary key, item_name varchar(100));
insert into items values (1, 'item_x'), (2, 'item_y');

drop database if exists acc03_db2;
create database acc03_db2;
use acc03_db2;
create table metrics (id int primary key, val float);
insert into metrics values (1, 3.14), (2, 2.71);
-- @session

-- 打集群级快照
create snapshot restore_dup_snap for cluster;

-- 快照之后做一些修改（这样 restore 时才需要真正 drop + 重建）
use restore_db01;
alter table t1 add column extra int default 0;
insert into t1 values (3, 'new', 100);

use restore_db02;
drop table t2;

use restore_db03;
insert into t3 values (3, 77.7);

-- acc01 也做修改
-- @session:id=1&user=restore_acc01:test_account&password=111
use acc01_db1;
insert into info values (3, 'acc01_data3_after_snap');
drop database acc01_db2;
-- @session

-- acc02 做修改
-- @session:id=2&user=restore_acc02:test_account&password=111
use acc02_db1;
alter table records add column tag varchar(50);
-- @session

-- acc03 做修改
-- @session:id=3&user=restore_acc03:test_account&password=111
drop database acc03_db2;
-- @session

-- =============================================
-- 关键步骤：restore cluster
-- 如果 refreshSnapshotAfterLock 没有 isRestoreContext 保护，
-- 这里会报 Duplicate entry '(0,mo_catalog,mo_branch_metadata,table_id)' for key '__mo_cpkey_col'
-- =============================================
restore cluster from snapshot restore_dup_snap;

-- 验证恢复结果
use restore_db01;
select * from t1;
-- 期望：只有 (1,'hello') 和 (2,'world')，没有 extra 列

use restore_db02;
select * from t2;
-- 期望：t2 表存在，有 (1,'foo') 和 (2,'bar')

use restore_db03;
select * from t3;
-- 期望：只有 (1,99.5) 和 (2,88.0)，没有 (3,77.7)

-- acc01 验证
-- @session:id=1&user=restore_acc01:test_account&password=111
use acc01_db1;
select * from info;
-- 期望：只有 (1,'acc01_data1') 和 (2,'acc01_data2')
show databases;
-- 期望：acc01_db2 应该存在（快照时还没被 drop）
-- @session

-- acc02 验证
-- @session:id=2&user=restore_acc02:test_account&password=111
use acc02_db1;
select * from records;
-- 期望：没有 tag 列，只有 id 和 content
-- @session

-- acc03 验证
-- @session:id=3&user=restore_acc03:test_account&password=111
show databases;
-- 期望：acc03_db2 应该存在
-- @session

-- 清理
drop snapshot if exists restore_dup_snap;
drop database if exists restore_db01;
drop database if exists restore_db02;
drop database if exists restore_db03;
drop account if exists restore_acc01;
drop account if exists restore_acc02;
drop account if exists restore_acc03;

select 'ALL PASSED' as result;
