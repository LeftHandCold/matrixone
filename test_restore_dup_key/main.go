package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

// 复现 refreshSnapshotAfterLock 导致 restore cluster 报 Duplicate entry 的问题
//
// 触发条件：老 MO 跑过一次 restore cluster（mo_columns 里已有 mo_branch_metadata
// 的列定义），换成带 refreshSnapshotAfterLock 的新 MO 后再跑 restore cluster，
// 快照推进导致事务看到存储层里老数据，insert 同样的 cpkey 产生冲突。
//
// 用法：
//   Step 1（老 MO）: go run main.go setup    — 创建环境 + 打快照 + 做修改 + restore
//   Step 2（新 MO）: go run main.go restore   — 再打快照 + 做修改 + restore（触发 bug）
//   清理:           go run main.go cleanup
//
// 默认连接 127.0.0.1:6001 dump/111，可通过环境变量 MO_HOST MO_USER MO_PASS 覆盖

func main() {
	host := envOr("MO_HOST", "127.0.0.1:6001")
	user := envOr("MO_USER", "dump")
	pass := envOr("MO_PASS", "111")

	if len(os.Args) < 2 {
		fmt.Println("用法: go run main.go <setup|restore|cleanup|all>")
		fmt.Println("  setup   — 第一轮：创建环境 + 打快照 + 修改 + restore（模拟老 MO）")
		fmt.Println("  restore — 第二轮：再打快照 + 修改 + restore（模拟新 MO，触发 bug）")
		fmt.Println("  cleanup — 清理所有测试数据")
		fmt.Println("  all     — 一次性跑完 setup + restore（同一个 MO 上测试）")
		os.Exit(1)
	}

	sysDSN := fmt.Sprintf("%s:%s@tcp(%s)/", user, pass, host)
	sysDB := mustOpen(sysDSN)
	defer sysDB.Close()

	switch os.Args[1] {
	case "setup":
		doSetup(sysDB, host)
	case "restore":
		doRestore(sysDB, host)
	case "cleanup":
		doCleanup(sysDB)
	case "all":
		doSetup(sysDB, host)
		doRestore(sysDB, host)
		doCleanup(sysDB)
	default:
		log.Fatalf("未知命令: %s", os.Args[1])
	}
}

// doSetup 模拟老 MO 的第一轮 BVT：创建环境 → 打快照 → 修改 → restore cluster
func doSetup(sysDB *sql.DB, host string) {
	log.Println("========== Step 1: Setup (模拟老 MO 第一轮 BVT) ==========")

	log.Println("清理残留...")
	doCleanup(sysDB)

	log.Println("创建 4 个 account...")
	mustExec(sysDB, "create account rd_acc01 admin_name = 'test_account' identified by '111'")
	mustExec(sysDB, "create account rd_acc02 admin_name = 'test_account' identified by '111'")
	mustExec(sysDB, "create account rd_acc03 admin_name = 'test_account' identified by '111'")
	mustExec(sysDB, "create account rd_acc04 admin_name = 'test_account' identified by '111'")

	log.Println("sys 建库建表...")
	for _, db := range []string{"rd_db01", "rd_db02", "rd_db03"} {
		mustExec(sysDB, "drop database if exists "+db)
		mustExec(sysDB, "create database "+db)
	}
	mustExec(sysDB, "use rd_db01")
	mustExec(sysDB, "create table t1 (id int primary key, val varchar(50))")
	mustExec(sysDB, "insert into t1 values (1,'hello'),(2,'world')")
	mustExec(sysDB, "use rd_db02")
	mustExec(sysDB, "create table t2 (id int primary key, name varchar(100))")
	mustExec(sysDB, "insert into t2 values (1,'foo'),(2,'bar')")
	mustExec(sysDB, "use rd_db03")
	mustExec(sysDB, "create table t3 (id int primary key, score decimal(10,2))")
	mustExec(sysDB, "insert into t3 values (1,99.5),(2,88.0)")

	log.Println("各 account 建库建表...")
	setupAccount(host, "rd_acc01", []string{
		"create database acc01_db1",
		"use acc01_db1",
		"create table info (id int primary key, data varchar(200))",
		"insert into info values (1,'acc01_data1'),(2,'acc01_data2')",
		"create database acc01_db2",
		"use acc01_db2",
		"create table log_tbl (id int primary key, msg text)",
		"insert into log_tbl values (1,'log entry 1')",
	})
	setupAccount(host, "rd_acc02", []string{
		"create database acc02_db1",
		"use acc02_db1",
		"create table records (id int primary key, content varchar(500))",
		"insert into records values (1,'record_a'),(2,'record_b'),(3,'record_c')",
	})
	setupAccount(host, "rd_acc03", []string{
		"create database acc03_db1",
		"use acc03_db1",
		"create table items (id int primary key, item_name varchar(100))",
		"insert into items values (1,'item_x'),(2,'item_y')",
		"create database acc03_db2",
		"use acc03_db2",
		"create table metrics (id int primary key, val float)",
		"insert into metrics values (1,3.14),(2,2.71)",
	})
	setupAccount(host, "rd_acc04", []string{
		"create database acc04_db1",
		"use acc04_db1",
		"create table docs (id int primary key, title varchar(200))",
		"insert into docs values (1,'doc_a'),(2,'doc_b')",
	})

	log.Println("打第一次集群快照 rd_snap1...")
	mustExec(sysDB, "drop snapshot if exists rd_snap1")
	mustExec(sysDB, "create snapshot rd_snap1 for cluster")

	log.Println("快照后做修改...")
	mustExec(sysDB, "use rd_db01")
	mustExec(sysDB, "alter table t1 add column extra int default 0")
	mustExec(sysDB, "use rd_db02")
	mustExec(sysDB, "drop table t2")
	setupAccount(host, "rd_acc01", []string{
		"use acc01_db1",
		"insert into info values (3,'after_snap1')",
		"drop database acc01_db2",
	})
	setupAccount(host, "rd_acc03", []string{
		"drop database acc03_db2",
	})

	log.Println("第一次 restore cluster（模拟老 MO 正常 restore）...")
	_, err := sysDB.Exec("restore cluster from snapshot rd_snap1")
	if err != nil {
		log.Fatalf("第一次 restore cluster 失败: %v", err)
	}
	log.Println("第一次 restore cluster 成功")
	// 此时 mo_columns 里已经有了 mo_branch_metadata 的列定义（已提交到存储层）

	log.Println("========== Step 1 完成 ==========")
	log.Println("现在可以停掉老 MO，换成新 MO 启动，然后运行: go run main.go restore")
}

// doRestore 模拟新 MO 的第二轮 BVT：再打快照 → 修改 → restore cluster（触发 bug）
func doRestore(sysDB *sql.DB, host string) {
	log.Println("========== Step 2: Restore (模拟新 MO 第二轮 BVT) ==========")

	log.Println("快照后再做一些修改...")
	mustExec(sysDB, "use rd_db01")
	mustExec(sysDB, "insert into t1 values (3,'new_data')")
	mustExec(sysDB, "use rd_db03")
	mustExec(sysDB, "insert into t3 values (3,77.7)")
	setupAccount(host, "rd_acc02", []string{
		"use acc02_db1",
		"alter table records add column tag varchar(50)",
	})
	setupAccount(host, "rd_acc04", []string{
		"use acc04_db1",
		"insert into docs values (3,'doc_c_after')",
	})

	log.Println("打第二次集群快照 rd_snap2...")
	mustExec(sysDB, "drop snapshot if exists rd_snap2")
	mustExec(sysDB, "create snapshot rd_snap2 for cluster")

	log.Println("快照后再修改...")
	mustExec(sysDB, "use rd_db01")
	mustExec(sysDB, "alter table t1 add column extra2 int default 0")
	mustExec(sysDB, "use rd_db02")
	mustExec(sysDB, "alter table t2 add column remark varchar(100)")
	setupAccount(host, "rd_acc01", []string{
		"use acc01_db1",
		"insert into info values (4,'after_snap2')",
	})
	setupAccount(host, "rd_acc03", []string{
		"use acc03_db1",
		"insert into items values (3,'item_z_after')",
	})

	log.Println("第二次 restore cluster（关键步骤）...")
	log.Println("如果 refreshSnapshotAfterLock 没有 isRestoreContext 保护，")
	log.Println("快照推进后事务会看到第一轮 restore 留在 mo_columns 里的 mo_branch_metadata 列定义，")
	log.Println("导致 Duplicate entry 错误")
	_, err := sysDB.Exec("restore cluster from snapshot rd_snap2")
	if err != nil {
		if strings.Contains(err.Error(), "Duplicate entry") ||
			strings.Contains(err.Error(), "__mo_cpkey_col") {
			log.Printf("*** 复现成功 *** restore cluster 报错: %v", err)
			log.Println("这就是 refreshSnapshotAfterLock 在 restore 场景下推进快照导致的问题")
			os.Exit(1)
		}
		log.Fatalf("restore cluster 报了其他错误: %v", err)
	}

	log.Println("第二次 restore cluster 成功（说明 isRestoreContext 保护生效了）")

	log.Println("验证数据...")
	passed := true
	passed = verify(sysDB, "rd_db01", "select count(*) from t1", "2") && passed
	passed = verify(sysDB, "rd_db03", "select count(*) from t3", "3") && passed
	passed = verifyAccount(host, "rd_acc01", "acc01_db1", "select count(*) from info", "3") && passed
	passed = verifyAccount(host, "rd_acc02", "acc02_db1", "select count(*) from records", "3") && passed
	passed = verifyAccount(host, "rd_acc03", "acc03_db1", "select count(*) from items", "2") && passed
	passed = verifyAccount(host, "rd_acc04", "acc04_db1", "select count(*) from docs", "3") && passed

	if passed {
		log.Println("所有验证通过")
	} else {
		log.Println("部分验证失败")
	}

	log.Println("========== Step 2 完成 ==========")
}

func doCleanup(sysDB *sql.DB) {
	log.Println("清理...")
	for _, s := range []string{
		"drop snapshot if exists rd_snap1",
		"drop snapshot if exists rd_snap2",
		"drop database if exists rd_db01",
		"drop database if exists rd_db02",
		"drop database if exists rd_db03",
		"drop account if exists rd_acc01",
		"drop account if exists rd_acc02",
		"drop account if exists rd_acc03",
		"drop account if exists rd_acc04",
	} {
		sysDB.Exec(s)
	}
}

// --- 工具函数 ---

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func mustOpen(dsn string) *sql.DB {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatalf("连接失败 %s: %v", dsn, err)
	}
	db.SetMaxOpenConns(5)
	if err := db.Ping(); err != nil {
		log.Fatalf("ping 失败 %s: %v", dsn, err)
	}
	return db
}

func mustExec(db *sql.DB, query string) {
	_, err := db.Exec(query)
	if err != nil {
		log.Fatalf("执行失败 [%s]: %v", query, err)
	}
}

func setupAccount(host, accName string, sqls []string) {
	dsn := fmt.Sprintf("%s#test_account:111@tcp(%s)/", accName, host)
	db := mustOpen(dsn)
	defer db.Close()
	for _, s := range sqls {
		mustExec(db, s)
	}
}

func verify(db *sql.DB, dbName, query, expected string) bool {
	db.Exec("use " + dbName)
	var result string
	err := db.QueryRow(query).Scan(&result)
	if err != nil {
		log.Printf("验证失败 [%s.%s]: %v", dbName, query, err)
		return false
	}
	if result != expected {
		log.Printf("验证失败 [%s.%s]: 期望 %s, 实际 %s", dbName, query, expected, result)
		return false
	}
	log.Printf("验证通过 [%s]: %s = %s", dbName, query, result)
	return true
}

func verifyAccount(host, accName, dbName, query, expected string) bool {
	dsn := fmt.Sprintf("%s#test_account:111@tcp(%s)/%s", accName, host, dbName)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Printf("验证失败 连接 %s: %v", accName, err)
		return false
	}
	defer db.Close()
	var result string
	err = db.QueryRow(query).Scan(&result)
	if err != nil {
		log.Printf("验证失败 [%s/%s.%s]: %v", accName, dbName, query, err)
		return false
	}
	if result != expected {
		log.Printf("验证失败 [%s/%s.%s]: 期望 %s, 实际 %s", accName, dbName, query, expected, result)
		return false
	}
	log.Printf("验证通过 [%s/%s]: %s = %s", accName, dbName, query, result)
	return true
}
