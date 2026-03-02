package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

// 复现 refreshSnapshotAfterLock 推进快照导致同一事务内 DDL 冲突的问题
//
// 方案 1 (txn): 在一个显式事务里执行多次 drop database，触发多次快照推进
// 方案 2 (restore): 用 restore cluster 触发（restore 内部在一个事务里执行多次 drop database）
//
// 用法:
//   go run main.go txn       — 方案1: 显式事务内多次 DDL
//   go run main.go restore   — 方案2: setup + restore cluster
//   go run main.go cleanup   — 清理

func main() {
	host := envOr("MO_HOST", "127.0.0.1:6001")
	user := envOr("MO_USER", "dump")
	pass := envOr("MO_PASS", "111")

	if len(os.Args) < 2 {
		fmt.Println("用法: go run main.go <txn|restore|cleanup>")
		os.Exit(1)
	}

	sysDSN := fmt.Sprintf("%s:%s@tcp(%s)/", user, pass, host)
	sysDB := mustOpen(sysDSN)
	defer sysDB.Close()

	switch os.Args[1] {
	case "txn":
		doTxnTest(sysDB)
	case "restore":
		doRestoreTest(sysDB, host)
	case "cleanup":
		doCleanup(sysDB)
	default:
		log.Fatalf("未知命令: %s", os.Args[1])
	}
}

// doTxnTest 在一个显式事务里执行多次 drop database
// 每次 drop database 都会触发 refreshSnapshotAfterLock 推进快照
// 如果快照推进导致事务看到外部已提交的数据，commit 时会冲突
func doTxnTest(sysDB *sql.DB) {
	log.Println("========== 方案1: 显式事务内多次 DDL ==========")

	log.Println("准备：创建多个数据库...")
	for i := 1; i <= 5; i++ {
		dbName := fmt.Sprintf("txn_test_db%02d", i)
		mustExec(sysDB, "drop database if exists "+dbName)
		mustExec(sysDB, "create database "+dbName)
		mustExec(sysDB, fmt.Sprintf("use %s", dbName))
		mustExec(sysDB, fmt.Sprintf("create table t1 (id int primary key, val varchar(50))"))
		mustExec(sysDB, fmt.Sprintf("insert into t1 values (%d, 'data_%d')", i, i))
	}
	log.Println("准备完成")

	log.Println("开始显式事务，在事务内执行多次 drop database...")
	tx, err := sysDB.Begin()
	if err != nil {
		log.Fatalf("begin 失败: %v", err)
	}

	for i := 1; i <= 5; i++ {
		dbName := fmt.Sprintf("txn_test_db%02d", i)
		log.Printf("  drop database %s ...", dbName)
		_, err := tx.Exec("drop database if exists " + dbName)
		if err != nil {
			log.Printf("  drop database %s 失败: %v", dbName, err)
			tx.Rollback()
			return
		}
	}

	log.Println("commit...")
	err = tx.Commit()
	if err != nil {
		if strings.Contains(err.Error(), "Duplicate entry") ||
			strings.Contains(err.Error(), "__mo_cpkey_col") {
			log.Printf("*** 复现成功 *** commit 报错: %v", err)
			return
		}
		log.Printf("commit 报了其他错误: %v", err)
		return
	}
	log.Println("commit 成功，没有复现")
	log.Println("检查 mo-service.log 里的 [DIAG] refreshSnapshotAfterLock 日志")
}

// doRestoreTest 用 restore cluster 触发
func doRestoreTest(sysDB *sql.DB, host string) {
	log.Println("========== 方案2: restore cluster ==========")

	log.Println("清理残留...")
	doCleanup(sysDB)

	log.Println("创建 4 个 account + 多个数据库...")
	mustExec(sysDB, "create account rd_acc01 admin_name = 'test_account' identified by '111'")
	mustExec(sysDB, "create account rd_acc02 admin_name = 'test_account' identified by '111'")
	mustExec(sysDB, "create account rd_acc03 admin_name = 'test_account' identified by '111'")
	mustExec(sysDB, "create account rd_acc04 admin_name = 'test_account' identified by '111'")

	// sys 建多个库
	for i := 1; i <= 5; i++ {
		db := fmt.Sprintf("rd_db%02d", i)
		mustExec(sysDB, "drop database if exists "+db)
		mustExec(sysDB, "create database "+db)
		mustExec(sysDB, "use "+db)
		mustExec(sysDB, "create table t1 (id int primary key, val varchar(50))")
		mustExec(sysDB, fmt.Sprintf("insert into t1 values (%d, 'data_%d')", i, i))
	}

	// 各 account 建库
	for _, acc := range []string{"rd_acc01", "rd_acc02", "rd_acc03", "rd_acc04"} {
		setupAccount(host, acc, []string{
			"create database acc_db1",
			"use acc_db1",
			"create table info (id int primary key, data varchar(200))",
			"insert into info values (1,'data1'),(2,'data2')",
			"create database acc_db2",
			"use acc_db2",
			"create table log_tbl (id int primary key, msg text)",
			"insert into log_tbl values (1,'log1')",
		})
	}

	log.Println("第一次快照 + restore（让存储层有历史数据）...")
	mustExec(sysDB, "drop snapshot if exists rd_snap1")
	mustExec(sysDB, "create snapshot rd_snap1 for cluster")

	// 修改
	mustExec(sysDB, "use rd_db01")
	mustExec(sysDB, "alter table t1 add column extra int default 0")
	for _, acc := range []string{"rd_acc01", "rd_acc02"} {
		setupAccount(host, acc, []string{
			"drop database acc_db2",
		})
	}

	log.Println("第一次 restore cluster...")
	_, err := sysDB.Exec("restore cluster from snapshot rd_snap1")
	if err != nil {
		log.Fatalf("第一次 restore 失败: %v", err)
	}
	log.Println("第一次 restore 成功")

	// 手动触发 checkpoint
	log.Println("触发 checkpoint（让数据落盘）...")
	sysDB.Exec("select mo_ctl('dn', 'flush', '')")
	sysDB.Exec("select mo_ctl('dn', 'checkpoint', '')")

	log.Println("第二次快照 + 修改...")
	mustExec(sysDB, "drop snapshot if exists rd_snap2")
	mustExec(sysDB, "create snapshot rd_snap2 for cluster")

	mustExec(sysDB, "use rd_db01")
	mustExec(sysDB, "alter table t1 add column extra2 int default 0")
	mustExec(sysDB, "use rd_db02")
	mustExec(sysDB, "alter table t1 add column remark varchar(100)")
	for _, acc := range []string{"rd_acc03", "rd_acc04"} {
		setupAccount(host, acc, []string{
			"drop database acc_db2",
		})
	}

	log.Println("第二次 restore cluster（关键步骤）...")
	log.Println("检查 mo-service.log 里的 [DIAG] refreshSnapshotAfterLock 日志")
	_, err = sysDB.Exec("restore cluster from snapshot rd_snap2")
	if err != nil {
		if strings.Contains(err.Error(), "Duplicate entry") ||
			strings.Contains(err.Error(), "__mo_cpkey_col") {
			log.Printf("*** 复现成功 *** restore cluster 报错: %v", err)
			return
		}
		log.Fatalf("restore cluster 报了其他错误: %v", err)
	}
	log.Println("第二次 restore 成功，没有复现")

	log.Println("第三次快照 + 修改...")
	mustExec(sysDB, "drop snapshot if exists rd_snap3")
	mustExec(sysDB, "create snapshot rd_snap3 for cluster")

	mustExec(sysDB, "use rd_db03")
	mustExec(sysDB, "alter table t1 add column extra3 int default 0")
	for _, acc := range []string{"rd_acc01", "rd_acc02"} {
		setupAccount(host, acc, []string{
			"use acc_db1",
			"insert into info values (3,'data3')",
		})
	}

	log.Println("第三次 restore cluster...")
	_, err = sysDB.Exec("restore cluster from snapshot rd_snap3")
	if err != nil {
		if strings.Contains(err.Error(), "Duplicate entry") ||
			strings.Contains(err.Error(), "__mo_cpkey_col") {
			log.Printf("*** 复现成功 *** restore cluster 报错: %v", err)
			return
		}
		log.Fatalf("restore cluster 报了其他错误: %v", err)
	}
	log.Println("第三次 restore 成功，没有复现")
	log.Println("检查 mo-service.log 里的 [DIAG] 日志分析快照推进情况")
}

func doCleanup(sysDB *sql.DB) {
	log.Println("清理...")
	for _, s := range []string{
		"drop snapshot if exists rd_snap1",
		"drop snapshot if exists rd_snap2",
		"drop snapshot if exists rd_snap3",
	} {
		sysDB.Exec(s)
	}
	for i := 1; i <= 5; i++ {
		sysDB.Exec(fmt.Sprintf("drop database if exists rd_db%02d", i))
		sysDB.Exec(fmt.Sprintf("drop database if exists txn_test_db%02d", i))
	}
	for _, acc := range []string{"rd_acc01", "rd_acc02", "rd_acc03", "rd_acc04"} {
		sysDB.Exec("drop account if exists " + acc)
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
