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
// 原理：restore cluster 在一个大事务里依次恢复多个 account 的数据库。
// 每次 DropDatabase 调用 refreshSnapshotAfterLock 推进快照，导致事务看到
// 外部已提交的数据（如前面 account 恢复写入的 mo_branch_metadata），
// commit 时产生 Duplicate entry 冲突。
//
// 用法：go run main.go [host:port] [user] [password]
// 默认：127.0.0.1:6001 dump 111

func main() {
	host := "127.0.0.1:6001"
	user := "dump"
	pass := "111"
	if len(os.Args) > 1 {
		host = os.Args[1]
	}
	if len(os.Args) > 2 {
		user = os.Args[2]
	}
	if len(os.Args) > 3 {
		pass = os.Args[3]
	}

	sysDSN := fmt.Sprintf("%s:%s@tcp(%s)/", user, pass, host)
	sysDB := mustOpen(sysDSN)
	defer sysDB.Close()

	log.Println("=== 清理环境 ===")
	cleanup(sysDB)

	log.Println("=== 创建 3 个 account ===")
	mustExec(sysDB, "create account restore_acc01 admin_name = 'test_account' identified by '111'")
	mustExec(sysDB, "create account restore_acc02 admin_name = 'test_account' identified by '111'")
	mustExec(sysDB, "create account restore_acc03 admin_name = 'test_account' identified by '111'")

	log.Println("=== sys 账户建库建表 ===")
	mustExec(sysDB, "drop database if exists restore_db01")
	mustExec(sysDB, "drop database if exists restore_db02")
	mustExec(sysDB, "drop database if exists restore_db03")
	mustExec(sysDB, "create database restore_db01")
	mustExec(sysDB, "create database restore_db02")
	mustExec(sysDB, "create database restore_db03")
	mustExec(sysDB, "use restore_db01")
	mustExec(sysDB, "create table t1 (id int primary key, val varchar(50))")
	mustExec(sysDB, "insert into t1 values (1,'hello'),(2,'world')")
	mustExec(sysDB, "use restore_db02")
	mustExec(sysDB, "create table t2 (id int primary key, name varchar(100))")
	mustExec(sysDB, "insert into t2 values (1,'foo'),(2,'bar')")
	mustExec(sysDB, "use restore_db03")
	mustExec(sysDB, "create table t3 (id int primary key, score decimal(10,2))")
	mustExec(sysDB, "insert into t3 values (1,99.5),(2,88.0)")

	log.Println("=== 各 account 建库建表 ===")
	setupAccount(host, "restore_acc01", []string{
		"create database acc01_db1",
		"use acc01_db1",
		"create table info (id int primary key, data varchar(200))",
		"insert into info values (1,'acc01_data1'),(2,'acc01_data2')",
		"create database acc01_db2",
		"use acc01_db2",
		"create table log_tbl (id int primary key, msg text)",
		"insert into log_tbl values (1,'log entry 1')",
	})
	setupAccount(host, "restore_acc02", []string{
		"create database acc02_db1",
		"use acc02_db1",
		"create table records (id int primary key, content varchar(500))",
		"insert into records values (1,'record_a'),(2,'record_b'),(3,'record_c')",
	})
	setupAccount(host, "restore_acc03", []string{
		"create database acc03_db1",
		"use acc03_db1",
		"create table items (id int primary key, item_name varchar(100))",
		"insert into items values (1,'item_x'),(2,'item_y')",
		"create database acc03_db2",
		"use acc03_db2",
		"create table metrics (id int primary key, val float)",
		"insert into metrics values (1,3.14),(2,2.71)",
	})

	log.Println("=== 打集群级快照 ===")
	mustExec(sysDB, "drop snapshot if exists restore_dup_snap")
	mustExec(sysDB, "create snapshot restore_dup_snap for cluster")

	log.Println("=== 快照后做修改（让 restore 需要真正 drop + 重建）===")
	mustExec(sysDB, "use restore_db01")
	mustExec(sysDB, "alter table t1 add column extra int default 0")
	mustExec(sysDB, "insert into t1 values (3,'new',100)")
	mustExec(sysDB, "use restore_db02")
	mustExec(sysDB, "drop table t2")
	mustExec(sysDB, "use restore_db03")
	mustExec(sysDB, "insert into t3 values (3,77.7)")

	// acc01 修改
	setupAccount(host, "restore_acc01", []string{
		"use acc01_db1",
		"insert into info values (3,'acc01_data3_after_snap')",
		"drop database acc01_db2",
	})
	// acc02 修改
	setupAccount(host, "restore_acc02", []string{
		"use acc02_db1",
		"alter table records add column tag varchar(50)",
	})
	// acc03 修改
	setupAccount(host, "restore_acc03", []string{
		"drop database acc03_db2",
	})

	log.Println("=== 执行 restore cluster（关键步骤）===")
	log.Println("如果没有 isRestoreContext 保护，这里会报 Duplicate entry 错误")
	_, err := sysDB.Exec("restore cluster from snapshot restore_dup_snap")
	if err != nil {
		if strings.Contains(err.Error(), "Duplicate entry") ||
			strings.Contains(err.Error(), "__mo_cpkey_col") {
			log.Printf("*** 复现成功 *** restore cluster 报错: %v", err)
			log.Println("这就是 refreshSnapshotAfterLock 在 restore 场景下推进快照导致的问题")
			cleanupFinal(sysDB)
			os.Exit(1)
		}
		log.Fatalf("restore cluster 报了其他错误: %v", err)
	}

	log.Println("=== restore cluster 成功，验证数据 ===")
	passed := true

	// 验证 sys 库
	passed = verify(sysDB, "restore_db01", "select count(*) from t1", "2") && passed
	passed = verify(sysDB, "restore_db02", "select count(*) from t2", "2") && passed
	passed = verify(sysDB, "restore_db03", "select count(*) from t3", "2") && passed

	// 验证 acc01
	passed = verifyAccount(host, "restore_acc01", "acc01_db1", "select count(*) from info", "2") && passed

	// 验证 acc02
	passed = verifyAccount(host, "restore_acc02", "acc02_db1", "select count(*) from records", "3") && passed

	// 验证 acc03
	passed = verifyAccount(host, "restore_acc03", "acc03_db1", "select count(*) from items", "2") && passed

	if passed {
		log.Println("=== 所有验证通过，restore cluster 正常工作 ===")
	} else {
		log.Println("=== 部分验证失败 ===")
	}

	cleanupFinal(sysDB)
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
	// MO account 连接方式：用户名格式为 account_name:user_name
	// 用户名里有冒号，必须用 mysql.Config 构建 DSN，否则 go-sql-driver 解析错误
	db := mustOpenAccount(host, accName, "")
	defer db.Close()
	for _, s := range sqls {
		mustExec(db, s)
	}
}

func cleanup(db *sql.DB) {
	for _, s := range []string{
		"drop snapshot if exists restore_dup_snap",
		"drop database if exists restore_db01",
		"drop database if exists restore_db02",
		"drop database if exists restore_db03",
		"drop account if exists restore_acc01",
		"drop account if exists restore_acc02",
		"drop account if exists restore_acc03",
	} {
		db.Exec(s)
	}
}

func cleanupFinal(db *sql.DB) {
	log.Println("=== 清理 ===")
	cleanup(db)
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
	db, err := sql.Open("mysql", buildAccountDSN(host, accName, dbName))
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

// buildAccountDSN 构建 account 连接的 DSN
// MO 支持 tenant#user 格式，用 # 分隔可以避免与 DSN 里的冒号冲突
func buildAccountDSN(host, accName, dbName string) string {
	// 用户名格式：account#user，密码单独传
	user := accName + "#test_account"
	return fmt.Sprintf("%s:111@tcp(%s)/%s", user, host, dbName)
}

func mustOpenAccount(host, accName, dbName string) *sql.DB {
	dsn := buildAccountDSN(host, accName, dbName)
	return mustOpen(dsn)
}
