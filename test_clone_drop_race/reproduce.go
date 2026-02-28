// reproduce.go — 复现 CLONE + DROP DATABASE 竞态条件
//
// 配合 MO 代码中的 hack 使用：
//   在 pkg/sql/compile/ddl.go 的 DropDatabase 中，lockMoDatabase 之后、
//   snapshot 刷新之前注入了环境变量控制的 sleep。
//
// 使用步骤：
//   1. 编译并启动 MO（设置环境变量 MO_DROP_DB_DELAY_MS=200）
//   2. 运行本工具：go run reproduce.go -host 127.0.0.1 -port 6001
//
// 原理：
//   - DROP DATABASE 拿到排他锁后 sleep 200ms
//   - 这 200ms 内 CLONE 的子事务继续提交新表
//   - sleep 结束后 GetLatestCommitTS() 刷新快照，但由于是 CN 本地值，
//     在多 CN 下可能不够新；在单 CN 下也可能因为 logtail 延迟不够新
//   - Relations() 用过时快照查询，漏掉新表 → 孤儿表
//
// 检测方法：
//   drop database 完成后，用新连接查 mo_catalog.mo_tables 是否有残留记录

package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

var (
	host      = flag.String("host", "127.0.0.1", "MO host")
	port      = flag.Int("port", 6001, "MO port")
	user      = flag.String("user", "root", "MO user")
	pass      = flag.String("pass", "111", "MO password")
	rounds    = flag.Int("rounds", 50, "number of test rounds")
	numTables = flag.Int("tables", 15, "number of tables in source db")
	srcDB     = flag.String("srcdb", "clone_race_src", "source database name")
)

func dsn() string {
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/?timeout=60s&readTimeout=120s&writeTimeout=120s&interpolateParams=true",
		*user, *pass, *host, *port)
}

func mustExec(db *sql.DB, query string) {
	_, err := db.Exec(query)
	if err != nil {
		log.Printf("WARN exec failed: %s — %v", query, err)
	}
}

func mustExecFatal(db *sql.DB, query string) {
	_, err := db.Exec(query)
	if err != nil {
		log.Fatalf("FATAL exec failed: %s — %v", query, err)
	}
}

func setupSourceDB(db *sql.DB) {
	log.Printf("创建源数据库 %s，%d 张表...", *srcDB, *numTables)
	mustExec(db, fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", *srcDB))
	mustExecFatal(db, fmt.Sprintf("CREATE DATABASE `%s`", *srcDB))

	for i := 0; i < *numTables; i++ {
		tblName := fmt.Sprintf("tbl_%03d", i)
		// 带多个索引 → 每个 clone table 子事务创建更多 relation
		ddl := fmt.Sprintf(`CREATE TABLE %s.%s (
			id BIGINT PRIMARY KEY AUTO_INCREMENT,
			key_name VARCHAR(255) NOT NULL,
			scope_type VARCHAR(64) NOT NULL,
			scope_user_id VARCHAR(128) DEFAULT '',
			category VARCHAR(128) DEFAULT '',
			content TEXT,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			UNIQUE KEY uk_%s (key_name, scope_type, scope_user_id),
			KEY idx_%s_cat (category),
			KEY idx_%s_created (created_at),
			KEY idx_%s_updated (updated_at)
		)`, *srcDB, tblName, tblName, tblName, tblName, tblName)
		mustExecFatal(db, ddl)

		// 插入数据
		var values []string
		for j := 0; j < 10; j++ {
			values = append(values, fmt.Sprintf(
				"('key_%d_%d', 'type_%d', 'user_%d', 'cat_%d', '%s')",
				i, j, j%5, j%10, j%3, strings.Repeat("x", 50)))
		}
		insertSQL := fmt.Sprintf("INSERT INTO %s.%s (key_name, scope_type, scope_user_id, category, content) VALUES %s",
			*srcDB, tblName, strings.Join(values, ","))
		mustExecFatal(db, insertSQL)
	}
	log.Printf("源数据库就绪：%d 张表", *numTables)
}

// checkOrphans 用全新连接检查孤儿表
func checkOrphans(dstDBName string) (dbExists bool, orphanTables []string, err error) {
	conn, err := sql.Open("mysql", dsn())
	if err != nil {
		return false, nil, err
	}
	defer conn.Close()
	conn.SetMaxOpenConns(1)

	// 检查 database 是否还存在
	var dbCount int
	err = conn.QueryRow(
		"SELECT COUNT(*) FROM mo_catalog.mo_database WHERE datname = ? AND account_id = 0",
		dstDBName).Scan(&dbCount)
	if err != nil {
		return false, nil, err
	}
	dbExists = dbCount > 0

	// 检查 mo_tables 中是否有残留
	rows, err := conn.Query(
		"SELECT reldatabase, relname, rel_id FROM mo_catalog.mo_tables WHERE reldatabase = ? AND account_id = 0",
		dstDBName)
	if err != nil {
		return dbExists, nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var db, tbl string
		var relID uint64
		if err := rows.Scan(&db, &tbl, &relID); err != nil {
			return dbExists, nil, err
		}
		orphanTables = append(orphanTables, fmt.Sprintf("%s.%s(id=%d)", db, tbl, relID))
	}
	return dbExists, orphanTables, rows.Err()
}

func runOneRound(roundID int, connPool *sql.DB) (found bool, errMsg string) {
	dstDB := fmt.Sprintf("clone_race_%d", roundID)

	mustExec(connPool, fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", dstDB))
	time.Sleep(20 * time.Millisecond)

	var wg sync.WaitGroup
	var cloneErr, dropErr error

	// goroutine A: clone database
	// CLONE 会先 create database，然后逐表 clone（每个表一个独立子事务）
	wg.Add(1)
	go func() {
		defer wg.Done()
		conn, err := sql.Open("mysql", dsn())
		if err != nil {
			cloneErr = err
			return
		}
		defer conn.Close()
		conn.SetMaxOpenConns(1)

		_, cloneErr = conn.Exec(fmt.Sprintf("CREATE DATABASE `%s` CLONE `%s`", dstDB, *srcDB))
	}()

	// goroutine B: 等目标库出现后立即 drop
	// MO 代码中的 hack 会让 DROP 在拿到锁后 sleep MO_DROP_DB_DELAY_MS 毫秒
	// 这段时间内 CLONE 的子事务继续提交新表
	wg.Add(1)
	go func() {
		defer wg.Done()
		conn, err := sql.Open("mysql", dsn())
		if err != nil {
			dropErr = err
			return
		}
		defer conn.Close()
		conn.SetMaxOpenConns(1)

		// 轮询等待目标数据库出现
		for i := 0; i < 2000; i++ {
			var count int
			err := conn.QueryRow(
				"SELECT COUNT(*) FROM mo_catalog.mo_database WHERE datname = ? AND account_id = 0",
				dstDB).Scan(&count)
			if err == nil && count > 0 {
				break
			}
			time.Sleep(1 * time.Millisecond)
		}

		// 立即 drop — MO 内部会在 lock 之后 sleep
		_, dropErr = conn.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", dstDB))
	}()

	wg.Wait()

	if cloneErr != nil {
		log.Printf("    clone error (可能正常): %v", cloneErr)
	}
	if dropErr != nil {
		log.Printf("    drop error (可能正常): %v", dropErr)
	}

	// 等 logtail 追上
	time.Sleep(200 * time.Millisecond)

	// 检查
	dbExists, orphans, err := checkOrphans(dstDB)
	if err != nil {
		mustExec(connPool, fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", dstDB))
		return false, fmt.Sprintf("check error: %v", err)
	}

	if dbExists {
		// db 还在，清理
		mustExec(connPool, fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", dstDB))
		return false, ""
	}

	// db 不在了但 mo_tables 有残留 → 孤儿表！
	if len(orphans) > 0 {
		return true, fmt.Sprintf("孤儿表！db %s 已删除但 mo_tables 残留 %d 条: %v",
			dstDB, len(orphans), orphans)
	}

	return false, ""
}

func main() {
	flag.Parse()
	rand.Seed(time.Now().UnixNano())

	log.Println("=== CLONE + DROP DATABASE 竞态条件复现工具 ===")
	log.Printf("目标: %s:%d, 轮次=%d, 表数=%d", *host, *port, *rounds, *numTables)
	log.Println("请确保 MO 启动时设置了 MO_DROP_DB_DELAY_MS=200")
	log.Println("")

	connPool, err := sql.Open("mysql", dsn())
	if err != nil {
		log.Fatalf("连接失败: %v", err)
	}
	defer connPool.Close()
	connPool.SetMaxOpenConns(5)

	if err := connPool.Ping(); err != nil {
		log.Fatalf("Ping 失败: %v", err)
	}

	setupSourceDB(connPool)

	totalOrphans := 0
	for i := 1; i <= *rounds; i++ {
		found, msg := runOneRound(i, connPool)
		if found {
			totalOrphans++
			log.Printf("!!! 第 %d/%d 轮: %s", i, *rounds, msg)
		} else if i%10 == 0 {
			log.Printf("    第 %d/%d 轮: OK (累计孤儿: %d)", i, *rounds, totalOrphans)
		}
	}

	log.Printf("=== 完成: %d/%d 轮发现孤儿表 ===", totalOrphans, *rounds)
	mustExec(connPool, fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", *srcDB))

	if totalOrphans > 0 {
		log.Printf("BUG 已复现！%d 轮出现孤儿表", totalOrphans)
	} else {
		log.Println("未复现。检查 MO 是否设置了 MO_DROP_DB_DELAY_MS 环境变量。")
	}
}
