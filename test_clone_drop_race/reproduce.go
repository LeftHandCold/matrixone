// reproduce.go — 复现 CLONE + DROP DATABASE 竞态条件
//
// 配合 MO 代码中的 hack 使用：
//   在 pkg/vm/engine/disttae/logtail.go 的 consumeEntry 中注入了环境变量控制的 sleep，
//   延迟 mo_tables 的 logtail apply，扩大 Relations() 查询时 logtail 尚未追上的窗口。
//
// 使用步骤：
//   1. 编译 MO（包含 hack）
//   2. 启动 MO 时设置环境变量：export MO_DELAY_CONSUME_MO_TABLES_MS=100
//   3. 运行本工具：go run reproduce.go -host 127.0.0.1 -port 6001
//
// 原理：
//   DropDatabase 的执行流程：
//     1. lockMoDatabaseAndRefreshSnapshot → 获取排他锁 + 刷新 snapshot
//     2. Relations() → 用 snapshotTS 查 mo_tables → 确定要删除的表列表
//     3. 逐个 drop table → drop database
//
//   snapshot 刷新依赖 GetLatestCommitTS() 或 WaitLogTailAppliedAt(zero)，
//   这两个值都来自 logtail consumer 的 updateTimestamp/NotifyLatestCommitTS。
//
//   注入的 sleep 延迟了 mo_tables 的 logtail apply（consumeEntry），
//   导致 updateTimestamp 也被延迟（它在所有 entries 处理完后才调用），
//   从而使 latestTS 落后于 CLONE 子事务的 commitTS。
//
//   DROP 在 CLONE 子事务提交后拿到排他锁，但 latestTS 还没追上，
//   snapshot 刷新不充分，Relations() 用过时快照查询，漏掉新表 → 孤儿表。

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
	rounds    = flag.Int("rounds", 100, "number of test rounds")
	numTables = flag.Int("tables", 15, "number of tables in source db")
	srcDB     = flag.String("srcdb", "clone_race_src", "source database name")
)

func dsn() string {
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/?timeout=60s&readTimeout=120s&writeTimeout=120s&interpolateParams=true",
		*user, *pass, *host, *port)
}

func mustExec(db *sql.DB, q string) {
	if _, err := db.Exec(q); err != nil {
		log.Printf("WARN: %s — %v", q, err)
	}
}

func mustExecFatal(db *sql.DB, q string) {
	if _, err := db.Exec(q); err != nil {
		log.Fatalf("FATAL: %s — %v", q, err)
	}
}

func setupSourceDB(db *sql.DB) {
	log.Printf("创建源数据库 %s，%d 张表...", *srcDB, *numTables)
	mustExec(db, fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", *srcDB))
	mustExecFatal(db, fmt.Sprintf("CREATE DATABASE `%s`", *srcDB))

	for i := 0; i < *numTables; i++ {
		tbl := fmt.Sprintf("tbl_%03d", i)
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
		)`, *srcDB, tbl, tbl, tbl, tbl, tbl)
		mustExecFatal(db, ddl)

		var vals []string
		for j := 0; j < 10; j++ {
			vals = append(vals, fmt.Sprintf(
				"('key_%d_%d','type_%d','user_%d','cat_%d','%s')",
				i, j, j%5, j%10, j%3, strings.Repeat("x", 50)))
		}
		mustExecFatal(db, fmt.Sprintf(
			"INSERT INTO %s.%s (key_name,scope_type,scope_user_id,category,content) VALUES %s",
			*srcDB, tbl, strings.Join(vals, ",")))
	}
	log.Printf("源数据库就绪：%d 张表", *numTables)
}

func checkOrphans(dstDBName string) (dbExists bool, orphans []string, err error) {
	conn, err := sql.Open("mysql", dsn())
	if err != nil {
		return false, nil, err
	}
	defer conn.Close()
	conn.SetMaxOpenConns(1)

	var dbCount int
	if err = conn.QueryRow(
		"SELECT COUNT(*) FROM mo_catalog.mo_database WHERE datname=? AND account_id=0",
		dstDBName).Scan(&dbCount); err != nil {
		return false, nil, err
	}
	dbExists = dbCount > 0

	rows, err := conn.Query(
		"SELECT reldatabase,relname,rel_id FROM mo_catalog.mo_tables WHERE reldatabase=? AND account_id=0",
		dstDBName)
	if err != nil {
		return dbExists, nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var db, tbl string
		var id uint64
		if err := rows.Scan(&db, &tbl, &id); err != nil {
			return dbExists, nil, err
		}
		orphans = append(orphans, fmt.Sprintf("%s.%s(id=%d)", db, tbl, id))
	}
	return dbExists, orphans, rows.Err()
}

func runOneRound(roundID int, pool *sql.DB) (found bool, msg string) {
	dst := fmt.Sprintf("clone_race_%d", roundID)
	mustExec(pool, fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", dst))
	time.Sleep(20 * time.Millisecond)

	var wg sync.WaitGroup
	var cloneErr, dropErr error

	// CLONE: create database dst clone src
	wg.Add(1)
	go func() {
		defer wg.Done()
		c, err := sql.Open("mysql", dsn())
		if err != nil {
			cloneErr = err
			return
		}
		defer c.Close()
		c.SetMaxOpenConns(1)
		_, cloneErr = c.Exec(fmt.Sprintf("CREATE DATABASE `%s` CLONE `%s`", dst, *srcDB))
	}()

	// DROP: 等 database 出现后立即 drop
	wg.Add(1)
	go func() {
		defer wg.Done()
		c, err := sql.Open("mysql", dsn())
		if err != nil {
			dropErr = err
			return
		}
		defer c.Close()
		c.SetMaxOpenConns(1)

		// 轮询等 database 出现
		for i := 0; i < 3000; i++ {
			var n int
			if err := c.QueryRow(
				"SELECT COUNT(*) FROM mo_catalog.mo_database WHERE datname=? AND account_id=0",
				dst).Scan(&n); err == nil && n > 0 {
				break
			}
			time.Sleep(1 * time.Millisecond)
		}
		_, dropErr = c.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", dst))
	}()

	wg.Wait()

	if cloneErr != nil {
		log.Printf("    clone err (可能正常): %v", cloneErr)
	}
	if dropErr != nil {
		log.Printf("    drop err (可能正常): %v", dropErr)
	}

	// 等 logtail 追上
	time.Sleep(500 * time.Millisecond)

	dbExists, orphans, err := checkOrphans(dst)
	if err != nil {
		mustExec(pool, fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", dst))
		return false, fmt.Sprintf("check err: %v", err)
	}

	if dbExists {
		mustExec(pool, fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", dst))
		return false, ""
	}

	if len(orphans) > 0 {
		return true, fmt.Sprintf("孤儿表！db %s 已删除但 mo_tables 残留 %d 条: %v",
			dst, len(orphans), orphans)
	}
	return false, ""
}

func main() {
	flag.Parse()
	rand.Seed(time.Now().UnixNano())

	log.Println("=== CLONE + DROP DATABASE 竞态条件复现工具 ===")
	log.Printf("目标: %s:%d, 轮次=%d, 表数=%d", *host, *port, *rounds, *numTables)
	log.Println("请确保 MO 启动时设置了 MO_DELAY_CONSUME_MO_TABLES_MS=100")
	log.Println("")

	pool, err := sql.Open("mysql", dsn())
	if err != nil {
		log.Fatalf("连接失败: %v", err)
	}
	defer pool.Close()
	pool.SetMaxOpenConns(5)
	if err := pool.Ping(); err != nil {
		log.Fatalf("Ping 失败: %v", err)
	}

	setupSourceDB(pool)

	total := 0
	for i := 1; i <= *rounds; i++ {
		found, msg := runOneRound(i, pool)
		if found {
			total++
			log.Printf("!!! 第 %d/%d 轮: %s", i, *rounds, msg)
		} else if i%10 == 0 {
			log.Printf("    第 %d/%d 轮: OK (累计孤儿: %d)", i, *rounds, total)
		}
	}

	log.Printf("=== 完成: %d/%d 轮发现孤儿表 ===", total, *rounds)
	mustExec(pool, fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", *srcDB))

	if total > 0 {
		log.Printf("BUG 已复现！%d 轮出现孤儿表", total)
	} else {
		log.Println("未复现。尝试增大 MO_DELAY_CONSUME_MO_TABLES_MS 值（如 200）或增加表数量。")
	}
}
