// reproduce.go — 复现 CLONE + DROP DATABASE 竞态条件
//
// 原理：
//   1. 创建一个源数据库 src_db，里面放足够多的表（带索引），让 clone database 耗时较长
//   2. 并发执行：
//      - goroutine A: clone database src_db → dst_db_N
//      - goroutine B: 等 dst_db_N 出现后立即 drop database dst_db_N
//   3. 循环多轮，检查是否出现孤儿表（database 已删除但 mo_tables 中还有记录）
//
// 检测方法：
//   drop database 完成后，用新连接查询 mo_catalog.mo_tables 看是否还有属于 dst_db 的记录
//
// 用法：
//   go run reproduce.go -host 127.0.0.1 -port 6001 -user root -pass 111 -rounds 200 -tables 15

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
	rounds    = flag.Int("rounds", 200, "number of test rounds")
	numTables = flag.Int("tables", 15, "number of tables in source db (more = wider race window)")
	srcDB     = flag.String("srcdb", "clone_race_src", "source database name")
)

func dsn() string {
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/?timeout=30s&readTimeout=60s&writeTimeout=60s&interpolateParams=true",
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
	log.Printf("Setting up source database %s with %d tables...", *srcDB, *numTables)
	mustExec(db, fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", *srcDB))
	mustExecFatal(db, fmt.Sprintf("CREATE DATABASE `%s`", *srcDB))

	for i := 0; i < *numTables; i++ {
		tblName := fmt.Sprintf("tbl_%03d", i)
		// 带多个索引的表 → 每个 clone table 子事务创建更多 relation（主表+索引表）
		// clone database 总耗时更长，竞态窗口更大
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

		// 插入数据让 clone 有实际数据要拷贝
		var values []string
		for j := 0; j < 20; j++ {
			values = append(values, fmt.Sprintf(
				"('key_%d_%d', 'type_%d', 'user_%d', 'cat_%d', 'content_%d_%s')",
				i, j, j%5, j%10, j%3, j, strings.Repeat("x", 100)))
		}
		insertSQL := fmt.Sprintf("INSERT INTO %s.%s (key_name, scope_type, scope_user_id, category, content) VALUES %s",
			*srcDB, tblName, strings.Join(values, ","))
		mustExecFatal(db, insertSQL)
	}
	log.Printf("Source database ready: %d tables with indexes and data", *numTables)
}

// checkOrphans 用一个全新的连接检查孤儿表。
// 新连接会拿到最新的快照，避免旧连接快照缓存的干扰。
func checkOrphans(dstDBName string) (dbExists bool, orphanTables []string, err error) {
	checkConn, err := sql.Open("mysql", dsn())
	if err != nil {
		return false, nil, fmt.Errorf("open check conn: %w", err)
	}
	defer checkConn.Close()
	checkConn.SetMaxOpenConns(1)

	// 1. 检查 database 是否还存在
	var dbCount int
	err = checkConn.QueryRow(
		"SELECT COUNT(*) FROM mo_catalog.mo_database WHERE datname = ? AND account_id = 0",
		dstDBName).Scan(&dbCount)
	if err != nil {
		return false, nil, fmt.Errorf("query mo_database: %w", err)
	}
	dbExists = dbCount > 0

	// 2. 检查 mo_tables 中是否有属于该 database 的记录
	rows, err := checkConn.Query(
		`SELECT reldatabase, relname, rel_id FROM mo_catalog.mo_tables 
		 WHERE reldatabase = ? AND account_id = 0`,
		dstDBName)
	if err != nil {
		return dbExists, nil, fmt.Errorf("query mo_tables: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var dbName, tblName string
		var relID uint64
		if err := rows.Scan(&dbName, &tblName, &relID); err != nil {
			return dbExists, nil, err
		}
		orphanTables = append(orphanTables, fmt.Sprintf("%s.%s(id=%d)", dbName, tblName, relID))
	}
	return dbExists, orphanTables, rows.Err()
}

func runOneRound(roundID int, connPool *sql.DB) (found bool, errMsg string) {
	dstDB := fmt.Sprintf("clone_race_dst_%d_%d", roundID, rand.Intn(100000))

	// 确保目标库不存在
	mustExec(connPool, fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", dstDB))
	time.Sleep(10 * time.Millisecond)

	var wg sync.WaitGroup
	var cloneErr, dropErr error

	// goroutine A: clone database
	wg.Add(1)
	go func() {
		defer wg.Done()
		cloneConn, err := sql.Open("mysql", dsn())
		if err != nil {
			cloneErr = fmt.Errorf("open clone conn: %w", err)
			return
		}
		defer cloneConn.Close()
		cloneConn.SetMaxOpenConns(1)

		cloneSQL := fmt.Sprintf("CREATE DATABASE `%s` CLONE `%s`", dstDB, *srcDB)
		_, cloneErr = cloneConn.Exec(cloneSQL)
	}()

	// goroutine B: 等目标库出现后立即 drop
	wg.Add(1)
	go func() {
		defer wg.Done()
		dropConn, err := sql.Open("mysql", dsn())
		if err != nil {
			dropErr = fmt.Errorf("open drop conn: %w", err)
			return
		}
		defer dropConn.Close()
		dropConn.SetMaxOpenConns(1)

		// 轮询等待目标数据库出现（最多等 2 秒）
		for i := 0; i < 1000; i++ {
			var count int
			err := dropConn.QueryRow(
				"SELECT COUNT(*) FROM mo_catalog.mo_database WHERE datname = ? AND account_id = 0",
				dstDB).Scan(&count)
			if err == nil && count > 0 {
				// 数据库已创建，随机延迟来命中不同的竞态窗口
				// 有时立即 drop，有时等几毫秒让部分 clone 子事务完成
				delay := rand.Intn(10)
				if delay > 0 {
					time.Sleep(time.Duration(delay) * time.Millisecond)
				}
				break
			}
			time.Sleep(2 * time.Millisecond)
		}

		dropSQL := fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", dstDB)
		_, dropErr = dropConn.Exec(dropSQL)
	}()

	wg.Wait()

	// 等 logtail 追上
	time.Sleep(100 * time.Millisecond)

	// 用全新连接检查
	dbExists, orphans, err := checkOrphans(dstDB)
	if err != nil {
		// 清理后返回
		mustExec(connPool, fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", dstDB))
		return false, fmt.Sprintf("check error: %v", err)
	}

	if dbExists {
		// database 还在 → drop 没成功或 clone 在 drop 之后重建了，清理
		mustExec(connPool, fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", dstDB))
		return false, ""
	}

	// database 不在了，但 mo_tables 里还有记录 → 孤儿表！
	if len(orphans) > 0 {
		return true, fmt.Sprintf("ORPHAN TABLES in %s (db gone, %d tables remain): %v",
			dstDB, len(orphans), orphans)
	}

	// 都干净，清理可能的残留
	if cloneErr != nil || dropErr != nil {
		mustExec(connPool, fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", dstDB))
	}
	return false, ""
}

func main() {
	flag.Parse()
	rand.Seed(time.Now().UnixNano())

	log.Printf("=== CLONE + DROP DATABASE Race Condition Reproducer ===")
	log.Printf("Target: %s:%d, rounds=%d, tables=%d", *host, *port, *rounds, *numTables)

	connPool, err := sql.Open("mysql", dsn())
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer connPool.Close()
	connPool.SetMaxOpenConns(5)

	if err := connPool.Ping(); err != nil {
		log.Fatalf("Cannot ping MO: %v", err)
	}

	setupSourceDB(connPool)

	totalOrphans := 0
	for i := 1; i <= *rounds; i++ {
		found, msg := runOneRound(i, connPool)
		if found {
			totalOrphans++
			log.Printf("!!! ROUND %d/%d: %s", i, *rounds, msg)
		} else {
			if i%20 == 0 || msg != "" {
				extra := ""
				if msg != "" {
					extra = " — " + msg
				}
				log.Printf("    ROUND %d/%d: OK (orphans so far: %d)%s", i, *rounds, totalOrphans, extra)
			}
		}
	}

	log.Printf("=== DONE: %d/%d rounds found orphan tables ===", totalOrphans, *rounds)

	// 清理源库
	mustExec(connPool, fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", *srcDB))

	if totalOrphans > 0 {
		log.Printf("BUG REPRODUCED: %d rounds had orphan tables after DROP DATABASE", totalOrphans)
	} else {
		log.Printf("No orphan tables found in %d rounds. Try: -rounds 500 -tables 20", *rounds)
	}
}
