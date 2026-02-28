// reproduce.go — 复现 CREATE TABLE + DROP DATABASE 竞态条件
//
// 场景：应用程序创建数据库后逐个创建表（如 data branch），同时另一个连接 DROP 该数据库。
// 由于 lock service bug（Exclusive lock 获取后锁模式仍为 Shared），CREATE TABLE 的
// lockMoDatabase(Shared) 可以绕过 DROP 的 Exclusive lock。
//
// DROP 的 Relations() 用旧的 snapshotTS 查询，漏掉并发创建的表，导致孤儿记录。
//
// 使用步骤：
//   1. 编译 MO（包含 ddl.go 中的 MO_DELAY_AFTER_LOCK_MO_DATABASE_MS hack，v1 fix 已移除）
//   2. 启动 MO 时设置环境变量：
//      export MO_DELAY_AFTER_LOCK_MO_DATABASE_MS=500
//   3. 运行本工具：go run reproduce.go -host 127.0.0.1 -port 6001

package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

var (
	host   = flag.String("host", "127.0.0.1", "MO host")
	port   = flag.Int("port", 6001, "MO port")
	user   = flag.String("user", "root", "MO user")
	pass   = flag.String("pass", "111", "MO password")
	rounds = flag.Int("rounds", 200, "number of test rounds")
	tables = flag.Int("tables", 20, "number of tables to create concurrently")
	delay = flag.Int("delay", 500, "suggested MO_DELAY_AFTER_LOCK_MO_DATABASE_MS value (ms)")
)

func dsn() string {
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/?timeout=30s&readTimeout=60s&writeTimeout=60s&interpolateParams=true",
		*user, *pass, *host, *port)
}

func mustExec(db *sql.DB, q string) {
	if _, err := db.Exec(q); err != nil {
		log.Printf("WARN: %s — %v", q, err)
	}
}

// checkOrphans 检查 database 是否已删除但 mo_tables 中仍有残留记录
func checkOrphans(conn *sql.DB, dbName string) (dbExists bool, orphans []string, err error) {
	var cnt int
	if err = conn.QueryRow(
		"SELECT COUNT(*) FROM mo_catalog.mo_database WHERE datname=? AND account_id=0",
		dbName).Scan(&cnt); err != nil {
		return
	}
	dbExists = cnt > 0

	rows, err := conn.Query(
		"SELECT relname, rel_id FROM mo_catalog.mo_tables WHERE reldatabase=? AND account_id=0",
		dbName)
	if err != nil {
		return
	}
	defer rows.Close()
	for rows.Next() {
		var name string
		var id uint64
		if err = rows.Scan(&name, &id); err != nil {
			return
		}
		orphans = append(orphans, fmt.Sprintf("%s(id=%d)", name, id))
	}
	err = rows.Err()
	return
}

func runOneRound(roundID int, pool *sql.DB) (found bool, msg string) {
	dbName := fmt.Sprintf("race_test_%d", roundID)

	// 清理
	mustExec(pool, fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", dbName))
	time.Sleep(10 * time.Millisecond)

	// 先创建数据库
	if _, err := pool.Exec(fmt.Sprintf("CREATE DATABASE `%s`", dbName)); err != nil {
		return false, fmt.Sprintf("create db err: %v", err)
	}

	var wg sync.WaitGroup
	var createDone atomic.Int32
	var dropErr error

	// 并发创建多张表（模拟 data branch 的 create table 操作）
	for i := 0; i < *tables; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			c, err := sql.Open("mysql", dsn())
			if err != nil {
				return
			}
			defer c.Close()
			c.SetMaxOpenConns(1)

			tbl := fmt.Sprintf("tbl_%03d", idx)
			ddl := fmt.Sprintf(
				"CREATE TABLE `%s`.`%s` (id BIGINT PRIMARY KEY, name VARCHAR(255), "+
					"val TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, "+
					"KEY idx_%s_name (name), KEY idx_%s_created (created_at))",
				dbName, tbl, tbl, tbl)
			_, err = c.Exec(ddl)
			if err != nil {
				// 可能因为 db 已被 drop 而失败，正常
				return
			}
			createDone.Add(1)
		}(i)
	}

	// DROP：等几张表创建成功后立即 drop
	wg.Add(1)
	go func() {
		defer wg.Done()
		// 等至少 2 张表创建成功，确保有 Shared lock 活动
		for i := 0; i < 2000; i++ {
			if createDone.Load() >= 2 {
				break
			}
			time.Sleep(1 * time.Millisecond)
		}
		c, err := sql.Open("mysql", dsn())
		if err != nil {
			dropErr = err
			return
		}
		defer c.Close()
		c.SetMaxOpenConns(1)
		_, dropErr = c.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", dbName))
	}()

	wg.Wait()

	if dropErr != nil {
		// drop 失败，清理
		mustExec(pool, fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", dbName))
		return false, ""
	}

	// 等 logtail 追上
	time.Sleep(500 * time.Millisecond)

	dbExists, orphans, err := checkOrphans(pool, dbName)
	if err != nil {
		mustExec(pool, fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", dbName))
		return false, fmt.Sprintf("check err: %v", err)
	}

	if dbExists {
		// db 还在说明 drop 没成功或还没生效
		mustExec(pool, fmt.Sprintf("DROP DATABASE IF EXISTS `%s`", dbName))
		return false, ""
	}

	if len(orphans) > 0 {
		return true, fmt.Sprintf("孤儿表! db=%s 已删除但 mo_tables 残留 %d 条: %v",
			dbName, len(orphans), orphans)
	}
	return false, ""
}

func main() {
	flag.Parse()

	log.Println("=== CREATE TABLE + DROP DATABASE 竞态条件复现工具 ===")
	log.Printf("目标: %s:%d, 轮次=%d, 每轮表数=%d", *host, *port, *rounds, *tables)
	log.Println("")
	log.Println("请确保:")
	log.Printf("  1. MO 编译时包含 ddl.go 中的 delay hack（v1 fix 已移除）")
	log.Printf("  2. 启动 MO 时设置: export MO_DELAY_AFTER_LOCK_MO_DATABASE_MS=%d", *delay)
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

	log.Printf("\n=== 完成: %d/%d 轮发现孤儿表 ===", total, *rounds)
	if total > 0 {
		log.Printf("BUG 已复现! %d 轮出现孤儿表", total)
	} else {
		log.Println("未复现。尝试增大 delay 或 tables 数量。")
	}
}
