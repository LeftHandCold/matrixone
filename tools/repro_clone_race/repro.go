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

// Reproduce the race between CLONE CREATE TABLE and DROP DATABASE.
//
// The bug window: CLONE reads snapshot (db exists) -> DROP DATABASE commits
// -> CLONE's CREATE TABLE commits (orphan row in mo_tables).
//
// Strategy:
// 1. Create a source table with enough data to make CLONE take some time
// 2. Concurrently: connA does CLONE, connB does DROP DATABASE
// 3. If both succeed, we hit the race — orphan table record exists
// 4. Restart MO to trigger the EOB panic

func main() {
	dsn := flag.String("dsn", "root:111@tcp(127.0.0.1:6001)/", "MySQL DSN")
	sourceDB := flag.String("source-db", "clone_src", "source database")
	iterations := flag.Int("n", 1000, "number of iterations")
	rows := flag.Int("rows", 10000, "rows in source table (more = wider race window)")
	flag.Parse()

	db, err := sql.Open("mysql", *dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(30)

	// Setup source db with a table that has enough rows to slow down CLONE
	log.Printf("Setting up source db %s with %d rows...", *sourceDB, *rows)
	mustExec(db, fmt.Sprintf("DROP DATABASE IF EXISTS %s", *sourceDB))
	mustExec(db, fmt.Sprintf("CREATE DATABASE %s", *sourceDB))
	mustExec(db, fmt.Sprintf(
		"CREATE TABLE %s.t1 (id INT PRIMARY KEY, val VARCHAR(255), padding VARCHAR(1000))",
		*sourceDB))

	// Batch insert
	batchSize := 1000
	for start := 0; start < *rows; start += batchSize {
		end := start + batchSize
		if end > *rows {
			end = *rows
		}
		q := fmt.Sprintf("INSERT INTO %s.t1 SELECT seq, CONCAT('val_', seq), REPEAT('x', 500) FROM generate_series(%d, %d) g(seq)", *sourceDB, start+1, end)
		if _, err := db.Exec(q); err != nil {
			// fallback: some MO versions may not have generate_series
			for i := start + 1; i <= end; i++ {
				mustExec(db, fmt.Sprintf(
					"INSERT INTO %s.t1 VALUES (%d, 'val_%d', '%s')",
					*sourceDB, i, i, "xxxxxxxxxxxxxxxxxxxx"))
			}
			break
		}
	}
	log.Printf("Source table ready.")

	var raceHits atomic.Int64
	var totalCloneOK, totalDropOK atomic.Int64

	for i := 0; i < *iterations; i++ {
		targetDB := fmt.Sprintf("clone_tgt_%d", i%50) // reuse names to avoid too many dbs

		// Create fresh target db
		db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", targetDB))
		mustExec(db, fmt.Sprintf("CREATE DATABASE %s", targetDB))

		var wg sync.WaitGroup
		var cloneErr, dropErr error

		wg.Add(2)

		// Goroutine A: CLONE (this takes time proportional to data size)
		go func() {
			defer wg.Done()
			connA, err := sql.Open("mysql", *dsn)
			if err != nil {
				cloneErr = err
				return
			}
			defer connA.Close()
			_, cloneErr = connA.Exec(fmt.Sprintf(
				"CREATE TABLE %s.t1 CLONE %s.t1", targetDB, *sourceDB))
		}()

		// Goroutine B: DROP DATABASE with varying delays to hit the window
		go func() {
			defer wg.Done()
			connB, err := sql.Open("mysql", *dsn)
			if err != nil {
				dropErr = err
				return
			}
			defer connB.Close()
			// Vary the delay to probe different points in CLONE's execution
			delay := time.Duration(i%10) * time.Millisecond
			time.Sleep(delay)
			_, dropErr = connB.Exec(fmt.Sprintf("DROP DATABASE %s", targetDB))
		}()

		wg.Wait()

		if cloneErr == nil {
			totalCloneOK.Add(1)
		}
		if dropErr == nil {
			totalDropOK.Add(1)
		}

		if cloneErr == nil && dropErr == nil {
			// BOTH succeeded — the race condition!
			// CLONE wrote a table record, then DROP deleted the db
			// but didn't know about the new table.
			raceHits.Add(1)
			log.Printf("[RACE HIT #%d] iter=%d — both CLONE and DROP succeeded!",
				raceHits.Load(), i)
		}

		if i%100 == 0 {
			log.Printf("  progress: iter=%d raceHits=%d cloneOK=%d dropOK=%d",
				i, raceHits.Load(), totalCloneOK.Load(), totalDropOK.Load())
		}

		// Cleanup
		db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", targetDB))
	}

	log.Printf("=== DONE ===")
	log.Printf("Iterations: %d", *iterations)
	log.Printf("Race hits (both CLONE+DROP OK): %d", raceHits.Load())
	log.Printf("Clone OK: %d, Drop OK: %d", totalCloneOK.Load(), totalDropOK.Load())

	if raceHits.Load() > 0 {
		log.Printf("!!! Race condition triggered %d times!", raceHits.Load())
		log.Printf("!!! Restart MO and check for OkExpectedEOB panic in replay.")
	} else {
		log.Printf("Race not triggered. Try increasing -n or -rows.")
	}

	mustExec(db, fmt.Sprintf("DROP DATABASE IF EXISTS %s", *sourceDB))
}

func mustExec(db *sql.DB, query string) {
	if _, err := db.Exec(query); err != nil {
		log.Fatalf("exec %q: %v", query, err)
	}
}
