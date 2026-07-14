// 清理生产库中被污染的牛客（NowCoder）提交与题库数据。
//
// 背景：旧爬虫用 questionNum / 标题 / main|uid 当 external_id，导致全站串题。
// 修复后正确 id 为纯数字题库 id；但入库是 submit_id OnConflict DoNothing，
// 旧行不会被覆盖，必须先删再全量爬。
//
// 用法：
//
//	go run .                 # dry-run，只统计
//	go run . -execute        # 真正删除
//
// 通过环境变量 CLEANUP_DSN 或 -dsn 传入连接串，勿把密码写进仓库。
package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	_ "github.com/lib/pq"
)

func main() {
	execute := flag.Bool("execute", false, "真正执行 DELETE（默认 dry-run）")
	defaultDSN := os.Getenv("CLEANUP_DSN")
	if defaultDSN == "" {
		defaultDSN = "host=127.0.0.1 port=5432 user=cwxu password= dbname=algo_core_data sslmode=disable connect_timeout=15"
	}
	dsn := flag.String("dsn", defaultDSN, "PostgreSQL DSN（也可用环境变量 CLEANUP_DSN）")
	flag.Parse()

	db, err := sql.Open("postgres", *dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(2)
	db.SetConnMaxLifetime(time.Minute)

	if err := db.Ping(); err != nil {
		log.Fatal("ping:", err)
	}
	fmt.Println("connected")

	printStats := func(label string) {
		fmt.Println("\n---", label, "---")
		mustPrint(db, "submit_logs NowCoder", `
			SELECT
			  COUNT(*)::text AS total,
			  COUNT(*) FILTER (WHERE external_id ~ '^[0-9]+$')::text AS good_digit_eid,
			  COUNT(*) FILTER (WHERE external_id IS NULL OR external_id = '')::text AS empty_eid,
			  COUNT(*) FILTER (WHERE external_id IS NOT NULL AND external_id <> '' AND external_id !~ '^[0-9]+$')::text AS bad_eid,
			  COUNT(*) FILTER (WHERE contest LIKE 'main|%')::text AS main_contest
			FROM submit_logs WHERE platform = 'NowCoder'`)
		mustPrint(db, "problems NowCoder", `
			SELECT
			  COUNT(*)::text AS total,
			  COUNT(*) FILTER (WHERE external_id ~ '^[0-9]+$')::text AS good,
			  COUNT(*) FILTER (WHERE external_id !~ '^[0-9]+$')::text AS bad
			FROM problems WHERE platform = 'NowCoder'`)
		mustPrint(db, "contest_logs NowCoder (保留)", `
			SELECT COUNT(*)::text AS total FROM contest_logs WHERE platform = 'NowCoder'`)
		mustPrint(db, "platforms NowCoder users", `
			SELECT COUNT(*)::text AS users FROM platforms WHERE platform = 'NowCoder'`)
	}

	printStats("BEFORE")

	// 污染概况（dry-run 也打印）
	mustPrint(db, "bad external_id top15", `
		SELECT LEFT(external_id, 80) AS external_id, COUNT(*)::text AS cnt
		FROM submit_logs
		WHERE platform = 'NowCoder'
		  AND external_id IS NOT NULL AND external_id <> '' AND external_id !~ '^[0-9]+$'
		GROUP BY LEFT(external_id, 80)
		ORDER BY COUNT(*) DESC
		LIMIT 15`)

	if !*execute {
		fmt.Println("\n[dry-run] 未删除任何数据。加 -execute 执行：")
		fmt.Println("  1) DELETE submit_logs WHERE platform = 'NowCoder'")
		fmt.Println("  2) DELETE problems   WHERE platform = 'NowCoder'")
		fmt.Println("  contest_logs / platforms 保留")
		fmt.Println("删完后调用 POST /v1/core/spider/update-all 或对每个绑定用户 needAll=true 全量爬取。")
		os.Exit(0)
	}

	tx, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()

	// 1) 提交记录：全部删（含少量已正确数字 id 的行），否则 OnConflict 挡重爬
	res, err := tx.Exec(`DELETE FROM submit_logs WHERE platform = 'NowCoder'`)
	if err != nil {
		log.Fatal("delete submit_logs:", err)
	}
	nSub, _ := res.RowsAffected()
	fmt.Printf("\ndeleted submit_logs: %d\n", nSub)

	// 2) 题库：全部删（坏 external_id 的孤儿 + 少量正确行，重爬后重建）
	res, err = tx.Exec(`DELETE FROM problems WHERE platform = 'NowCoder'`)
	if err != nil {
		log.Fatal("delete problems:", err)
	}
	nProb, _ := res.RowsAffected()
	fmt.Printf("deleted problems: %d\n", nProb)

	if err := tx.Commit(); err != nil {
		log.Fatal("commit:", err)
	}
	fmt.Println("commit ok")

	printStats("AFTER")

	mustPrint(db, "待全量重爬用户", `
		SELECT user_id::text, username
		FROM platforms
		WHERE platform = 'NowCoder'
		ORDER BY user_id`)

	fmt.Println("\n下一步：对以上用户触发 needAll=true 全量爬虫（update-all / 管理台）。")
}

func mustPrint(db *sql.DB, title, q string) {
	fmt.Println("\n===", title, "===")
	rows, err := db.Query(q)
	if err != nil {
		fmt.Println("ERR:", err)
		return
	}
	defer rows.Close()
	cols, _ := rows.Columns()
	vals := make([]any, len(cols))
	ptrs := make([]any, len(cols))
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	n := 0
	for rows.Next() {
		if err := rows.Scan(ptrs...); err != nil {
			fmt.Println("scan:", err)
			return
		}
		for i, c := range cols {
			v := vals[i]
			switch t := v.(type) {
			case []byte:
				v = string(t)
			case nil:
				v = "NULL"
			}
			if i > 0 {
				fmt.Print(" | ")
			}
			fmt.Printf("%s=%v", c, v)
		}
		fmt.Println()
		n++
	}
	if n == 0 {
		fmt.Println("(no rows)")
	}
}
