// 清理牛客 UUID（Tracker 脏身份）提交与题库，**绝不删除数字 external_id 题面**。
//
// 背景：旧训练源优先 questionUuid，与 AC 站数字 id 双计；新逻辑优先 problem.id。
// 入库 submit_id OnConflict DoNothing，须先删 UUID 脏行再全量重爬。
//
// 删除范围（仅 UUID）：
//  1) submit_logs：external_id 为 32hex，或 problem 首 token 为 32hex
//  2) problems：platform=NowCoder 且 external_id 为 32hex（数字题号保留）
//  3) 上述题的 tags / 题单条目 / user_problem_status
//  4) user_ac_problems / user_ac_problem_days 中 e:/n: UUID 键及 p:{uuid题id}
//
// 不删：数字 id 提交、数字 id 题面、contest_logs、platforms 绑定。
//
// 用法：
//
//	go run .                 # dry-run
//	go run . -execute        # 真正删除
//
// DSN：环境变量 CLEANUP_DSN 或 -dsn（勿把密码写进仓库）。
package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

// 32 位 hex UUID（无连字符；库内已 normalize 去连字符）
const uuidRE = `^[0-9a-f]{32}$`

func main() {
	execute := flag.Bool("execute", false, "真正执行 DELETE（默认 dry-run）")
	defaultDSN := os.Getenv("CLEANUP_DSN")
	if defaultDSN == "" {
		defaultDSN = "host=127.0.0.1 port=5432 user=cwxu password= dbname=algo_core_data sslmode=disable connect_timeout=15"
	}
	dsn := flag.String("dsn", defaultDSN, "PostgreSQL DSN（也可用环境变量 CLEANUP_DSN）")
	flag.Parse()

	// pgx stdlib 需要 postgres:// 或 key=value；key=value 可直接用
	db, err := sql.Open("pgx", *dsn)
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
	fmt.Println("SAFETY: only delete NowCoder rows whose identity is 32-hex UUID; digit external_id problems are NEVER deleted")

	printStats := func(label string) {
		fmt.Println("\n---", label, "---")
		mustPrint(db, "submit_logs NowCoder", `
			SELECT
			  COUNT(*)::text AS total,
			  COUNT(*) FILTER (WHERE external_id ~ '^[0-9]+$')::text AS digit_eid,
			  COUNT(*) FILTER (WHERE lower(coalesce(external_id,'')) ~ '`+uuidRE+`')::text AS uuid_eid,
			  COUNT(*) FILTER (
			    WHERE lower(split_part(btrim(problem), ' ', 1)) ~ '`+uuidRE+`'
			  )::text AS problem_token_uuid,
			  COUNT(*) FILTER (WHERE external_id IS NULL OR external_id = '')::text AS empty_eid
			FROM submit_logs WHERE platform = 'NowCoder'`)
		mustPrint(db, "problems NowCoder", `
			SELECT
			  COUNT(*)::text AS total,
			  COUNT(*) FILTER (WHERE external_id ~ '^[0-9]+$')::text AS digit,
			  COUNT(*) FILTER (WHERE lower(external_id) ~ '`+uuidRE+`')::text AS uuid,
			  COUNT(*) FILTER (
			    WHERE external_id !~ '^[0-9]+$'
			      AND lower(external_id) !~ '`+uuidRE+`'
			  )::text AS other
			FROM problems WHERE platform = 'NowCoder'`)
		mustPrint(db, "user_ac uuid-ish keys", `
			SELECT COUNT(*)::text AS cnt
			FROM user_ac_problems
			WHERE platform = 'NowCoder'
			  AND (
			    problem_key ~ '^e:NowCoder:[0-9a-f]{32}$'
			    OR problem_key ~ '^n:NowCoder:[0-9a-f]{32}$'
			  )`)
	}

	printStats("BEFORE")

	// 将要删除的题面样本（仅 UUID）
	mustPrint(db, "sample UUID problems (will delete title/face)", `
		SELECT id::text, left(title,40) AS title, external_id
		FROM problems
		WHERE platform = 'NowCoder' AND lower(external_id) ~ '`+uuidRE+`'
		ORDER BY id DESC
		LIMIT 10`)
	// 数字题面样本（必须保留）
	mustPrint(db, "sample DIGIT problems (KEEP)", `
		SELECT id::text, left(title,40) AS title, external_id
		FROM problems
		WHERE platform = 'NowCoder' AND external_id ~ '^[0-9]+$'
		ORDER BY id DESC
		LIMIT 5`)

	if !*execute {
		fmt.Println("\n[dry-run] 未删除。加 -execute 仅删 UUID 身份：")
		fmt.Println("  - submit_logs: uuid external_id 或 problem 首 token 为 uuid")
		fmt.Println("  - problems: external_id 为 32hex（数字题号题面保留）")
		fmt.Println("  - 关联 tags / problemset_items / user_problem_status / user_ac* uuid 键")
		fmt.Println("删完后：POST /api/core/spider/update-platform {\"platform\":\"NowCoder\"} needAll 全量（含冻结绑定用户）")
		os.Exit(0)
	}

	tx, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()

	// 受影响用户（删提交后重建日统计 / AC 预聚合可选：此处只清 uuid 键，日统计用 SQL 修正）
	var affectedUsers []int64
	rows, err := tx.Query(`
		SELECT DISTINCT user_id FROM submit_logs
		WHERE platform = 'NowCoder'
		  AND (
		    lower(coalesce(external_id,'')) ~ '` + uuidRE + `'
		    OR lower(split_part(btrim(problem), ' ', 1)) ~ '` + uuidRE + `'
		  )`)
	if err != nil {
		log.Fatal("list affected users:", err)
	}
	for rows.Next() {
		var uid int64
		if err := rows.Scan(&uid); err != nil {
			rows.Close()
			log.Fatal(err)
		}
		affectedUsers = append(affectedUsers, uid)
	}
	rows.Close()
	fmt.Printf("affected users (uuid submits): %d\n", len(affectedUsers))

	// UUID 题 id 列表
	var uuidProblemIDs []int64
	prows, err := tx.Query(`
		SELECT id FROM problems
		WHERE platform = 'NowCoder' AND lower(external_id) ~ '` + uuidRE + `'`)
	if err != nil {
		log.Fatal("list uuid problems:", err)
	}
	for prows.Next() {
		var id int64
		if err := prows.Scan(&id); err != nil {
			prows.Close()
			log.Fatal(err)
		}
		uuidProblemIDs = append(uuidProblemIDs, id)
	}
	prows.Close()
	fmt.Printf("uuid problems to delete: %d\n", len(uuidProblemIDs))

	// 1) 提交：仅 UUID 身份
	res, err := tx.Exec(`
		DELETE FROM submit_logs
		WHERE platform = 'NowCoder'
		  AND (
		    lower(coalesce(external_id,'')) ~ '` + uuidRE + `'
		    OR lower(split_part(btrim(problem), ' ', 1)) ~ '` + uuidRE + `'
		  )`)
	if err != nil {
		log.Fatal("delete submit_logs:", err)
	}
	nSub, _ := res.RowsAffected()
	fmt.Printf("deleted submit_logs (uuid only): %d\n", nSub)

	// 2) 关联：仅 uuid 题 id
	if len(uuidProblemIDs) > 0 {
		idList := int64List(uuidProblemIDs)
		for _, q := range []struct {
			name string
			sql  string
		}{
			{"problem_tags", `DELETE FROM problem_tags WHERE problem_id IN (` + idList + `)`},
			{"problemset_items", `DELETE FROM problemset_items WHERE problem_id IN (` + idList + `)`},
			{"user_problem_status", `DELETE FROM user_problem_status WHERE problem_id IN (` + idList + `)`},
			// contest_problems 若挂 problem_id
			{"contest_problems(problem_id)", `
				DELETE FROM contest_problems
				WHERE problem_id IN (` + idList + `)
				   OR (platform = 'NowCoder' AND lower(external_id) ~ '` + uuidRE + `')`},
		} {
			r, e := tx.Exec(q.sql)
			if e != nil {
				// 表可能不存在 / 列不同：警告继续
				fmt.Printf("warn %s: %v\n", q.name, e)
				continue
			}
			n, _ := r.RowsAffected()
			fmt.Printf("deleted %s: %d\n", q.name, n)
		}
	}

	// 3) 题面：仅 UUID external_id（硬条件，防止误杀数字题）
	res, err = tx.Exec(`
		DELETE FROM problems
		WHERE platform = 'NowCoder'
		  AND lower(external_id) ~ '` + uuidRE + `'
		  AND external_id !~ '^[0-9]+$'`)
	if err != nil {
		log.Fatal("delete problems:", err)
	}
	nProb, _ := res.RowsAffected()
	fmt.Printf("deleted problems (uuid only): %d\n", nProb)

	// 4) 预聚合：uuid e:/n: 键 + 已删 p: 键
	res, err = tx.Exec(`
		DELETE FROM user_ac_problems
		WHERE platform = 'NowCoder'
		  AND (
		    problem_key ~ '^e:NowCoder:[0-9a-f]{32}$'
		    OR problem_key ~ '^n:NowCoder:[0-9a-f]{32}$'
		  )`)
	if err != nil {
		log.Fatal("delete user_ac uuid keys:", err)
	}
	nAC, _ := res.RowsAffected()
	fmt.Printf("deleted user_ac_problems uuid keys: %d\n", nAC)

	if len(uuidProblemIDs) > 0 {
		idList := int64List(uuidProblemIDs)
		// p:{id} 键
		keys := make([]string, 0, len(uuidProblemIDs))
		for _, id := range uuidProblemIDs {
			keys = append(keys, fmt.Sprintf("'p:%d'", id))
		}
		keyList := strings.Join(keys, ",")
		r, e := tx.Exec(`DELETE FROM user_ac_problems WHERE problem_key IN (` + keyList + `)`)
		if e != nil {
			fmt.Printf("warn user_ac p: keys: %v\n", e)
		} else {
			n, _ := r.RowsAffected()
			fmt.Printf("deleted user_ac_problems p:uuid-problem keys: %d\n", n)
		}
		r, e = tx.Exec(`DELETE FROM user_ac_problem_days WHERE problem_key IN (` + keyList + `)
			OR problem_key ~ '^e:NowCoder:[0-9a-f]{32}$'
			OR problem_key ~ '^n:NowCoder:[0-9a-f]{32}$'`)
		if e != nil {
			fmt.Printf("warn user_ac_problem_days: %v\n", e)
		} else {
			n, _ := r.RowsAffected()
			fmt.Printf("deleted user_ac_problem_days uuid-related: %d\n", n)
		}
		_ = idList
	} else {
		r, e := tx.Exec(`
			DELETE FROM user_ac_problem_days
			WHERE problem_key ~ '^e:NowCoder:[0-9a-f]{32}$'
			   OR problem_key ~ '^n:NowCoder:[0-9a-f]{32}$'`)
		if e != nil {
			fmt.Printf("warn user_ac_problem_days: %v\n", e)
		} else {
			n, _ := r.RowsAffected()
			fmt.Printf("deleted user_ac_problem_days uuid keys: %d\n", n)
		}
	}

	// 5) 对受影响用户重建 NowCoder 日统计（仅该平台行）：删后按剩余 submit_logs 重算
	//    避免 UUID 双计把 daily 撑大；数字提交保留后重算即可
	for _, uid := range affectedUsers {
		if _, e := tx.Exec(`DELETE FROM daily_user_stats WHERE user_id = $1 AND platform = 'NowCoder'`, uid); e != nil {
			fmt.Printf("warn daily delete user=%d: %v\n", uid, e)
			continue
		}
		if _, e := tx.Exec(`
			INSERT INTO daily_user_stats (user_id, day, platform, submit_cnt, ac_cnt)
			SELECT
				user_id,
				date_trunc('day', time)::date AS day,
				'NowCoder' AS platform,
				COUNT(*)::int AS submit_cnt,
				COUNT(*) FILTER (WHERE is_ac = true)::int AS ac_cnt
			FROM submit_logs
			WHERE user_id = $1 AND platform = 'NowCoder'
			GROUP BY user_id, date_trunc('day', time)::date
			HAVING COUNT(*) > 0
		`, uid); e != nil {
			fmt.Printf("warn daily rebuild user=%d: %v\n", uid, e)
		}
	}
	fmt.Printf("rebuilt daily_user_stats NowCoder for %d users\n", len(affectedUsers))

	// 安全校验：数字题面数量不得变少到 0（若 before 有数字题）
	var digitLeft int
	if err := tx.QueryRow(`
		SELECT COUNT(*) FROM problems
		WHERE platform = 'NowCoder' AND external_id ~ '^[0-9]+$'`).Scan(&digitLeft); err != nil {
		log.Fatal("verify digit problems:", err)
	}
	fmt.Printf("VERIFY digit problems remaining: %d\n", digitLeft)

	var uuidLeft int
	if err := tx.QueryRow(`
		SELECT COUNT(*) FROM problems
		WHERE platform = 'NowCoder' AND lower(external_id) ~ '` + uuidRE + `'`).Scan(&uuidLeft); err != nil {
		log.Fatal("verify uuid problems:", err)
	}
	if uuidLeft != 0 {
		log.Fatalf("VERIFY FAIL: still %d uuid problems", uuidLeft)
	}
	fmt.Println("VERIFY uuid problems remaining: 0")

	if err := tx.Commit(); err != nil {
		log.Fatal("commit:", err)
	}
	fmt.Println("commit ok")

	printStats("AFTER")
	mustPrint(db, "待全量重爬用户（含冻结；凡绑定 NowCoder）", `
		SELECT COUNT(*)::text AS users FROM platforms WHERE platform = 'NowCoder'`)
	fmt.Println("\n下一步：POST /api/core/spider/update-platform {\"platform\":\"NowCoder\"}")
	fmt.Println("DoBatchPlatform 按 platforms 表入队，不排除冻结/休眠用户。")
}

func int64List(ids []int64) string {
	parts := make([]string, len(ids))
	for i, id := range ids {
		parts[i] = fmt.Sprintf("%d", id)
	}
	return strings.Join(parts, ",")
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
