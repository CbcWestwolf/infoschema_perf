package util

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"go.uber.org/atomic"
)

func openDB(user, host string, port int, db_name string) (*sql.DB, error) {
	return sql.Open("mysql", fmt.Sprintf("%s@tcp(%s:%d)/%s", user, host, port, db_name))
}

func openMultiConns(connNum int, host string, port int, user string, db_name string) (db *sql.DB, conns []*sql.Conn, err error) {
	if connNum <= 0 {
		panic("n should be greater than 0")
	}
	db, err = openDB(user, host, port, db_name)
	if err != nil {
		return nil, nil, err
	}
	db.SetMaxOpenConns(connNum)
	for i := 0; i < connNum; i++ {
		conn, err := db.Conn(context.Background())
		if err != nil {
			for _, c := range conns {
				c.Close()
			}
			return nil, nil, err
		}
		conns = append(conns, conn)
	}

	return
}

func GetMultiConnsForExec() (chs []chan string, waitAndClose func()) {
	var wg sync.WaitGroup
	db, conns, err := openMultiConns(Thread, Host, Port, User, "")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	chs = make([]chan string, Thread)
	for i := 0; i < Thread; i++ {
		chs[i] = make(chan string)
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for sql := range chs[i] {
				if Stdout {
					fmt.Println("Exec:", sql)
				}
				_, err := conns[i].ExecContext(context.Background(), sql)
				if err != nil {
					fmt.Fprintln(os.Stderr, err, sql)
				}
			}
		}(i)
	}

	waitAndClose = func() {
		for i := 0; i < Thread; i++ {
			close(chs[i])
		}
		wg.Wait()

		for _, conn := range conns {
			conn.Close()
		}
		db.Close()
	}

	return
}

func getMultiConnsForQuery(wg *sync.WaitGroup, ctx *context.Context, c *atomic.Uint64, input chan string) (waitAndClose func()) {
	db, conns, err := openMultiConns(Thread, Host, Port, User, "")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	if ctx == nil {
		panic("ctx should not be nil")
	}

	for i := 0; i < Thread; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for {
				select {
				case <-(*ctx).Done():
					return
				case sql, ok := <-input:
					if !ok {
						return
					}

					if Stdout {
						fmt.Println("Query:", sql)
					}

					rows, err := conns[i].QueryContext(context.Background(), sql)
					if err != nil {
						fmt.Fprintln(os.Stderr, err, sql)
						continue
					}
					cols, err := rows.Columns()
					if err != nil {
						fmt.Fprintln(os.Stderr, err, sql)
						continue
					}
					dest := make([]*interface{}, len(cols))
					c.Inc()
					resNum := 0
					for rows.Next() {
						rows.Scan(dest)
						resNum++
					}
					if Stdout {
						fmt.Printf("#rows: %d\n", resNum)
					}
					if err := rows.Close(); err != nil {
						fmt.Fprintln(os.Stderr, err, sql)
					}
				}
			}
		}(i)
	}
	waitAndClose = func() {
		for _, conn := range conns {
			conn.Close()
		}
		db.Close()
	}

	return
}

func QuerySQL(getSQL func() string) {
	var (
		wg      sync.WaitGroup
		counter = *atomic.NewUint64(0)
		ctx     = context.Background()
	)
	timeout := 60 * time.Second
	if TimeStr != "" {
		t, err := time.ParseDuration(TimeStr)
		if err == nil {
			timeout = t
		}
	}
	var cancel context.CancelFunc
	fmt.Printf("Execute for %s\n", timeout.String())
	ctx, cancel = context.WithTimeout(ctx, timeout)
	defer cancel()

	input := make(chan string)
	clean := getMultiConnsForQuery(&wg, &ctx, &counter, input)
	defer clean()

	startTime := time.Now()
Loop:
	for {
		select {
		case <-ctx.Done():
			break Loop
		default:
			input <- getSQL()
		}
	}
	wg.Wait()
	close(input)

	fmt.Printf("Count %d, duration %s\n", counter.Load(), time.Since(startTime).String())
}
