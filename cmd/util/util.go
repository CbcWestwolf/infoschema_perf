package util

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
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

// Remember to close the returning db, conns and chs
func GetMultiConnsForExec(wg *sync.WaitGroup) (db *sql.DB, conns []*sql.Conn, chs []chan string, err error) {
	db, conns, err = openMultiConns(Thread, Host, Port, User, "")
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
				_, err := conns[i].ExecContext(context.Background(), sql)
				if err != nil {
					fmt.Fprintln(os.Stderr, err, sql)
				}
			}
		}(i)
	}

	return
}

func getMultiConnsForQuery(wg *sync.WaitGroup, ctx *context.Context, c *atomic.Uint64) (db *sql.DB, conns []*sql.Conn, chs []chan string, err error) {
	db, conns, err = openMultiConns(Thread, Host, Port, User, "")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	if ctx == nil {
		panic("ctx should not be nil")
	}

	chs = make([]chan string, Thread)

	for i := 0; i < Thread; i++ {
		chs[i] = make(chan string)
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for {
				select {
				case <-(*ctx).Done():
					return
				case sql, ok := <-chs[i]:
					if !ok {
						return
					}

					if rows, err := conns[i].QueryContext(context.Background(), sql); err != nil {
						fmt.Fprintln(os.Stderr, err, sql)
					} else if err := rows.Close(); err != nil {
						fmt.Fprintln(os.Stderr, err, sql)
					} else if c != nil {
						c.Inc()
					}
				}
			}
		}(i)
	}

	return
}

func QuerySQL(getSQL func() string) {
	var (
		wg      sync.WaitGroup
		counter = *atomic.NewUint64(0)
		ctx     = context.Background()
	)
	if TimeStr != "" {
		t, err := time.ParseDuration(TimeStr)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Fail to parse %s", TimeStr)
		} else {
			var cancel context.CancelFunc
			fmt.Printf("Execute for %s\n", t.String())
			ctx, cancel = context.WithTimeout(ctx, t)
			defer cancel()
		}
	} else {
		fmt.Println("Execute with no time limit")
	}

	db, conns, chs, err := getMultiConnsForQuery(&wg, &ctx, &counter)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer func() {
		for _, conn := range conns {
			conn.Close()
		}
		db.Close()
	}()

	startTime := time.Now()
	tick := time.NewTicker(Tick)
	defer tick.Stop()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				chs[rand.Intn(Thread)] <- getSQL()
			}
		}
	}()

	<-ctx.Done()
	for i := 0; i < Thread; i++ {
		close(chs[i])
	}

	wg.Wait()
	fmt.Printf("Count %d, duration %s\n", counter.Load(), time.Since(startTime).String())
}
