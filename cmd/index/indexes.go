package index

import (
	"fmt"
	"infoschema_perf/cmd/util"
	"math/rand"
	"os"
	"strings"

	"github.com/spf13/cobra"
)

var (
	IndexCmd = &cobra.Command{
		Use:   "index",
		Short: "Prepare, test and clean table for test indexes",
	}

	prepareCmd = &cobra.Command{
		Use:   "prepare",
		Short: fmt.Sprintf("Prepare tables for test (%s)", prepareTableSQL),
		Run:   prepare,
	}

	cleanCmd = &cobra.Command{
		Use:   "clean",
		Short: fmt.Sprintf("Clean tables after test (%s)", util.CleanSQL),
		Run:   util.Clean,
	}

	queryCmds = []*cobra.Command{
		{
			Use:   "q1",
			Short: queryIndexSQL1,
			Run:   query1,
		},
		{
			Use:   "q2",
			Short: queryIndexSQL2,
			Run:   query2,
		},
		{
			Use:   "q3",
			Short: queryIndexSQL3,
			Run:   query3,
		},
		{
			Use:   "q4",
			Short: queryIndexSQL4,
			Run:   query4,
		},
		{
			Use:   "q5",
			Short: queryIndexSQL5,
			Run:   query5,
		},
		{
			Use:   "q6",
			Short: queryIndexSQL6,
			Run:   query6,
		},
	}
)

const (
	prepareDbSQL    = "CREATE DATABASE IF NOT EXISTS %s_%d"
	prepareTableSQL = "CREATE TABLE IF NOT EXISTS %s.%s (id int primary key, %s);"
	queryIndexSQL1  = "SELECT * FROM INFORMATION_SCHEMA.TIDB_INDEXES WHERE table_schema = '%s' AND table_name = '%s';"
	queryIndexSQL2  = "SELECT * FROM INFORMATION_SCHEMA.TIDB_INDEXES WHERE table_schema = '%s' AND table_name = '%s' AND lower(key_name) = 'primary';"
	queryIndexSQL3  = "SELECT * FROM INFORMATION_SCHEMA.TIDB_INDEX_USAGE WHERE table_schema = '%s' AND table_name = '%s';"
	queryIndexSQL4  = "SELECT * FROM INFORMATION_SCHEMA.TIDB_INDEX_USAGE WHERE table_schema = '%s' AND table_name = '%s' AND index_name = '%s';"
	queryIndexSQL5  = "SELECT * FROM information_schema.key_column_usage WHERE table_schema = '%s' AND table_name = '%s';"
	queryIndexSQL6  = "SELECT * FROM information_schema.key_column_usage WHERE table_schema = '%s' AND table_name = '%s' AND lower(constraint_name) = 'primary';"
)

func init_flags() {
	IndexCmd.PersistentFlags().IntVar(&util.DatabaseCnt, "db_cnt", 1, "The number of databases to create")
	IndexCmd.PersistentFlags().StringVar(&util.DatabaseNamePrefix, "db_prefix", "info_test", "The prefix of the database name")
	IndexCmd.PersistentFlags().IntVar(&util.TableCnt, "table_cnt", 10, "The number of tables to create")
	IndexCmd.PersistentFlags().StringVar(&util.TableNamePrefix, "table_prefix", "t", "The prefix of the table name")
	IndexCmd.PersistentFlags().IntVar(&util.ColumnCnt, "column_cnt", 5, "The number of columns to create")
	IndexCmd.PersistentFlags().StringVar(&util.ColumnNamePrefix, "column_prefix", "c", "The prefix of the column name")
	IndexCmd.PersistentFlags().IntVar(&util.IndexCnt, "index_cnt", 3, "The number of indexes to create")
	IndexCmd.PersistentFlags().StringVar(&util.IndexNamePrefix, "index_prefix", "i", "The prefix of the index name")
}

func init() {
	init_flags()

	IndexCmd.AddCommand(prepareCmd, cleanCmd)
	IndexCmd.AddCommand(queryCmds...)

	if util.IndexCnt > util.ColumnCnt {
		fmt.Fprintf(os.Stderr, "index count is truncated to the same as column count %d", util.ColumnCnt)
		util.IndexCnt = util.ColumnCnt
	}
}

func prepare(_ *cobra.Command, _ []string) {
	chs, clean := util.GetMultiConnsForExec()
	defer clean()

	var sb strings.Builder
	for i := 0; i < util.ColumnCnt; i++ {
		sb.WriteString(fmt.Sprintf("%s_%d int", util.ColumnNamePrefix, i))
		if i != util.ColumnCnt-1 {
			sb.WriteString(", ")
		}
	}
	if util.IndexCnt > 0 {
		sb.WriteString(", ")
		for i := 0; i < util.IndexCnt; i++ {
			sb.WriteString(fmt.Sprintf("key %s_%d(%s_%d)", util.IndexNamePrefix, i, util.ColumnNamePrefix, i))
			if i != util.IndexCnt-1 {
				sb.WriteString(", ")
			}
		}
	}

	for i := 0; i < util.DatabaseCnt; i++ {
		chs[i%util.Thread] <- fmt.Sprintf(prepareDbSQL, util.DatabaseNamePrefix, i)
		for j := 0; j < util.TableCnt; j++ {
			chs[i%util.Thread] <- fmt.Sprintf(prepareTableSQL, fmt.Sprintf("%s_%d", util.DatabaseNamePrefix, i),
				fmt.Sprintf("%s_%d", util.TableNamePrefix, j), sb.String())
		}
	}

	fmt.Println("Finish prepare tables for indexes")
}

func query1(_ *cobra.Command, _ []string) {
	util.QuerySQL(func() string {
		return fmt.Sprintf(queryIndexSQL1, fmt.Sprintf("%s_%d", util.DatabaseNamePrefix, rand.Intn(util.DatabaseCnt)),
			fmt.Sprintf("%s_%d", util.TableNamePrefix, rand.Intn(util.TableCnt)))
	})
	fmt.Printf("Finish query '%s'", queryIndexSQL1)
}

func query2(_ *cobra.Command, _ []string) {
	util.QuerySQL(func() string {
		return fmt.Sprintf(queryIndexSQL2, fmt.Sprintf("%s_%d", util.DatabaseNamePrefix, rand.Intn(util.DatabaseCnt)),
			fmt.Sprintf("%s_%d", util.TableNamePrefix, rand.Intn(util.TableCnt)))
	})
	fmt.Printf("Finish query '%s'", queryIndexSQL2)
}

func query3(_ *cobra.Command, _ []string) {
	util.QuerySQL(func() string {
		return fmt.Sprintf(queryIndexSQL3, fmt.Sprintf("%s_%d", util.DatabaseNamePrefix, rand.Intn(util.DatabaseCnt)),
			fmt.Sprintf("%s_%d", util.TableNamePrefix, rand.Intn(util.TableCnt)))
	})
	fmt.Printf("Finish query '%s'", queryIndexSQL3)
}

func query4(_ *cobra.Command, _ []string) {
	util.QuerySQL(func() string {
		return fmt.Sprintf(queryIndexSQL4, fmt.Sprintf("%s_%d", util.DatabaseNamePrefix, rand.Intn(util.DatabaseCnt)),
			fmt.Sprintf("%s_%d", util.TableNamePrefix, rand.Intn(util.TableCnt)),
			fmt.Sprintf("%s_%d", util.IndexNamePrefix, rand.Intn(util.IndexCnt)))
	})
	fmt.Printf("Finish query '%s'", queryIndexSQL4)
}

func query5(_ *cobra.Command, _ []string) {
	util.QuerySQL(func() string {
		return fmt.Sprintf(queryIndexSQL5, fmt.Sprintf("%s_%d", util.DatabaseNamePrefix, rand.Intn(util.DatabaseCnt)),
			fmt.Sprintf("%s_%d", util.TableNamePrefix, rand.Intn(util.TableCnt)))
	})
	fmt.Printf("Finish query '%s'", queryIndexSQL5)
}

func query6(_ *cobra.Command, _ []string) {
	util.QuerySQL(func() string {
		return fmt.Sprintf(queryIndexSQL6, fmt.Sprintf("%s_%d", util.DatabaseNamePrefix, rand.Intn(util.DatabaseCnt)),
			fmt.Sprintf("%s_%d", util.TableNamePrefix, rand.Intn(util.TableCnt)))
	})
	fmt.Printf("Finish query '%s'", queryIndexSQL6)
}
