package statistics

import (
	"fmt"
	"infoschema_perf/cmd/util"
	"math/rand"
	"strings"

	"github.com/spf13/cobra"
)

var (
	StatisticsCmd = &cobra.Command{
		Use:   "statistics",
		Short: "Prepare, test and clean table for test",
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
			Short: queryStatisticsSQL1,
			Run:   query1,
		},
		{
			Use:   "q2",
			Short: queryStatisticsSQL2,
			Run:   query2,
		},
	}
)

const (
	prepareDbSQL        = "CREATE DATABASE IF NOT EXISTS %s_%d"
	prepareTableSQL     = "CREATE TABLE IF NOT EXISTS %s.%s (id int primary key, %s);"
	queryStatisticsSQL1 = "SELECT * FROM INFORMATION_SCHEMA.STATISTICS WHERE table_schema = '%s' AND table_name = '%s' AND column_name = '%s';"
	queryStatisticsSQL2 = "SELECT * FROM INFORMATION_SCHEMA.STATISTICS WHERE table_schema = '%s' AND table_name = '%s' AND index_name = 'PRIMARY' ;"
)

func init_flags() {
	StatisticsCmd.PersistentFlags().IntVar(&util.TableCnt, "table_cnt", 10, "The number of tables to create")
	StatisticsCmd.PersistentFlags().StringVar(&util.TableNamePrefix, "table_prefix", "t", "The prefix of the table name")
	StatisticsCmd.PersistentFlags().IntVar(&util.ColumnCnt, "column_cnt", 5, "The number of columns to create")
	StatisticsCmd.PersistentFlags().StringVar(&util.ColumnNamePrefix, "column_prefix", "c", "The prefix of the column name")
}

func init() {
	init_flags()

	StatisticsCmd.AddCommand(prepareCmd, cleanCmd)
	StatisticsCmd.AddCommand(queryCmds...)
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

	for i := 0; i < util.DatabaseCnt; i++ {
		chs[i%util.Thread] <- fmt.Sprintf(prepareDbSQL, util.DatabaseNamePrefix, i)
		for j := 0; j < util.TableCnt; j++ {
			chs[i%util.Thread] <- fmt.Sprintf(prepareTableSQL, fmt.Sprintf("%s_%d", util.DatabaseNamePrefix, i),
				fmt.Sprintf("%s_%d", util.TableNamePrefix, j), sb.String())
		}
	}

	fmt.Println("Finish prepare tables for statistics")
}

func query1(_ *cobra.Command, _ []string) {
	util.QuerySQL(func() string {
		return fmt.Sprintf(queryStatisticsSQL1, fmt.Sprintf("%s_%d", util.DatabaseNamePrefix, rand.Intn(util.DatabaseCnt)),
			fmt.Sprintf("%s_%d", util.TableNamePrefix, rand.Intn(util.TableCnt)),
			fmt.Sprintf("%s_%d", util.ColumnNamePrefix, rand.Intn(util.ColumnCnt)))
	})
	fmt.Printf("Finish query '%s'", queryStatisticsSQL1)
}

func query2(_ *cobra.Command, _ []string) {
	util.QuerySQL(func() string {
		return fmt.Sprintf(queryStatisticsSQL2, fmt.Sprintf("%s_%d", util.DatabaseNamePrefix, rand.Intn(util.DatabaseCnt)),
			fmt.Sprintf("%s_%d", util.TableNamePrefix, rand.Intn(util.TableCnt)))
	})
	fmt.Printf("Finish query '%s'", queryStatisticsSQL2)
}
