package column

import (
	"fmt"
	"infoschema_perf/cmd/util"
	"math/rand"
	"strings"

	"github.com/spf13/cobra"
)

var (
	ColumnCmd = &cobra.Command{
		Use:   "column",
		Short: "Prepare and test column",
	}

	prepareCmd = &cobra.Command{
		Use:   "prepare",
		Short: fmt.Sprintf("Prepare tables for test (%s)", prepareColumnSQL),
		Run:   prepare,
	}

	queryCmds = []*cobra.Command{
		{
			Use:   "q1",
			Short: queryColumnSQL1,
			Run:   query1,
		},
		{
			Use:   "q2",
			Short: queryColumnSQL2,
			Run:   query2,
		},
		{
			Use:   "q3",
			Short: queryColumnSQL3,
			Run:   query3,
		},
	}
)

const (
	prepareDbSQL     = "CREATE DATABASE IF NOT EXISTS %s_%d"
	prepareColumnSQL = "CREATE TABLE IF NOT EXISTS %s.%s (id int primary key, %s);"
	queryColumnSQL1  = "SELECT table_name, column_name, column_type, generation_expression, extra FROM information_schema.columns WHERE table_schema = '%s' ORDER BY table_name, ordinal_position;"
	queryColumnSQL2  = "SELECT * FROM information_schema.columns WHERE table_schema = '%s' AND table_name = '%s';"
	queryColumnSQL3  = "SELECT * FROM information_schema.columns WHERE table_schema = '%s' AND table_name = '%s' AND column_name = '%s';"
)

func init_flags() {
	ColumnCmd.PersistentFlags().IntVar(&util.TableCnt, "table_cnt", 10, "The number of tables to create")
	ColumnCmd.PersistentFlags().StringVar(&util.TableNamePrefix, "table_prefix", "t", "The prefix of the table name")
	ColumnCmd.PersistentFlags().IntVar(&util.ColumnCnt, "column_cnt", 5, "The number of columns to create")
	ColumnCmd.PersistentFlags().StringVar(&util.ColumnNamePrefix, "column_prefix", "c", "The prefix of the column name")
}

func init() {
	init_flags()

	ColumnCmd.AddCommand(prepareCmd)
	ColumnCmd.AddCommand(queryCmds...)
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

	for i := util.DatabaseStart; i < util.DatabaseEnd; i++ {
		chs[i%util.Thread] <- fmt.Sprintf(prepareDbSQL, util.DatabaseNamePrefix, i)
		for j := 0; j < util.TableCnt; j++ {
			chs[i%util.Thread] <- fmt.Sprintf(prepareColumnSQL, fmt.Sprintf("%s_%d", util.DatabaseNamePrefix, i),
				fmt.Sprintf("%s_%d", util.TableNamePrefix, j), sb.String())
		}
	}

	fmt.Println("Finish prepare table for column")
}

func query1(_ *cobra.Command, _ []string) {
	util.QuerySQL(func() string {
		return fmt.Sprintf(queryColumnSQL1, fmt.Sprintf("%s_%d", util.DatabaseNamePrefix, rand.Intn(util.DatabaseEnd-util.DatabaseStart)+util.DatabaseStart))
	})
	fmt.Printf("Finish query '%s'", queryColumnSQL1)
}

func query2(_ *cobra.Command, _ []string) {
	util.QuerySQL(func() string {
		return fmt.Sprintf(queryColumnSQL2, fmt.Sprintf("%s_%d", util.DatabaseNamePrefix, rand.Intn(util.DatabaseEnd-util.DatabaseStart)+util.DatabaseStart),
			fmt.Sprintf("%s_%d", util.TableNamePrefix, rand.Intn(util.TableCnt)))
	})
	fmt.Printf("Finish query '%s'", queryColumnSQL2)
}

func query3(_ *cobra.Command, _ []string) {
	util.QuerySQL(func() string {
		return fmt.Sprintf(queryColumnSQL3, fmt.Sprintf("%s_%d", util.DatabaseNamePrefix, rand.Intn(util.DatabaseEnd-util.DatabaseStart)+util.DatabaseStart),
			fmt.Sprintf("%s_%d", util.TableNamePrefix, rand.Intn(util.TableCnt)),
			fmt.Sprintf("%s_%d", util.ColumnNamePrefix, rand.Intn(util.ColumnCnt)))
	})
	fmt.Printf("Finish query '%s'", queryColumnSQL3)
}
