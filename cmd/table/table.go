package table

import (
	"fmt"
	"infoschema_perf/cmd/util"
	"math/rand"
	"strings"

	"github.com/spf13/cobra"
)

var (
	TableCmd = &cobra.Command{
		Use:   "table",
		Short: "Prepare, test and clean table for test",
	}

	prepareCmd = &cobra.Command{
		Use:   "prepare",
		Short: fmt.Sprintf("Prepare tables for test (%s)", prepareTableSQL),
		Run:   prepare,
	}

	cleanCmd = &cobra.Command{
		Use:   "clean",
		Short: fmt.Sprintf("Clean tables after test (%s)", cleanSQL),
		Run:   clean,
	}

	queryCmds = []*cobra.Command{
		{
			Use:   "table_q1",
			Short: queryTableSQL1,
			Run:   query1,
		},
		{
			Use:   "table_q2",
			Short: queryTableSQL2,
			Run:   query2,
		},
	}
)

const (
	prepareDbSQL    = "CREATE DATABASE IF NOT EXISTS %s_%d"
	prepareTableSQL = "CREATE TABLE IF NOT EXISTS %s.%s (id int primary key, name varchar(255));"
	cleanSQL        = "DROP DATABASE IF EXISTS %s_%d"
	queryTableSQL1  = "SELECT * FROM information_schema.tables WHERE TABLE_SCHEMA NOT IN (%s) limit 10000;"
	queryTableSQL2  = "SELECT * FROM information_schema.tables WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s';"
)

func init_flags() {
	TableCmd.PersistentFlags().IntVar(&util.DatabaseCnt, "db_cnt", 1, "The number of databases to create")
	TableCmd.PersistentFlags().StringVar(&util.DatabaseNamePrefix, "db_prefix", "info_test", "The prefix of the database name")
	TableCmd.PersistentFlags().IntVar(&util.TableCnt, "table_cnt", 10, "The number of tables to create")
	TableCmd.PersistentFlags().StringVar(&util.TableNamePrefix, "table_prefix", "t", "The prefix of the table name")
}

func init() {
	init_flags()

	TableCmd.AddCommand(prepareCmd, cleanCmd)
	TableCmd.AddCommand(queryCmds...)
}

func prepare(_ *cobra.Command, _ []string) {
	chs, clean := util.GetMultiConnsForExec()
	defer clean()

	for i := 0; i < util.DatabaseCnt; i++ {
		chs[i%util.Thread] <- fmt.Sprintf(prepareDbSQL, util.DatabaseNamePrefix, i)
		for j := 0; j < util.TableCnt; j++ {
			chs[i%util.Thread] <- fmt.Sprintf(prepareTableSQL, fmt.Sprintf("%s_%d", util.DatabaseNamePrefix, i),
				fmt.Sprintf("%s_%d", util.TableNamePrefix, j))
		}
	}

	fmt.Println("Finish prepare tables")
}

func clean(_ *cobra.Command, _ []string) {
	chs, clean := util.GetMultiConnsForExec()
	defer clean()

	for i := 0; i < util.DatabaseCnt; i++ {
		chs[i%util.Thread] <- fmt.Sprintf(cleanSQL, util.DatabaseNamePrefix, i)
	}

	fmt.Println("Finish clean tables")
}

func generateTableList() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("'%s_%d'", util.DatabaseNamePrefix, rand.Intn(util.DatabaseCnt)))
	for i := 1; i <= rand.Intn(util.DatabaseCnt); i++ {
		sb.WriteString(",")
		sb.WriteString(fmt.Sprintf("'%s_%d'", util.DatabaseNamePrefix, rand.Intn(util.DatabaseCnt)))
	}

	return sb.String()
}

func query1(_ *cobra.Command, _ []string) {
	util.QuerySQL(func() string {
		return fmt.Sprintf(queryTableSQL1, generateTableList())
	})
	fmt.Printf("Finish query '%s'", queryTableSQL1)
}

func query2(_ *cobra.Command, _ []string) {
	util.QuerySQL(func() string {
		return fmt.Sprintf(queryTableSQL2, fmt.Sprintf("%s_%d", util.DatabaseNamePrefix, rand.Intn(util.DatabaseCnt)),
			fmt.Sprintf("%s_%d", util.TableNamePrefix, rand.Intn(util.TableCnt)))
	})
	fmt.Printf("Finish query '%s'", queryTableSQL2)
}
