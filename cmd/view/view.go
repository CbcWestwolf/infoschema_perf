package view

import (
	"fmt"
	"infoschema_perf/cmd/util"
	"math/rand"

	"github.com/spf13/cobra"
)

var (
	ViewCmd = &cobra.Command{
		Use:   "view",
		Short: "Prepare, test and clean view for test",
	}

	prepareCmd = &cobra.Command{
		Use:   "prepare",
		Short: fmt.Sprintf("Prepare views for test (%s)", prepareViewSQL),
		Run:   prepare,
	}

	cleanCmd = &cobra.Command{
		Use:   "clean",
		Short: fmt.Sprintf("Clean views after test (%s)", util.CleanSQL),
		Run:   util.Clean,
	}

	queryCmds = []*cobra.Command{
		{
			Use:   "q1",
			Short: queryViewSQL1,
			Run:   query1,
		},
		{
			Use:   "q2",
			Short: queryViewSQL2,
			Run:   query2,
		},
	}
)

const (
	prepareDbSQL    = "CREATE DATABASE IF NOT EXISTS %s_%d"
	prepareTableSQL = "CREATE TABLE IF NOT EXISTS %s.%s (id int primary key, name varchar(255));"
	prepareViewSQL  = "CREATE OR REPLACE VIEW %s.%s AS SELECT * FROM %s.%s"
	queryViewSQL1   = "SELECT * FROM information_schema.views WHERE table_schema = '%s';"
	queryViewSQL2   = "SELECT * FROM information_schema.views WHERE table_schema = '%s' AND table_name = '%s';"
)

func init_flags() {
	ViewCmd.PersistentFlags().IntVar(&util.DatabaseCnt, "db_cnt", 1, "The number of databases to create")
	ViewCmd.PersistentFlags().StringVar(&util.DatabaseNamePrefix, "db_prefix", "info_test", "The prefix of the database name")
	ViewCmd.PersistentFlags().IntVar(&util.TableCnt, "table_cnt", 10, "The number of views to create")
	ViewCmd.PersistentFlags().StringVar(&util.TableNamePrefix, "table_prefix", "t", "The prefix of the table name")
	ViewCmd.PersistentFlags().StringVar(&util.ViewNamePrefix, "view_prefix", "v", "The prefix of the view name")
}

func init() {
	init_flags()

	ViewCmd.AddCommand(prepareCmd, cleanCmd)
	ViewCmd.AddCommand(queryCmds...)
}

func prepare(_ *cobra.Command, _ []string) {
	chs, clean := util.GetMultiConnsForExec()
	defer clean()

	for i := 0; i < util.DatabaseCnt; i++ {
		chs[i%util.Thread] <- fmt.Sprintf(prepareDbSQL, util.DatabaseNamePrefix, i)
		chs[i%util.Thread] <- fmt.Sprintf(prepareTableSQL, fmt.Sprintf("%s_%d", util.DatabaseNamePrefix, i), util.TableNamePrefix)
		for j := 0; j < util.TableCnt; j++ {
			chs[i%util.Thread] <- fmt.Sprintf(prepareViewSQL, fmt.Sprintf("%s_%d", util.DatabaseNamePrefix, i),
				fmt.Sprintf("%s_%d", util.ViewNamePrefix, j),
				fmt.Sprintf("%s_%d", util.DatabaseNamePrefix, i), util.TableNamePrefix)
		}
	}

	fmt.Println("Finish prepare tables")
}

func query1(_ *cobra.Command, _ []string) {
	util.QuerySQL(func() string {
		return fmt.Sprintf(queryViewSQL1, fmt.Sprintf("%s_%d", util.DatabaseNamePrefix, rand.Intn(util.DatabaseCnt)))
	})
	fmt.Printf("Finish query '%s'", queryViewSQL1)
}

func query2(_ *cobra.Command, _ []string) {
	util.QuerySQL(func() string {
		return fmt.Sprintf(queryViewSQL2, fmt.Sprintf("%s_%d", util.DatabaseNamePrefix, rand.Intn(util.DatabaseCnt)),
			fmt.Sprintf("%s_%d", util.ViewNamePrefix, rand.Intn(util.TableCnt)))
	})
	fmt.Printf("Finish query '%s'", queryViewSQL2)
}
