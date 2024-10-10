package db

import (
	"fmt"
	"infoschema_perf/cmd/util"
	"math/rand"

	"github.com/spf13/cobra"
)

var (
	DbCmd = &cobra.Command{
		Use:   "db",
		Short: "Prepare, test and clean database for test",
	}

	prepareCmd = &cobra.Command{
		Use:   "prepare",
		Short: fmt.Sprintf("Prepare databases for test (%s)", prepareDbSQL),
		Run:   prepare,
	}

	cleanCmd = &cobra.Command{
		Use:   "clean",
		Short: fmt.Sprintf("Clean databases after test (%s)", cleanDbSQL),
		Run:   clean,
	}

	queryCmds = []*cobra.Command{
		{
			Use:   "q1",
			Short: queryDbSQL1,
			Run:   query1,
		},
		{
			Use:   "q2",
			Short: queryDbSQL2,
			Run:   query2,
		},
	}
)

const (
	prepareDbSQL = "CREATE DATABASE IF NOT EXISTS %s_%d"
	cleanDbSQL   = "DROP DATABASE IF EXISTS %s_%d"
	queryDbSQL1  = "SELECT * FROM information_schema.schemata WHERE schema_name = '%s_%d'"
	queryDbSQL2  = "SELECT * FROM information_schema.schemata WHERE schema_name LIKE '%s%%';"
)

func init_flags() {
	DbCmd.PersistentFlags().IntVar(&util.DatabaseCnt, "db_cnt", 1, "The number of databases to create")
	DbCmd.PersistentFlags().StringVar(&util.DatabaseNamePrefix, "db_prefix", "info_test", "The prefix of the database name")
}

func init() {
	init_flags()

	DbCmd.AddCommand(prepareCmd, cleanCmd)
	DbCmd.AddCommand(queryCmds...)
}

func prepare(_ *cobra.Command, _ []string) {
	chs, clean := util.GetMultiConnsForExec()
	defer clean()

	for i := 0; i < util.DatabaseCnt; i++ {
		sql := fmt.Sprintf(prepareDbSQL, util.DatabaseNamePrefix, i)
		chs[i%util.Thread] <- sql
	}
	fmt.Println("Finish prepare databases")
}

func clean(_ *cobra.Command, _ []string) {
	chs, clean := util.GetMultiConnsForExec()
	defer clean()

	for i := 0; i < util.DatabaseCnt; i++ {
		sql := fmt.Sprintf(cleanDbSQL, util.DatabaseNamePrefix, i)
		chs[i%util.Thread] <- sql
	}
	fmt.Println("Finish clean databases")
}

func query1(_ *cobra.Command, _ []string) {
	util.QuerySQL(func() string {
		return fmt.Sprintf(queryDbSQL1, util.DatabaseNamePrefix, rand.Intn(util.DatabaseCnt))
	})
	fmt.Printf("Finish query '%s'", queryDbSQL1)
}

func query2(_ *cobra.Command, _ []string) {
	util.QuerySQL(func() string {
		return fmt.Sprintf(queryDbSQL2, util.DatabaseNamePrefix)
	})
	fmt.Printf("Finish query '%s'", queryDbSQL2)
}
