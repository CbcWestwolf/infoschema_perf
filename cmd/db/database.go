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
		Short: "Prepare and test database",
	}

	prepareCmd = &cobra.Command{
		Use:   "prepare",
		Short: fmt.Sprintf("Prepare databases for test (%s)", prepareDbSQL),
		Run:   prepare,
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
	queryDbSQL1  = "SELECT * FROM information_schema.schemata WHERE schema_name = '%s_%d'"
	queryDbSQL2  = "SELECT SCHEMA_NAME FROM information_schema.schemata WHERE schema_name LIKE '%s%%';"
)

func init() {
	DbCmd.AddCommand(prepareCmd)
	DbCmd.AddCommand(queryCmds...)
}

func prepare(_ *cobra.Command, _ []string) {
	chs, clean := util.GetMultiConnsForExec()
	defer clean()

	for i := util.DatabaseStart; i < util.DatabaseEnd; i++ {
		sql := fmt.Sprintf(prepareDbSQL, util.DatabaseNamePrefix, i)
		chs[i%util.Thread] <- sql
	}
	fmt.Println("Finish prepare databases")
}

func query1(_ *cobra.Command, _ []string) {
	util.QuerySQL(func() string {
		return fmt.Sprintf(queryDbSQL1, util.DatabaseNamePrefix, rand.Intn(util.DatabaseEnd-util.DatabaseStart)+util.DatabaseStart)
	})
	fmt.Printf("Finish query '%s'", queryDbSQL1)
}

func query2(_ *cobra.Command, _ []string) {
	util.QuerySQL(func() string {
		return fmt.Sprintf(queryDbSQL2, fmt.Sprintf("%s_%d", util.DatabaseNamePrefix, rand.Intn(util.DatabaseEnd-util.DatabaseStart)+util.DatabaseStart))
	})
	fmt.Printf("Finish query '%s'", queryDbSQL2)
}
