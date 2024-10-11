package check

import (
	"fmt"
	"infoschema_perf/cmd/util"
	"math/rand"
	"strings"

	"github.com/spf13/cobra"
)

var (
	CheckConstraintCmd = &cobra.Command{
		Use:   "check",
		Short: "Prepare, test and clean check constraints for test",
	}

	prepareCmd = &cobra.Command{
		Use:   "prepare",
		Short: fmt.Sprintf("Prepare tables for test (%s)", prepareCheckConstraintSQL),
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
			Short: queryCheckConstraintSQL1,
			Run:   query1,
		},
		{
			Use:   "q2",
			Short: queryCheckConstraintSQL2,
			Run:   query2,
		},
		{
			Use:   "q3",
			Short: queryCheckConstraintSQL3,
			Run:   query3,
		},
	}
)

const (
	constraintName            = "%s_chk_%d" // <tableName>_chk_<1, 2, 3...>
	prepareDbSQL              = "CREATE DATABASE IF NOT EXISTS %s_%d"
	prepareCheckConstraintSQL = "CREATE TABLE IF NOT EXISTS %s.%s (id int primary key, %s);"
	queryCheckConstraintSQL1  = "SELECT * FROM information_schema.check_constraints WHERE constraint_schema = '%s' AND constraint_name = '%s';"
	queryCheckConstraintSQL2  = "SELECT * FROM information_schema.tidb_check_constraints WHERE constraint_schema = '%s' AND table_name = '%s';"
	queryCheckConstraintSQL3  = "SELECT * FROM information_schema.tidb_check_constraints WHERE constraint_schema = '%s' AND table_name = '%s' AND constraint_name = '%s';"
)

func init_flags() {
	CheckConstraintCmd.PersistentFlags().IntVar(&util.TableCnt, "table_cnt", 10, "The number of tables to create")
	CheckConstraintCmd.PersistentFlags().StringVar(&util.TableNamePrefix, "table_prefix", "t", "The prefix of the table name")
	CheckConstraintCmd.PersistentFlags().IntVar(&util.ConstraintCnt, "check_cnt", 5, "The number of check constraints to create")
}

func init() {
	init_flags()

	CheckConstraintCmd.AddCommand(prepareCmd, cleanCmd)
	CheckConstraintCmd.AddCommand(queryCmds...)
}

func prepare(_ *cobra.Command, _ []string) {
	chs, clean := util.GetMultiConnsForExec()
	defer clean()

	chs[0] <- "set global tidb_enable_check_constraint = 1;"

	var sb strings.Builder
	for i := 0; i < util.ConstraintCnt; i++ {
		sb.WriteString(fmt.Sprintf("CHECK (id > %d)", i))
		if i != util.ConstraintCnt-1 {
			sb.WriteString(", ")
		}
	}

	for i := 0; i < util.DatabaseCnt; i++ {
		chs[i%util.Thread] <- fmt.Sprintf(prepareDbSQL, util.DatabaseNamePrefix, i)
		for j := 0; j < util.TableCnt; j++ {
			chs[i%util.Thread] <- fmt.Sprintf(prepareCheckConstraintSQL, fmt.Sprintf("%s_%d", util.DatabaseNamePrefix, i),
				fmt.Sprintf("%s_%d", util.TableNamePrefix, j), sb.String())
		}
	}

	fmt.Println("Finish prepare tables for check constraints")
}

func query1(_ *cobra.Command, _ []string) {
	util.QuerySQL(func() string {
		return fmt.Sprintf(queryCheckConstraintSQL1, fmt.Sprintf("%s_%d", util.DatabaseNamePrefix, rand.Intn(util.DatabaseCnt)),
			fmt.Sprintf(constraintName, fmt.Sprintf("%s_%d", util.TableNamePrefix, rand.Intn(util.TableCnt)), rand.Intn(util.ConstraintCnt)+1))
	})
	fmt.Printf("Finish query '%s'", queryCheckConstraintSQL1)
}

func query2(_ *cobra.Command, _ []string) {
	util.QuerySQL(func() string {
		return fmt.Sprintf(queryCheckConstraintSQL2, fmt.Sprintf("%s_%d", util.DatabaseNamePrefix, rand.Intn(util.DatabaseCnt)),
			fmt.Sprintf("%s_%d", util.TableNamePrefix, rand.Intn(util.TableCnt)))
	})
	fmt.Printf("Finish query '%s'", queryCheckConstraintSQL2)
}

func query3(_ *cobra.Command, _ []string) {
	util.QuerySQL(func() string {
		return fmt.Sprintf(queryCheckConstraintSQL3, fmt.Sprintf("%s_%d", util.DatabaseNamePrefix, rand.Intn(util.DatabaseCnt)),
			fmt.Sprintf("%s_%d", util.TableNamePrefix, rand.Intn(util.TableCnt)),
			fmt.Sprintf(constraintName, fmt.Sprintf("%s_%d", util.TableNamePrefix, rand.Intn(util.TableCnt)), rand.Intn(util.ConstraintCnt)+1))
	})
	fmt.Printf("Finish query '%s'", queryCheckConstraintSQL3)
}
