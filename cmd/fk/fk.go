package fk

import (
	"fmt"
	"infoschema_perf/cmd/util"
	"math/rand"

	"github.com/spf13/cobra"
)

var (
	FkCmd = &cobra.Command{
		Use:   "fk",
		Short: "Prepare, test and clean fk for test",
	}

	prepareCmd = &cobra.Command{
		Use:   "prepare",
		Short: fmt.Sprintf("Prepare fk for test (%s)", prepareFkTableSQL),
		Run:   prepare,
	}

	cleanCmd = &cobra.Command{
		Use:   "clean",
		Short: fmt.Sprintf("Clean fk after test (%s)", util.CleanSQL),
		Run:   util.Clean,
	}

	queryCmds = []*cobra.Command{
		{
			Use:   "q1",
			Short: queryFkSQL1,
			Run:   query1,
		},
		{
			Use:   "q2",
			Short: queryFkSQL2,
			Run:   query2,
		},
	}
)

const (
	prepareDbSQL            = "CREATE DATABASE IF NOT EXISTS %s_%d"
	prepareRefferedTableSQL = "CREATE TABLE IF NOT EXISTS %s.%s (id int primary key, name varchar(255));"
	prepareFkTableSQL       = "CREATE TABLE IF NOT EXISTS %s.%s (id int primary key, f_id int, foreign key fk_id (f_id) references %s(id));"
	queryFkSQL1             = "SELECT * FROM information_schema.key_column_usage WHERE table_schema = '%s' AND table_name = '%s' AND referenced_table_schema IS NOT NULL;"
	queryFkSQL2             = "SELECT * FROM information_schema.table_constraints WHERE table_schema = '%s' AND table_name = '%s' AND constraint_type = 'FOREIGN KEY';"
)

func init_flags() {
	FkCmd.PersistentFlags().IntVar(&util.TableCnt, "table_cnt", 10, "The number of views to create")
	FkCmd.PersistentFlags().StringVar(&util.TableNamePrefix, "table_prefix", "t", "The prefix of the table name")
}

func init() {
	init_flags()

	FkCmd.AddCommand(prepareCmd, cleanCmd)
	FkCmd.AddCommand(queryCmds...)
}

func prepare(_ *cobra.Command, _ []string) {
	chs, clean := util.GetMultiConnsForExec()
	defer clean()

	for i := 0; i < util.DatabaseCnt; i++ {
		chs[i%util.Thread] <- fmt.Sprintf(prepareDbSQL, util.DatabaseNamePrefix, i)
		chs[i%util.Thread] <- fmt.Sprintf(prepareRefferedTableSQL, fmt.Sprintf("%s_%d", util.DatabaseNamePrefix, i), util.TableNamePrefix)
		for j := 0; j < util.TableCnt; j++ {
			chs[i%util.Thread] <- fmt.Sprintf(prepareFkTableSQL, fmt.Sprintf("%s_%d", util.DatabaseNamePrefix, i),
				fmt.Sprintf("%s_%d", util.TableNamePrefix, j),
				util.TableNamePrefix)
		}
	}

	fmt.Println("Finish prepare tables")
}

func query1(_ *cobra.Command, _ []string) {
	util.QuerySQL(func() string {
		return fmt.Sprintf(queryFkSQL1, fmt.Sprintf("%s_%d", util.DatabaseNamePrefix, rand.Intn(util.DatabaseCnt)),
			fmt.Sprintf("%s_%d", util.TableNamePrefix, rand.Intn(util.TableCnt)),
		)
	})
	fmt.Printf("Finish query '%s'", queryFkSQL1)
}

func query2(_ *cobra.Command, _ []string) {
	util.QuerySQL(func() string {
		return fmt.Sprintf(queryFkSQL2, fmt.Sprintf("%s_%d", util.DatabaseNamePrefix, rand.Intn(util.DatabaseCnt)),
			fmt.Sprintf("%s_%d", util.TableNamePrefix, rand.Intn(util.TableCnt)),
		)
	})
	fmt.Printf("Finish query '%s'", queryFkSQL2)
}
