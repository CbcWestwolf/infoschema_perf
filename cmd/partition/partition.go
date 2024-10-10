package partition

import (
	"fmt"
	"infoschema_perf/cmd/util"
	"math/rand"

	"github.com/spf13/cobra"
)

var (
	PartitionCmd = &cobra.Command{
		Use:   "partition",
		Short: "Prepare, test and clean partition for test",
	}

	prepareCmd = &cobra.Command{
		Use:   "prepare",
		Short: fmt.Sprintf("Prepare partitions for test (%s)", preparePartitionSQL),
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
			Short: queryPartitionSQL1,
			Run:   query1,
		},
		{
			Use:   "q2",
			Short: queryPartitionSQL2,
			Run:   query2,
		},
	}
)

const (
	prepareDbSQL        = "CREATE DATABASE IF NOT EXISTS %s_%d"
	preparePartitionSQL = `CREATE TABLE IF NOT EXISTS %s.%s (id int primary key NONCLUSTERED GLOBAL, num int) PARTITION BY RANGE (num) (
    PARTITION p0 VALUES LESS THAN (6),
    PARTITION p1 VALUES LESS THAN (11),
    PARTITION p2 VALUES LESS THAN (16),
    PARTITION p3 VALUES LESS THAN (21)
	);`
	queryPartitionSQL1 = "SELECT PARTITION_NAME FROM INFORMATION_SCHEMA.PARTITIONS WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s'"
	// SELECT sum(table_rows) FROM information_schema.partitions WHERE tidb_partition_id IN (%?);
	// It is hard to get tidb_partition_id
	queryPartitionSQL2 = "SELECT sum(table_rows) FROM INFORMATION_SCHEMA.PARTITIONS WHERE TABLE_SCHEMA = '%s' AND tidb_partition_id IS NOT NULL;"
)

func init_flags() {
	PartitionCmd.PersistentFlags().IntVar(&util.TableCnt, "table_cnt", 10, "The number of tables to create")
	PartitionCmd.PersistentFlags().StringVar(&util.TableNamePrefix, "table_prefix", "t", "The prefix of the table name")
}

func init() {
	init_flags()

	PartitionCmd.AddCommand(prepareCmd, cleanCmd)
	PartitionCmd.AddCommand(queryCmds...)
}

func prepare(_ *cobra.Command, _ []string) {
	chs, clean := util.GetMultiConnsForExec()
	defer clean()

	for i := 0; i < util.DatabaseCnt; i++ {
		chs[i%util.Thread] <- fmt.Sprintf(prepareDbSQL, util.DatabaseNamePrefix, i)
		for j := 0; j < util.TableCnt; j++ {
			chs[i%util.Thread] <- fmt.Sprintf(preparePartitionSQL, fmt.Sprintf("%s_%d", util.DatabaseNamePrefix, i),
				fmt.Sprintf("%s_%d", util.TableNamePrefix, j))
		}
	}

	fmt.Println("Finish prepare tables")
}

func query1(_ *cobra.Command, _ []string) {
	util.QuerySQL(func() string {
		return fmt.Sprintf(queryPartitionSQL1, fmt.Sprintf("%s_%d", util.DatabaseNamePrefix, rand.Intn(util.DatabaseCnt)),
			fmt.Sprintf("%s_%d", util.TableNamePrefix, rand.Intn(util.TableCnt)))
	})
	fmt.Printf("Finish query '%s'", queryPartitionSQL1)
}

func query2(_ *cobra.Command, _ []string) {
	util.QuerySQL(func() string {
		return fmt.Sprintf(queryPartitionSQL2, fmt.Sprintf("%s_%d", util.DatabaseNamePrefix, rand.Intn(util.DatabaseCnt)))
	})
	fmt.Printf("Finish query '%s'", queryPartitionSQL2)
}
