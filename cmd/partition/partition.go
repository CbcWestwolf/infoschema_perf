package partition

import (
	"fmt"
	"infoschema_perf/cmd/util"
	"math/rand"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

var (
	PartitionCmd = &cobra.Command{
		Use:   "partition",
		Short: "Prepare and test partition",
	}

	prepareCmd = &cobra.Command{
		Use:   "prepare",
		Short: fmt.Sprintf("Prepare partitions for test (%s)", preparePartitionSQL),
		Run:   prepare,
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
		{
			Use:   "q3",
			Short: queryPartitionSQL3,
			Run:   query3,
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
	preparePartitionTemplate = `CREATE TABLE IF NOT EXISTS %s.%s (id int primary key NONCLUSTERED GLOBAL, num int%s) PARTITION BY RANGE (num) (
    %s
	);`
	queryPartitionSQL1 = "SELECT PARTITION_NAME FROM INFORMATION_SCHEMA.PARTITIONS WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s'"
	// SELECT sum(table_rows) FROM information_schema.partitions WHERE tidb_partition_id IN (%?);
	// It is hard to get tidb_partition_id
	queryPartitionSQL2 = "SELECT sum(table_rows) FROM INFORMATION_SCHEMA.PARTITIONS WHERE TABLE_SCHEMA = '%s' AND tidb_partition_id IS NOT NULL;"
	queryPartitionSQL3 = "SELECT count(*) FROM information_schema.partitions WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s' AND partition_name = '%s'"
)

func init_flags() {
	PartitionCmd.PersistentFlags().IntVar(&util.TableCnt, "table_cnt", 10, "The number of tables to create")
	PartitionCmd.PersistentFlags().StringVar(&util.TableNamePrefix, "table_prefix", "t", "The prefix of the table name")
	PartitionCmd.PersistentFlags().IntVar(&util.ColumnCnt, "column_cnt", 5, "The number of columns to create")
	PartitionCmd.PersistentFlags().StringVar(&util.ColumnNamePrefix, "column_prefix", "c", "The prefix of the column name")
	PartitionCmd.PersistentFlags().IntVar(&util.PartitionCnt, "partition_cnt", 5, "The number of partitions to create")
}

func init() {
	init_flags()

	PartitionCmd.AddCommand(prepareCmd)
	PartitionCmd.AddCommand(queryCmds...)
}

func prepare(_ *cobra.Command, _ []string) {
	chs, clean := util.GetMultiConnsForExec()
	defer clean()

	var columnSB strings.Builder
	for i := 2; i < util.ColumnCnt; i++ {
		columnSB.WriteString(fmt.Sprintf(", %s_%d int", util.ColumnNamePrefix, i))
	}
	column := columnSB.String()

	var partitionSB strings.Builder
	for i := 0; i < util.PartitionCnt; i++ {
		if i != 0 {
			partitionSB.WriteString(", ")
		}
		partitionSB.WriteString(fmt.Sprintf("PARTITION p%d VALUES LESS THAN (%d)", i, (i+1)*5+1))
	}
	partition := partitionSB.String()

	for i := util.DatabaseStart; i < util.DatabaseEnd; i++ {
		chs[i%util.Thread] <- fmt.Sprintf(prepareDbSQL, util.DatabaseNamePrefix, i)
	}

	time.Sleep(2 * time.Second)

	for i := util.DatabaseStart; i < util.DatabaseEnd; i++ {
		for j := 0; j < util.TableCnt; j++ {
			chs[(i+j)%util.Thread] <- fmt.Sprintf(preparePartitionTemplate, fmt.Sprintf("%s_%d", util.DatabaseNamePrefix, i),
				fmt.Sprintf("%s_%d", util.TableNamePrefix, j), column, partition)
		}
	}

	fmt.Println("Finish prepare tables")
}

func query1(_ *cobra.Command, _ []string) {
	util.QuerySQL(func() string {
		return fmt.Sprintf(queryPartitionSQL1, fmt.Sprintf("%s_%d", util.DatabaseNamePrefix, rand.Intn(util.DatabaseEnd-util.DatabaseStart)+util.DatabaseStart),
			fmt.Sprintf("%s_%d", util.TableNamePrefix, rand.Intn(util.TableCnt)))
	})
	fmt.Printf("Finish query '%s'", queryPartitionSQL1)
}

func query2(_ *cobra.Command, _ []string) {
	util.QuerySQL(func() string {
		return fmt.Sprintf(queryPartitionSQL2, fmt.Sprintf("%s_%d", util.DatabaseNamePrefix, rand.Intn(util.DatabaseEnd-util.DatabaseStart)+util.DatabaseStart))
	})
	fmt.Printf("Finish query '%s'", queryPartitionSQL2)
}

func query3(_ *cobra.Command, _ []string) {
	util.QuerySQL(func() string {
		return fmt.Sprintf(queryPartitionSQL3, fmt.Sprintf("%s_%d", util.DatabaseNamePrefix, rand.Intn(util.DatabaseEnd-util.DatabaseStart)+util.DatabaseStart),
			fmt.Sprintf("%s_%d", util.TableNamePrefix, rand.Intn(util.TableCnt)),
			fmt.Sprintf("p%d", rand.Intn(util.PartitionCnt)))
	})
	fmt.Printf("Finish query '%s'", queryPartitionSQL3)
}
