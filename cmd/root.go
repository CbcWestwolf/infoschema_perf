package cmd

import (
	"fmt"
	"infoschema_perf/cmd/check"
	"infoschema_perf/cmd/column"
	"infoschema_perf/cmd/db"
	"infoschema_perf/cmd/fk"
	"infoschema_perf/cmd/index"
	"infoschema_perf/cmd/partition"
	"infoschema_perf/cmd/sequence"
	"infoschema_perf/cmd/statistics"
	"infoschema_perf/cmd/table"
	"infoschema_perf/cmd/util"
	"infoschema_perf/cmd/view"

	"github.com/spf13/cobra"
)

var (
	rootCmd = &cobra.Command{Use: "infoschema_perf", Short: "infoschema_perf is a tool to test the performance of information_schema queries"}
)

const (
	cleanSQL = "DROP DATABASE IF EXISTS %s_%d"
)

// Execute executes the root command.
func Execute() error {
	return rootCmd.Execute()
}

func init_flags() {
	rootCmd.PersistentFlags().StringVar(&util.Host, "host", "127.0.0.1", "The host of the database")
	rootCmd.PersistentFlags().IntVar(&util.Port, "port", 4000, "The port of the database")
	rootCmd.PersistentFlags().StringVar(&util.User, "user", "root", "The user of the database")
	rootCmd.PersistentFlags().IntVar(&util.Thread, "thread", 8, "The number of threads to use")
	rootCmd.PersistentFlags().StringVar(&util.TimeStr, "time", "60s", "The duration of the test.")
	rootCmd.PersistentFlags().BoolVar(&util.Stdout, "stdout", false, "Whether to print the result to stdout")

	rootCmd.PersistentFlags().IntVar(&util.DatabaseStart, "db_start", 0, "The start of databases to create")
	rootCmd.PersistentFlags().IntVar(&util.DatabaseEnd, "db_end", 3, "The end of databases to create")
	rootCmd.PersistentFlags().StringVar(&util.DatabaseNamePrefix, "db_prefix", "info_test", "The prefix of the database name")
}

var cleanCmd = &cobra.Command{
	Use:   "clean",
	Short: fmt.Sprintf("Clean databases after test (%s)", cleanSQL),
	Run: func(_ *cobra.Command, _ []string) {
		chs, clean := util.GetMultiConnsForExec()
		defer clean()

		for i := util.DatabaseStart; i < util.DatabaseEnd; i++ {
			chs[i%util.Thread] <- fmt.Sprintf(cleanSQL, util.DatabaseNamePrefix, i)
		}

		fmt.Println("Finish clean tables")
	},
}

func init() {
	init_flags()

	rootCmd.AddCommand(cleanCmd)

	rootCmd.AddCommand(db.DbCmd)
	rootCmd.AddCommand(table.TableCmd)
	rootCmd.AddCommand(statistics.StatisticsCmd)
	rootCmd.AddCommand(index.IndexCmd)
	rootCmd.AddCommand(view.ViewCmd)
	rootCmd.AddCommand(fk.FkCmd)
	rootCmd.AddCommand(partition.PartitionCmd)
	rootCmd.AddCommand(check.CheckConstraintCmd)
	rootCmd.AddCommand(sequence.SequenceCmd)
	rootCmd.AddCommand(column.ColumnCmd)
}
