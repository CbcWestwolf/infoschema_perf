package cmd

import (
	"fmt"
	"infoschema_perf/cmd/db"
	"infoschema_perf/cmd/index"
	"infoschema_perf/cmd/statistics"
	"infoschema_perf/cmd/table"
	"infoschema_perf/cmd/util"
	"infoschema_perf/cmd/view"

	"github.com/spf13/cobra"
)

var (
	rootCmd = &cobra.Command{Use: "infoschema_perf", Short: "infoschema_perf is a tool to test the performance of information_schema queries"}
)

// Execute executes the root command.
func Execute() error {
	print_flags()
	return rootCmd.Execute()
}

func init_flags() {
	rootCmd.PersistentFlags().StringVar(&util.Host, "host", "127.0.0.1", "The host of the database")
	rootCmd.PersistentFlags().IntVar(&util.Port, "port", 4000, "The port of the database")
	rootCmd.PersistentFlags().StringVar(&util.User, "user", "root", "The user of the database")
	rootCmd.PersistentFlags().IntVar(&util.Thread, "thread", 4, "The number of threads to use")
	rootCmd.PersistentFlags().StringVar(&util.TimeStr, "time", "60s", "The duration of the test. Set to \"\" means forever")
	rootCmd.PersistentFlags().BoolVar(&util.Stdout, "stdout", false, "Whether to print the result to stdout")
}

func print_flags() {
	fmt.Printf("Host: %s, Port: %d, User: %s, Thread: %d, Time: %s\n", util.Host, util.Port, util.User, util.Thread, util.TimeStr)
}

func init() {
	init_flags()

	rootCmd.AddCommand(db.DbCmd)
	rootCmd.AddCommand(table.TableCmd)
	rootCmd.AddCommand(statistics.StatisticsCmd)
	rootCmd.AddCommand(index.IndexCmd)
	rootCmd.AddCommand(view.ViewCmd)
}
