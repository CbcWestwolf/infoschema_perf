package sequence

import (
	"fmt"
	"infoschema_perf/cmd/util"
	"math/rand"

	"github.com/spf13/cobra"
)

var (
	SequenceCmd = &cobra.Command{
		Use:   "sequence",
		Short: "Prepare, test and clean sequence for test",
	}

	prepareCmd = &cobra.Command{
		Use:   "prepare",
		Short: fmt.Sprintf("Prepare sequences for test (%s)", prepareSequenceSQL),
		Run:   prepare,
	}

	cleanCmd = &cobra.Command{
		Use:   "clean",
		Short: fmt.Sprintf("Clean sequences after test (%s)", util.CleanSQL),
		Run:   util.Clean,
	}

	queryCmds = []*cobra.Command{
		{
			Use:   "q1",
			Short: querySequenceSQL1,
			Run:   query1,
		},
	}
)

const (
	prepareDbSQL       = "CREATE DATABASE IF NOT EXISTS %s_%d"
	prepareSequenceSQL = "CREATE SEQUENCE IF NOT EXISTS %s.%s;"
	querySequenceSQL1  = "SELECT * FROM information_schema.sequences WHERE sequence_schema = '%s' AND sequence_name = '%s';"
)

func init_flags() {
	SequenceCmd.PersistentFlags().IntVar(&util.TableCnt, "sequence_cnt", 10, "The number of sequences to create")
	SequenceCmd.PersistentFlags().StringVar(&util.TableNamePrefix, "sequence_prefix", "t", "The prefix of the sequence name")
}

func init() {
	init_flags()

	SequenceCmd.AddCommand(prepareCmd, cleanCmd)
	SequenceCmd.AddCommand(queryCmds...)
}

func prepare(_ *cobra.Command, _ []string) {
	chs, clean := util.GetMultiConnsForExec()
	defer clean()

	for i := 0; i < util.DatabaseCnt; i++ {
		chs[i%util.Thread] <- fmt.Sprintf(prepareDbSQL, util.DatabaseNamePrefix, i)
		for j := 0; j < util.TableCnt; j++ {
			chs[i%util.Thread] <- fmt.Sprintf(prepareSequenceSQL, fmt.Sprintf("%s_%d", util.DatabaseNamePrefix, i),
				fmt.Sprintf("%s_%d", util.TableNamePrefix, j))
		}
	}

	fmt.Println("Finish prepare tables")
}

func query1(_ *cobra.Command, _ []string) {
	util.QuerySQL(func() string {
		return fmt.Sprintf(querySequenceSQL1, fmt.Sprintf("%s_%d", util.DatabaseNamePrefix, rand.Intn(util.DatabaseCnt)),
			fmt.Sprintf("%s_%d", util.TableNamePrefix, rand.Intn(util.TableCnt)))
	})
	fmt.Printf("Finish query '%s'", querySequenceSQL1)
}
