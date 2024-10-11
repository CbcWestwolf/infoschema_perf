## build

```bash
go build
```

## Usage

```bash
> ./infoschema_perf
infoschema_perf is a tool to test the performance of information_schema queries

Usage:
  infoschema_perf [command]

Available Commands:
  check       Prepare and test check constraints
  clean       Clean databases after test (DROP DATABASE IF EXISTS %s_%d)
  column      Prepare and test column
  completion  Generate the autocompletion script for the specified shell
  db          Prepare and test database
  fk          Prepare and test fk
  help        Help about any command
  index       Prepare and test indexes
  partition   Prepare and test partition
  sequence    Prepare and test sequence
  statistics  Prepare and test table
  table       Prepare and test table
  view        Prepare and test view

Flags:
      --db_cnt int         The number of databases to create (default 3)
      --db_prefix string   The prefix of the database name (default "info_test")
  -h, --help               help for infoschema_perf
      --host string        The host of the database (default "127.0.0.1")
      --port int           The port of the database (default 4000)
      --stdout             Whether to print the result to stdout
      --thread int         The number of threads to use (default 4)
      --time string        The duration of the test. (default "60s")
      --user string        The user of the database (default "root")

Use "infoschema_perf [command] --help" for more information about a command.
```