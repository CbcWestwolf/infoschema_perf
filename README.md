## build

```bash
go build
```

## Usage

```bash
> ./infoschema_perf
Host: 127.0.0.1, Port: 4000, User: root, Thread: 4, Time: 
infoschema_perf is a tool to test the performance of information_schema queries

Usage:
  infoschema_perf [command]

Available Commands:
  completion  Generate the autocompletion script for the specified shell
  db          Prepare, test and clean database for test
  help        Help about any command

Flags:
  -h, --help          help for infoschema_perf
      --host string   The host of the database (default "127.0.0.1")
      --port int      The port of the database (default 4000)
      --thread int    The number of threads to use (default 4)
      --time string   The duration of the test. "" means forever
      --user string   The user of the database (default "root")

Use "infoschema_perf [command] --help" for more information about a command.
```