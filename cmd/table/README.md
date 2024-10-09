```text
./infoschema_perf table --help

Host: 127.0.0.1, Port: 4000, User: root, Thread: 4, Time: 
Prepare, test and clean table for test

Usage:
  infoschema_perf table [command]

Available Commands:
  clean       Clean tables after test (DROP DATABASE IF EXISTS %s_%d)
  prepare     Prepare tables for test (CREATE TABLE IF NOT EXISTS %s.%s (id int primary key, name varchar(255));)
  table_q1    SELECT * FROM information_schema.tables WHERE TABLE_SCHEMA NOT IN (%s) limit 10000;
  table_q2    SELECT * FROM information_schema.tables WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s';

Flags:
      --db_cnt int            The number of databases to create (default 1)
      --db_prefix string      The prefix of the database name (default "info_test")
  -h, --help                  help for table
      --table_cnt int         The number of tables to create (default 10)
      --table_prefix string   The prefix of the table name (default "t")

Global Flags:
      --host string   The host of the database (default "127.0.0.1")
      --port int      The port of the database (default 4000)
      --thread int    The number of threads to use (default 4)
      --time string   The duration of the test. "" means forever
      --user string   The user of the database (default "root")

Use "infoschema_perf table [command] --help" for more information about a command.
```