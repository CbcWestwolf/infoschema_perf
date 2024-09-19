```text
./infoschema_perf db --help 

Host: 127.0.0.1, Port: 4000, User: root, Thread: 4, Time: 
Prepare, test and clean database for test

Usage:
  infoschema_perf db [command]

Available Commands:
  clean       Clean databases after test (DROP DATABASE IF EXISTS %s_%d)
  prepare     Prepare databases for test (CREATE DATABASE IF NOT EXISTS %s_%d)
  schemata_q1 SELECT * FROM information_schema.schemata WHERE schema_name = '%s_%d'
  schemata_q2 SELECT * FROM information_schema.schemata WHERE schema_name LIKE '%s%%';

Flags:
      --db_cnt int         The number of databases to create (default 1)
      --db_prefix string   The prefix of the database name (default "info_test")
  -h, --help               help for db

Global Flags:
      --host string   The host of the database (default "127.0.0.1")
      --port int      The port of the database (default 4000)
      --thread int    The number of threads to use (default 4)
      --time string   The duration of the test. "" means forever
      --user string   The user of the database (default "root")

Use "infoschema_perf db [command] --help" for more information about a command.
```