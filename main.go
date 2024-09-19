package main

import (
	"infoschema_perf/cmd"

	_ "github.com/go-sql-driver/mysql"
)

func main() {
	cmd.Execute()
}
