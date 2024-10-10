package util

import "time"

var (
	Host   string
	Port   int
	User   string
	Thread int

	DatabaseCnt        int
	DatabaseNamePrefix string

	TableCnt        int
	TableNamePrefix string

	ColumnCnt        int
	ColumnNamePrefix string

	Stdout  bool
	TimeStr string
)

const (
	Tick = 10 * time.Millisecond
)
