package util

var (
	Host   string
	Port   int
	User   string
	Thread int

	DatabaseStart      int
	DatabaseEnd        int
	DatabaseNamePrefix string

	TableCnt        int
	TableNamePrefix string
	ViewNamePrefix  string

	ColumnCnt        int
	ColumnNamePrefix string
	ConstraintCnt    int

	IndexCnt        int
	IndexNamePrefix string
	UniqueCnt       int

	Stdout  bool
	TimeStr string
)
