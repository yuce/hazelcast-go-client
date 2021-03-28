package sql

import "fmt"

type ColumnType int32

const (
	VARCHAR ColumnType = iota
	BOOLEAN
	TINYINT
	SMALLINT
	INTEGER
	BIGINT
	DECIMAL
	REAL
	DOUBLE
	DATE
	TIME
	TIMESTAMP
	TIMESTAMP_WITH_TIME_ZONE
	OBJECT
	NULL
)

func (c *ColumnType) String() string {
	return fmt.Sprintf("%d", c)
}
