package sql

import (
	"fmt"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/serialization"
)

type DataHolder interface {
	getRowCount() int
	getColumnValueForClient(columnIndex int, rowIndex int) interface{}
	getColumnValuesForServer(columnIndex int, columnType ColumnType) []interface{}
}

type Page struct {
	columnTypes []ColumnType
	last        bool
	data        DataHolder
}

func NewPageFromColumns(columnTypes []ColumnType, columns [][]serialization.Data, last bool) Page {
	return Page{
		columnTypes: columnTypes,
		last:        last,
		data:        &ColumnarDataHolder{
			columns: columns,
		},
	}
}

func (p *Page) ColumnCount() int {
	return len(p.columnTypes)
}

func (p *Page) ColumnTypes() []ColumnType {
	return p.columnTypes
}

func (p *Page) Last() bool {
	return p.last
}
func (p *Page) ColumnValuesForServer(columnIndex int) []interface{} {
	if columnIndex >= p.ColumnCount() {
		panic("No such index")
	}
	return p.data.getColumnValuesForServer(columnIndex, p.columnTypes[columnIndex])
}

type ColumnarDataHolder struct {
	columns [][]serialization.Data
}

func (c *ColumnarDataHolder) getRowCount() int {
	return len(c.columns[0])
}
func (c *ColumnarDataHolder) getColumnValueForClient(columnIndex int, rowIndex int) interface{} {
	return c.columns[columnIndex][rowIndex]
}
func (c *ColumnarDataHolder) getColumnValuesForServer(columnIndex int, columnType ColumnType) []interface{} {
	if columnType == OBJECT {
		panic("getColumnValuesForServer error")
	}
	res := make([]interface{}, 0)

	rows := c.columns[columnIndex]
	for _,v := range rows {
		fmt.Println(string(v.Buffer()))
		res = append(res, v)
	}

	return res
}
