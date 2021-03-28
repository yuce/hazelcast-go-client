package sql

import "github.com/hazelcast/hazelcast-go-client/v4/internal/serialization"

type DataHolder interface {
	getRowCount() int
	getColumnValueForClient(columnIndex int, rowIndex int) interface{}
	getColumnValuesForServer(columnIndex int, columnType ColumnType) []interface{}
}

type Page struct {
	columnTypes []ColumnType
	last bool
	data DataHolder
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
func (p *Page) ColumnValuesForServer(columnIndex int) []interface{}{
	if columnIndex >= p.ColumnCount() {
		panic("No such index")
	}
	return p.data.getColumnValuesForServer(columnIndex, p.columnTypes[columnIndex])
}

func NewPageFromColumns(columnTypes []ColumnType, columns [][]serialization.Data, isLast bool) Page {
	return Page{

	}
}