package sql

type SqlRow interface {
	ValueAtIndex(index int) interface{}
	ValueWithName(name string) interface{}
	Metadata() *SqlRowMetadata
}

type sqlRowImpl struct {
	rowMetadata *SqlRowMetadata
	row         rowImpl
}

func NewSqlRow(rowMetadata *SqlRowMetadata, row rowImpl) SqlRow {
	return &sqlRowImpl{
		rowMetadata: rowMetadata,
		row:         row,
	}
}

func (r *sqlRowImpl) ValueAtIndex(index int) interface{} {
	return r.row.ValueAtIndex(index)
}

func (r *sqlRowImpl) Metadata() *SqlRowMetadata {
	return r.rowMetadata
}

func (r *sqlRowImpl) ValueWithName(name string) interface{} {
	return nil
}
