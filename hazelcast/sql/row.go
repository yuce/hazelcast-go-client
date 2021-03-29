package sql

type row interface {
	columnCount() int
}

type rowImpl struct {
	values []interface{}
}

func (r *rowImpl) columnCount() int {
	return len(r.values)
}

func (r *rowImpl) ValueAtIndex(index int) interface{} {
	return r.values[index]
}

func NewDataRow(values []interface{}) rowImpl {
	return rowImpl{values: values}
}
