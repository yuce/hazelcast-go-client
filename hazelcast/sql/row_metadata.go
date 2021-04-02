package sql

type SqlRowMetadata struct {
	columns     []ColumnMetadata
	nameToIndex map[string]int
}

func (s *SqlRowMetadata) ColumnCount() int {
	return len(s.columns)
}

func (s *SqlRowMetadata) Column(index int) ColumnMetadata {
	if index >= s.ColumnCount() {
		panic("Index does not exists")
	}
	return s.columns[index]
}

func NewRowMetadata(columns []ColumnMetadata) *SqlRowMetadata {
	if columns == nil || len(columns) == 0 {
		panic("Columns empty")
	}

	nameToIndex := make(map[string]int)

	for i := 0; i < len(columns); i++ {
		nameToIndex[columns[i].Name()] = i
	}

	return &SqlRowMetadata{
		columns:     columns,
		nameToIndex: nameToIndex,
	}
}
