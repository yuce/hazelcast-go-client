package sql

type ColumnMetadata struct {
	name       string
	columnType ColumnType
	nullable   bool
}

func NewColumnMetadata(name string, columnType int32, isNullableExists bool, nullable bool) ColumnMetadata {
	return ColumnMetadata{name: name, columnType: ColumnType(columnType), nullable: nullable}
}

func (s *ColumnMetadata) Nullable() bool {
	return s.nullable
}

func (s *ColumnMetadata) SetNullable(nullable bool) {
	s.nullable = nullable
}

func (s *ColumnMetadata) Type() ColumnType {
	return s.columnType
}

func (s *ColumnMetadata) SetColumnType(columnType ColumnType) {
	s.columnType = columnType
}

func (s *ColumnMetadata) Name() string {
	return s.name
}

func (s *ColumnMetadata) SetName(name string) {
	s.name = name
}
