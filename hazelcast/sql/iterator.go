package sql

import (
	"encoding/binary"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/serialization"
	"math"
)

type Iterator struct {
	serializationService serialization.Service
	rowMetadata          *SqlRowMetadata
	currentPage          Page
	currentRowCount      int
	currentPosition      int
	last                 bool
}

func (i *Iterator) HasNext() bool {
	for i.currentPosition == i.currentRowCount {
		// Reached end of the page. Try fetching the next one if possible.
		if !i.last {
			// todo fetch next page
		} else {
			return false
		}
	}
	return true
}

func (i *Iterator) Next() SqlRow {
	if !i.HasNext() {
		panic("No such element")
	}
	row := i.currentRow()
	i.currentPosition++
	return NewSqlRow(i.rowMetadata, row)
}

func DecodeInt(buffer []byte, offset int32) int32 {
	return int32(binary.LittleEndian.Uint32(buffer[offset:]))
}

func DecodeShortInt(buffer []byte, offset int32) int16 {
	return int16(binary.LittleEndian.Uint16(buffer[offset:]))
}

func DecodeFloat(buffer []byte, offset int32) float32 {
	return math.Float32frombits(uint32(DecodeInt(buffer, offset)))
}

func DecodeDouble(buffer []byte, offset int32) float64 {
	return math.Float64frombits(uint64(DecodeLong(buffer, offset)))
}
func DecodeLong(buffer []byte, offset int32) int64 {
	return int64(binary.LittleEndian.Uint64(buffer[offset:]))
}

func DecodeBoolean(buffer []byte, offset int32) bool {
	return buffer[offset] == 1
}

func deserializeRowData(value serialization.Data, columnType ColumnType, service serialization.Service) interface{} {
	var resValue interface{}
	if value == nil {
		return ""
	}
	switch columnType {
	case VARCHAR:
		resValue = string(value.Buffer())
	case BOOLEAN:
		resValue = DecodeBoolean(value.Buffer(), 0)
	case TINYINT:
		resValue = int8(value.Buffer()[0])
	case SMALLINT:
		resValue = DecodeShortInt(value.Buffer(), 0)

	case INTEGER:
		resValue = DecodeInt(value.Buffer(), 0)

	case BIGINT:
		resValue = DecodeLong(value.Buffer(), 0)

	case REAL:
		resValue = DecodeFloat(value.Buffer(), 0)

	case DOUBLE:
		resValue = DecodeDouble(value.Buffer(), 0)

	case DATE:
		// todo
	case TIME:
		// todo

	case TIMESTAMP:
		// todo

	case TIMESTAMP_WITH_TIME_ZONE:
		// todo

	case DECIMAL:
		// todo

	case NULL:
		resValue = nil

	case OBJECT:

		s, err := service.ToObject(value)
		if err == nil {
			resValue = s
		} else {
			resValue = nil
		}

	default:
		panic("Unknown type " + columnType.String())
	}
	return resValue
}

func (i *Iterator) currentRow() rowImpl {
	values := make([]interface{}, i.rowMetadata.ColumnCount())

	for j := 0; j < i.currentPage.ColumnCount(); j++ {
		value := i.currentPage.ColumnValuesForClient(j, i.currentPosition)

		values[j] = deserializeRowData(value, i.rowMetadata.Column(j).columnType, i.serializationService)
	}

	return NewDataRow(values)
}

func (i *Iterator) onNextPage(page Page) {
	i.currentPage = page
	i.currentPosition = 0
	i.currentRowCount = page.RowCount()

	if page.last {
		i.last = true
	}

}

func NewIterator(rowMetadata *SqlRowMetadata, page Page, serializationService serialization.Service) SqlRowIterator {
	if rowMetadata == nil {
		panic("Null row metadata")
	}

	iterator := &Iterator{
		rowMetadata:          rowMetadata,
		serializationService: serializationService,
	}

	iterator.onNextPage(page)

	return iterator
}

func NewEmptyIterator() SqlRowIterator {
	return &Iterator{
		last: true,
	}
}
