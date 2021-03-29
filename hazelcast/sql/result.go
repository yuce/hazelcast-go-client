package sql

import (
	"github.com/hazelcast/hazelcast-go-client/v4/internal/serialization"
)

type SqlRowIterator interface {
	Next() SqlRow
	HasNext() bool
}

type Result interface {
	Rows() SqlRowIterator
}

type resultImpl struct {
	iterator SqlRowIterator
}

func (r *resultImpl) Rows() SqlRowIterator {
	return r.iterator
}

func NewResult(page Page, rowMetadata *SqlRowMetadata, updateCount int64, serializationService serialization.Service) Result {
	if rowMetadata == nil {
		return &resultImpl{iterator: NewEmptyIterator()}
	}

	iterator := NewIterator(rowMetadata, page, serializationService)
	return &resultImpl{iterator: iterator}
}
