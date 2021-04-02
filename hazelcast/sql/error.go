package sql

import (
	"github.com/hazelcast/hazelcast-go-client/v4/internal"
)

type Error struct {
	code                int32
	message             string
	originatingMemberId internal.UUID
}

func (s *Error) Error() string {
	return s.message
}

func NewError(code int32, message string, originatingMemberId internal.UUID) Error {
	return Error{code: code, message: message, originatingMemberId: originatingMemberId}
}

func (s *Error) OriginatingMemberId() internal.UUID {
	return s.originatingMemberId
}

func (s *Error) SetOriginatingMemberId(originatingMemberId internal.UUID) {
	s.originatingMemberId = originatingMemberId
}

func (s *Error) Message() string {
	return s.message
}

func (s *Error) SetMessage(message string) {
	s.message = message
}

func (s *Error) Code() int32 {
	return s.code
}

func (s *Error) SetCode(code int32) {
	s.code = code
}
