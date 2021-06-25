package invocation

const EventInvocationLost = "internal.invocation.lost"

type InvocationLostHandler = func(event *InvocationLost)

type InvocationLost struct {
	CorrelationIDs []int64
}

func NewInvocationLost(correlationIDs []int64) *InvocationLost {
	return &InvocationLost{CorrelationIDs: correlationIDs}
}

func (InvocationLost) EventName() string {
	return EventInvocationLost
}
