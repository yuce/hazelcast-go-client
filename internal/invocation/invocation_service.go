package invocation

import (
	"fmt"
	"github.com/hazelcast/hazelcast-go-client/v4/hazelcast/logger"
	ihzerror "github.com/hazelcast/hazelcast-go-client/v4/internal/hzerror"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/proto/bufutil"
	"os"
	"sync/atomic"
	"time"
)

type ServiceCreationBundle struct {
	Handler      Handler
	RequestCh    <-chan Invocation
	ResponseCh   <-chan *proto.ClientMessage
	SmartRouting bool
	Logger       logger.Logger
}

func (b ServiceCreationBundle) Check() {
	// Handler can be nil
	if b.Logger == nil {
		panic("Logger is nil")
	}
	if b.RequestCh == nil {
		panic("RequestCh is nil")
	}
	if b.ResponseCh == nil {
		panic("ResponseCh is nil")
	}
}

type Service interface {
	// SetHandler should be called only before client is started
	SetHandler(handler Handler)
	// TODO: make HandleResponse private
	//shutdown()
}

type ServiceImpl struct {
	nextCorrelationID int64
	requestCh         <-chan Invocation
	responseCh        <-chan *proto.ClientMessage
	invocations       map[int64]Invocation
	invocationTimeout time.Duration
	retryPause        time.Duration
	shutDown          atomic.Value
	smartRouting      bool
	handler           Handler
	logger            logger.Logger
}

func NewServiceImpl(bundle ServiceCreationBundle) *ServiceImpl {
	bundle.Check()
	handler := bundle.Handler
	if handler == nil {
		handler = &DefaultHandler{}
	}
	service := &ServiceImpl{
		requestCh:         bundle.RequestCh,
		responseCh:        bundle.ResponseCh,
		invocations:       map[int64]Invocation{},
		invocationTimeout: 120 * time.Second,
		retryPause:        1 * time.Second,
		smartRouting:      bundle.SmartRouting,
		handler:           bundle.Handler,
		logger:            bundle.Logger,
	}
	service.shutDown.Store(false)
	go service.processIncoming()
	return service
}

func (s *ServiceImpl) SetHandler(handler Handler) {
	s.handler = handler
}

func (s *ServiceImpl) processIncoming() {
	requestCount := 0
	responseCount := 0
	for {
		select {
		case inv := <-s.requestCh:
			requestCount++
			fmt.Fprintln(os.Stderr, "req:", requestCount, "resp:", responseCount)
			s.sendInvocation(inv)
		case msg := <-s.responseCh:
			responseCount++
			fmt.Fprintln(os.Stderr, "processIncoming:", msg)
			fmt.Fprintln(os.Stderr, "req:", requestCount, "resp:", responseCount)
			s.handleClientMessage(msg)
		}
	}
}

func (s *ServiceImpl) sendInvocation(invocation Invocation) Result {
	//if s.shutDown.Load() == true {
	//	invocation.CompleteWithErr(core.NewHazelcastClientNotActiveError("client is shut down", nil))
	//}
	s.registerInvocation(invocation)
	if err := s.handler.Invoke(invocation); err != nil {
		s.handleError(invocation.Request().CorrelationID(), err)
	}
	return invocation
}

func (s *ServiceImpl) handleClientMessage(msg *proto.ClientMessage) {
	if msg.Err != nil {
		s.logger.Error(msg.Err)
		if msg.StartFrame != nil {
			s.handleError(msg.CorrelationID(), msg.Err)
		} else {

		}
		return
	}
	correlationID := msg.CorrelationID()
	if msg.StartFrame.HasEventFlag() || msg.StartFrame.HasBackupEventFlag() {
		if inv, found := s.invocations[correlationID]; !found {
			s.logger.Trace("invocation with unknown correlation id: ", correlationID)
		} else if inv.EventHandler() != nil {
			go inv.EventHandler()(msg)
		}
		return
	}
	if invocation := s.unregisterInvocation(correlationID); invocation != nil {
		if msg.GetMessageType() == int32(bufutil.MessageTypeException) {
			err := ihzerror.CreateHazelcastError(msg.DecodeError())
			s.handleError(correlationID, err)
		} else {
			invocation.Complete(msg)
		}
	} else {
		s.logger.Trace("no invocation found with the correlation id: ", correlationID)
	}
}

func (s *ServiceImpl) handleError(correlationID int64, invocationErr error) {
	//correlationID := invocation.Request().CorrelationID()
	if inv := s.unregisterInvocation(correlationID); inv != nil {
		s.logger.Error(invocationErr)
		inv.Complete(&proto.ClientMessage{Err: invocationErr})
		//panic("handleError: implement me!")
	} else {
		s.logger.Trace("no invocation found with correlation id: ", correlationID)
	}
}

func (s *ServiceImpl) registerInvocation(invocation Invocation) {
	message := invocation.Request()
	if message == nil {
		panic("message loaded from invocation request is nil")
	}
	correlationID := s.nextCorrelationID
	s.nextCorrelationID++
	message.SetCorrelationID(correlationID)
	message.SetPartitionId(invocation.PartitionID())
	s.invocations[correlationID] = invocation
}

func (s *ServiceImpl) unregisterInvocation(correlationID int64) Invocation {
	if invocation, ok := s.invocations[correlationID]; ok {
		delete(s.invocations, correlationID)
		return invocation
	}
	return nil
}