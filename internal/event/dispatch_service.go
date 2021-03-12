package event

import (
	"fmt"
	"sync"
	"sync/atomic"
)

type Event interface {
	Name() string
}

type EventHandler func(event Event)

type controlType int

const (
	subscribe controlType = iota
	unsubscribe
)

type controlMessage struct {
	controlType    controlType
	eventName      string
	subscriptionID int
	handler        EventHandler
}

type DispatchService interface {
	Subscribe(eventName string, subscriptionID int, handler EventHandler)
	Unsubscribe(eventName string, subscriptionID int, handler EventHandler)
	Publish(event Event)
	Stop() // experimental
}

type DispatchServiceImpl struct {
	subscriptions map[string]map[int]EventHandler
	eventCh       chan Event
	controlCh     chan controlMessage
	doneCh        chan struct{}
	running       atomic.Value
}

func NewDispatchServiceImpl() *DispatchServiceImpl {
	service := &DispatchServiceImpl{
		subscriptions: map[string]map[int]EventHandler{},
		eventCh:       make(chan Event, 1),
		controlCh:     make(chan controlMessage, 1),
		doneCh:        make(chan struct{}, 1),
	}
	service.running.Store(false)
	service.start()
	return service
}

// Subscribe attaches handler to listen for events with eventName.
// Do not rely on the order of handlers, they may be shuffled.
func (s *DispatchServiceImpl) Subscribe(eventName string, subscriptionID int, handler EventHandler) {
	// subscribing to a not-runnning service is no-op
	if s.running.Load() != true {
		return
	}
	s.controlCh <- controlMessage{
		controlType:    subscribe,
		eventName:      eventName,
		subscriptionID: subscriptionID,
		handler:        handler,
	}
}

func (s *DispatchServiceImpl) Unsubscribe(eventName string, subscriptionID int, handler EventHandler) {
	// unsubscribing from a not-runnning service is no-op
	if s.running.Load() != true {
		return
	}
	s.controlCh <- controlMessage{
		// TODO: rename controlType
		controlType:    unsubscribe,
		eventName:      eventName,
		subscriptionID: subscriptionID,
		handler:        handler,
	}
}

func (s *DispatchServiceImpl) Publish(event Event) {
	// publishing to a not-runnning service is no-op
	if s.running.Load() != true || event == nil {
		return
	}
	s.eventCh <- event
}

func (s *DispatchServiceImpl) start() {
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		wg.Done()
		for {
			select {
			case event := <-s.eventCh:
				s.dispatch(event)
			case control := <-s.controlCh:
				switch control.controlType {
				case subscribe:
					s.subscribe(control.eventName, control.subscriptionID, control.handler)
				case unsubscribe:
					s.unsubscribe(control.eventName, control.subscriptionID, control.handler)
				default:
					panic(fmt.Sprintf("unknown control type: %d", control.controlType))
				}
			case <-s.doneCh:
				return
			}
		}
	}()
	wg.Wait()
	s.running.Store(true)
}

func (s *DispatchServiceImpl) Stop() {
	// stopping a not-running service is no-op
	if s.running.Load() != true {
		return
	}
	s.running.Store(false)
	s.doneCh <- struct{}{}
	close(s.doneCh)
	close(s.eventCh)
	close(s.controlCh)
}

func (s *DispatchServiceImpl) dispatch(event Event) {
	if handlers, ok := s.subscriptions[event.Name()]; ok {
		for _, handler := range handlers {
			go handler(event)
		}
	}
}

func (s *DispatchServiceImpl) subscribe(eventName string, subscriptionID int, handler EventHandler) {
	subscriptionHandlers, ok := s.subscriptions[eventName]
	if !ok {
		subscriptionHandlers = map[int]EventHandler{}
		s.subscriptions[eventName] = subscriptionHandlers
	}
	subscriptionHandlers[subscriptionID] = handler
}

func (s *DispatchServiceImpl) unsubscribe(eventName string, unsubscribeSubscriptionID int, unsubscribedHandler EventHandler) {
	if handlers, ok := s.subscriptions[eventName]; ok {
		for subscriptionID, _ := range handlers {
			if subscriptionID == unsubscribeSubscriptionID {
				delete(handlers, subscriptionID)
				break
			}
		}
	}
}
