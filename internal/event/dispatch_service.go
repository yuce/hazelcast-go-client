/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License")
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package event

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/hazelcast/hazelcast-go-client/internal/logger"
)

const DefaultSubscriptionID = -1

type Event interface {
	EventName() string
}

type Handler func(event Event)

type controlType int

const (
	subscribe controlType = iota
	subscribeSync
	unsubscribe
)

const (
	created int32 = iota
	ready
	stopped
)

type DispatchService struct {
	logger            logger.Logger
	syncSubscriptions map[string]map[int64]Handler
	doneCh            chan struct{}
	subscriptions     map[string]map[int64]Handler
	state             int32
	eventMu           *sync.RWMutex
}

func NewDispatchService(logger logger.Logger) *DispatchService {
	service := &DispatchService{
		subscriptions:     map[string]map[int64]Handler{},
		syncSubscriptions: map[string]map[int64]Handler{},
		eventMu:           &sync.RWMutex{},
		state:             created,
		logger:            logger,
	}
	atomic.StoreInt32(&service.state, ready)
	return service
}

func (s *DispatchService) Stop() {
	// stopping a not-running service is no-op
	if !atomic.CompareAndSwapInt32(&s.state, ready, stopped) {
		return
	}
}

// Subscribe attaches handler to listen for events with eventName.
// Do not rely on the order of handlers, they may be shuffled.
func (s *DispatchService) Subscribe(eventName string, subscriptionID int64, handler Handler) {
	// subscribing to a not-runnning service is no-op
	if atomic.LoadInt32(&s.state) != ready {
		return
	}
	if subscriptionID == DefaultSubscriptionID {
		subscriptionID = MakeSubscriptionID(handler)
	}
	s.logger.Trace(func() string {
		return fmt.Sprintf("event.DispatchService.Subscribe: %s, %d, %v", eventName, subscriptionID, handler)
	})
	s.eventMu.Lock()
	s.subscribe(eventName, subscriptionID, handler)
	s.eventMu.Unlock()
}

// SubscribeSync attaches handler to listen for events with eventName.
// Sync handlers are dispatched first, the events are ordered.
// Do not rely on the order of handlers, they may be shuffled.
func (s *DispatchService) SubscribeSync(eventName string, subscriptionID int64, handler Handler) {
	// subscribing to a not-runnning service is no-op
	if atomic.LoadInt32(&s.state) != ready {
		return
	}
	if subscriptionID == DefaultSubscriptionID {
		subscriptionID = MakeSubscriptionID(handler)
	}
	s.logger.Trace(func() string {
		return fmt.Sprintf("event.DispatchService.SubscribeSync: %s, %d, %v", eventName, subscriptionID, handler)
	})
	s.eventMu.Lock()
	s.subscribeSync(eventName, subscriptionID, handler)
	s.eventMu.Unlock()
}

func (s *DispatchService) Unsubscribe(eventName string, subscriptionID int64) {
	// unsubscribing from a not-runnning service is no-op
	if atomic.LoadInt32(&s.state) != ready {
		return
	}
	s.logger.Trace(func() string {
		return fmt.Sprintf("event.DispatchService.Unsubscribe: %s, %d", eventName, subscriptionID)
	})
	s.eventMu.Lock()
	s.unsubscribe(eventName, subscriptionID)
	s.eventMu.Unlock()
}

func (s *DispatchService) Publish(event Event) {
	// publishing to a not-runnning service is no-op
	if atomic.LoadInt32(&s.state) != ready {
		return
	}
	s.logger.Trace(func() string {
		return fmt.Sprintf("event.DispatchService.Publish: %s", event.EventName())
	})
	s.eventMu.RLock()
	s.dispatch(event)
	s.eventMu.RUnlock()
}

func (s *DispatchService) dispatch(event Event) {
	s.logger.Trace(func() string {
		return fmt.Sprintf("event.DispatchService.dispatch: %s", event.EventName())
	})
	// first dispatch sync handlers
	if handlers, ok := s.syncSubscriptions[event.EventName()]; ok {
		for _, handler := range handlers {
			handler(event)
		}
	}
	// then dispatch async handlers
	if handlers, ok := s.subscriptions[event.EventName()]; ok {
		for _, handler := range handlers {
			go handler(event)
		}
	}
}

func (s *DispatchService) subscribe(eventName string, subscriptionID int64, handler Handler) {
	s.logger.Trace(func() string {
		return fmt.Sprintf("event.DispatchService.subscribe: %s, %d", eventName, subscriptionID)
	})
	subscriptionHandlers, ok := s.subscriptions[eventName]
	if !ok {
		subscriptionHandlers = map[int64]Handler{}
		s.subscriptions[eventName] = subscriptionHandlers
	}
	subscriptionHandlers[subscriptionID] = handler
}

func (s *DispatchService) subscribeSync(eventName string, subscriptionID int64, handler Handler) {
	s.logger.Trace(func() string {
		return fmt.Sprintf("event.DispatchService.subscribeSync: %s, %d", eventName, subscriptionID)
	})
	subscriptionHandlers, ok := s.syncSubscriptions[eventName]
	if !ok {
		subscriptionHandlers = map[int64]Handler{}
		s.syncSubscriptions[eventName] = subscriptionHandlers
	}
	subscriptionHandlers[subscriptionID] = handler
}

func (s *DispatchService) unsubscribe(eventName string, subscriptionID int64) {
	s.logger.Trace(func() string {
		return fmt.Sprintf("event.DispatchService.unsubscribe: %s, %d", eventName, subscriptionID)
	})
	if handlers, ok := s.syncSubscriptions[eventName]; ok {
		for sid := range handlers {
			if sid == subscriptionID {
				delete(handlers, sid)
				return
			}
		}
	}
	if handlers, ok := s.subscriptions[eventName]; ok {
		for sid := range handlers {
			if sid == subscriptionID {
				delete(handlers, sid)
				break
			}
		}
	}
}
