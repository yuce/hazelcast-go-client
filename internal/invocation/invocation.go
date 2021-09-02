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

package invocation

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	proto2 "github.com/hazelcast/hazelcast-go-client/proto"

	pubcluster "github.com/hazelcast/hazelcast-go-client/cluster"
	"github.com/hazelcast/hazelcast-go-client/hzerrors"
	"github.com/hazelcast/hazelcast-go-client/internal/cb"
	ihzerrors "github.com/hazelcast/hazelcast-go-client/internal/hzerrors"
)

var ErrResponseChannelClosed = errors.New("response channel closed")

type Result interface {
	Get() (*proto2.ClientMessage, error)
	GetWithTimeout(duration time.Duration) (*proto2.ClientMessage, error)
}

type Invocation interface {
	Complete(message *proto2.ClientMessage)
	Completed() bool
	EventHandler() proto2.ClientMessageHandler
	Get() (*proto2.ClientMessage, error)
	GetWithContext(ctx context.Context) (*proto2.ClientMessage, error)
	PartitionID() int32
	Request() *proto2.ClientMessage
	Address() pubcluster.Address
	Close()
	CanRetry(err error) bool
}

type Impl struct {
	deadline      time.Time
	response      chan *proto2.ClientMessage
	eventHandler  func(clientMessage *proto2.ClientMessage)
	request       *proto2.ClientMessage
	address       pubcluster.Address
	completed     int32
	partitionID   int32
	RedoOperation bool
}

func NewImpl(clientMessage *proto2.ClientMessage, partitionID int32, address pubcluster.Address, deadline time.Time, redoOperation bool) *Impl {
	return &Impl{
		partitionID:   partitionID,
		address:       address,
		request:       clientMessage,
		response:      make(chan *proto2.ClientMessage, 1),
		deadline:      deadline,
		RedoOperation: redoOperation,
	}
}

func (i *Impl) Complete(message *proto2.ClientMessage) {
	if atomic.CompareAndSwapInt32(&i.completed, 0, 1) {
		i.response <- message
	}
}

func (i *Impl) Completed() bool {
	return i.completed != 0
}

func (i *Impl) EventHandler() proto2.ClientMessageHandler {
	return i.eventHandler
}

func (i *Impl) Get() (*proto2.ClientMessage, error) {
	ctx, cancel := context.WithDeadline(context.Background(), i.deadline)
	msg, err := i.GetWithContext(ctx)
	cancel()
	return msg, err
}

func (i *Impl) GetWithContext(ctx context.Context) (*proto2.ClientMessage, error) {
	select {
	case response, ok := <-i.response:
		if ok {
			return i.unwrapResponse(response)
		}
		return nil, cb.WrapNonRetryableError(ErrResponseChannelClosed)
	case <-ctx.Done():
		err := ctx.Err()
		if err != nil && !i.CanRetry(err) {
			err = cb.WrapNonRetryableError(err)
		}
		return nil, err
	}
}

func (i *Impl) PartitionID() int32 {
	return i.partitionID
}

func (i *Impl) Request() *proto2.ClientMessage {
	return i.request
}

func (i *Impl) Address() pubcluster.Address {
	return i.address
}

/*
func (i *Proxy) StoreSentConnection(conn interface{}) {
	i.sentConnection.Store(conn)
}
*/

// SetEventHandler sets the event handler for the invocation.
// It should only be called at the site of creation.
func (i *Impl) SetEventHandler(handler proto2.ClientMessageHandler) {
	i.eventHandler = handler
}

func (i *Impl) Close() {
	close(i.response)
}

func (i *Impl) CanRetry(err error) bool {
	var nonRetryableError *cb.NonRetryableError
	if errors.As(err, &nonRetryableError) {
		return false
	}
	return i.MaybeCanRetry(err)
}

func (i *Impl) MaybeCanRetry(err error) bool {
	if errors.Is(err, hzerrors.ErrIO) || errors.Is(err, hzerrors.ErrHazelcastInstanceNotActive) {
		return true
	}
	// check whether the error is retryable
	if ihzerrors.IsRetryable(err) {
		return true
	}
	if errors.Is(err, hzerrors.ErrTargetDisconnected) {
		return i.Request().Retryable || i.RedoOperation
	}
	return false
}

func (i *Impl) unwrapResponse(response *proto2.ClientMessage) (*proto2.ClientMessage, error) {
	if response.Err != nil {
		if i.CanRetry(response.Err) {
			return nil, response.Err
		}
		return nil, cb.WrapNonRetryableError(response.Err)
	}
	return response, nil
}
