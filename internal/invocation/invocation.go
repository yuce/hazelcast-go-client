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
	"fmt"
	"sync/atomic"
	"time"

	pubcluster "github.com/hazelcast/hazelcast-go-client/cluster"
	"github.com/hazelcast/hazelcast-go-client/hzerrors"
	"github.com/hazelcast/hazelcast-go-client/internal/cb"
	ihzerrors "github.com/hazelcast/hazelcast-go-client/internal/hzerrors"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
)

var ErrResponseChannelClosed = errors.New("response channel closed")

const (
	fresh     = 0
	completed = 1
)

type Result interface {
	Get() (*proto.ClientMessage, error)
	GetWithTimeout(duration time.Duration) (*proto.ClientMessage, error)
}

type Invocation interface {
	Complete(message *proto.ClientMessage)
	Completed() bool
	EventHandler() proto.ClientMessageHandler
	GetWithContext(ctx context.Context) (*proto.ClientMessage, error)
	PartitionID() int32
	Request() *proto.ClientMessage
	Address() pubcluster.Address
	Close()
	CanRetry(err error) bool
}

type Impl struct {
	timeout       time.Duration
	response      chan *proto.ClientMessage
	eventHandler  func(clientMessage *proto.ClientMessage)
	request       *proto.ClientMessage
	address       pubcluster.Address
	completed     int32
	partitionID   int32
	RedoOperation bool
}

func NewImpl(clientMessage *proto.ClientMessage, partitionID int32, address pubcluster.Address, timeout time.Duration, redoOperation bool) *Impl {
	return &Impl{
		partitionID:   partitionID,
		address:       address,
		request:       clientMessage,
		response:      make(chan *proto.ClientMessage, 1),
		timeout:       timeout,
		RedoOperation: redoOperation,
	}
}

func (i *Impl) Complete(message *proto.ClientMessage) {
	if atomic.CompareAndSwapInt32(&i.completed, fresh, completed) {
		i.response <- message
	}
}

func (i *Impl) Completed() bool {
	return i.completed == completed
}

func (i *Impl) EventHandler() proto.ClientMessageHandler {
	return i.eventHandler
}

func (i *Impl) GetWithContext(ctx context.Context) (*proto.ClientMessage, error) {
	select {
	case response, ok := <-i.response:
		if ok {
			return i.unwrapResponse(response)
		}
		return nil, cb.WrapNonRetryableError(ErrResponseChannelClosed)
	case <-ctx.Done():
		err := ctx.Err()
		if err != nil && !i.CanRetry(err) {
			i.Close()
			err = cb.WrapNonRetryableError(err)
		}
		return nil, err
	case <-time.After(i.timeout):
		i.Close()
		return nil, fmt.Errorf("invocation: %w", hzerrors.ErrOperationTimeout)
	}
}

func (i *Impl) PartitionID() int32 {
	return i.partitionID
}

func (i *Impl) Request() *proto.ClientMessage {
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
func (i *Impl) SetEventHandler(handler proto.ClientMessageHandler) {
	i.eventHandler = handler
}

func (i *Impl) Close() {
	if atomic.CompareAndSwapInt32(&i.completed, fresh, completed) {
		close(i.response)
	}
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

func (i *Impl) unwrapResponse(response *proto.ClientMessage) (*proto.ClientMessage, error) {
	if response.Err != nil {
		if i.CanRetry(response.Err) {
			return nil, response.Err
		}
		return nil, cb.WrapNonRetryableError(response.Err)
	}
	return response, nil
}
