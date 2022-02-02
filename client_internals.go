//go:build hazelcastinternal
// +build hazelcastinternal

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

package hazelcast

import (
	"context"

	"github.com/hazelcast/hazelcast-go-client/internal/cluster"
	"github.com/hazelcast/hazelcast-go-client/internal/event"
	"github.com/hazelcast/hazelcast-go-client/internal/invocation"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/internal/serialization"
	"github.com/hazelcast/hazelcast-go-client/types"
)

const PartitionIDOffset = proto.PartitionIDOffset
const IntSizeInBytes = proto.IntSizeInBytes
const UnfragmentedMessage = proto.UnfragmentedMessage

type Data = serialization.Data
type ClientMessage = proto.ClientMessage
type ClientMessageHandler = proto.ClientMessageHandler
type Frame = proto.Frame
type ForwardFrameIterator = proto.ForwardFrameIterator

func NewClientMessageForEncode() *ClientMessage {
	return proto.NewClientMessageForEncode()
}

func NewFrameWith(content []byte, flags uint16) Frame {
	return proto.NewFrameWith(content, flags)
}

type ClientInternal struct {
	client *Client
	proxy  *proxy
}

func NewClientInternal(c *Client) *ClientInternal {
	return &ClientInternal{
		client: c,
		proxy:  c.proxyManager.invocationProxy,
	}
}

func (ci *ClientInternal) ConnectionManager() *cluster.ConnectionManager {
	return ci.client.ic.ConnectionManager
}

func (ci *ClientInternal) DispatchService() *event.DispatchService {
	return ci.client.ic.EventDispatcher
}

func (ci *ClientInternal) InvocationService() *invocation.Service {
	return ci.client.ic.InvocationService
}

func (ci *ClientInternal) InvocationHandler() invocation.Handler {
	return ci.client.ic.InvocationHandler
}

func (ci *ClientInternal) EncodeData(obj interface{}) (Data, error) {
	return ci.proxy.convertToData(obj)
}

func (ci *ClientInternal) DecodeData(data Data) (interface{}, error) {
	return ci.proxy.convertToObject(data)
}

func (ci *ClientInternal) InvokeOnRandomTarget(ctx context.Context, request *ClientMessage, handler ClientMessageHandler) (*ClientMessage, error) {
	return ci.proxy.invokeOnRandomTarget(ctx, request, handler)
}

func (ci *ClientInternal) InvokeOnPartition(ctx context.Context, request *ClientMessage, partitionID int32) (*ClientMessage, error) {
	return ci.proxy.invokeOnPartition(ctx, request, partitionID)
}

func (ci *ClientInternal) InvokeOnKey(ctx context.Context, request *ClientMessage, keyData Data) (*ClientMessage, error) {
	return ci.proxy.invokeOnKey(ctx, request, keyData)
}

func (ci *ClientInternal) InvokeOnMember(ctx context.Context, request *ClientMessage, uuid types.UUID) (*ClientMessage, error) {
	panic("TODO")
}
