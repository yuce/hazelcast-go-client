// Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package codec

import (
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/internal/proto/codec/internal"
	"github.com/hazelcast/hazelcast-go-client/serialization"
)

const (
	// hex: 0x030700
	QueuePeekCodecRequestMessageType = int32(198400)
	// hex: 0x030701
	QueuePeekCodecResponseMessageType = int32(198401)

	QueuePeekCodecRequestInitialFrameSize = proto.PartitionIDOffset + proto.IntSizeInBytes
)

// Retrieves, but does not remove, the head of this queue, or returns null if this queue is empty.
type queuePeekCodec struct{}

var QueuePeekCodec queuePeekCodec

func (queuePeekCodec) EncodeRequest(name string) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(false)

	initialFrame := proto.NewFrame(make([]byte, QueuePeekCodecRequestInitialFrameSize))
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(QueuePeekCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	internal.StringCodec.Encode(clientMessage, name)

	return clientMessage
}

func (queuePeekCodec) DecodeResponse(clientMessage *proto.ClientMessage) serialization.Data {
	frameIterator := clientMessage.FrameIterator()
	// empty initial frame
	frameIterator.Next()

	return internal.CodecUtil.DecodeNullableForData(frameIterator)
}
