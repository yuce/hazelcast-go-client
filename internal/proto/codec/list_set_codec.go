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
	// hex: 0x051000
	ListSetCodecRequestMessageType = int32(331776)
	// hex: 0x051001
	ListSetCodecResponseMessageType = int32(331777)

	ListSetCodecRequestIndexOffset      = proto.PartitionIDOffset + proto.IntSizeInBytes
	ListSetCodecRequestInitialFrameSize = ListSetCodecRequestIndexOffset + proto.IntSizeInBytes
)

// The element previously at the specified position
type listSetCodec struct{}

var ListSetCodec listSetCodec

func (listSetCodec) EncodeRequest(name string, index int32, value serialization.Data) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(false)

	initialFrame := proto.NewFrame(make([]byte, ListSetCodecRequestInitialFrameSize))
	internal.FixSizedTypesCodec.EncodeInt(initialFrame.Content, ListSetCodecRequestIndexOffset, index)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(ListSetCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	internal.StringCodec.Encode(clientMessage, name)
	internal.DataCodec.Encode(clientMessage, value)

	return clientMessage
}

func (listSetCodec) DecodeResponse(clientMessage *proto.ClientMessage) serialization.Data {
	frameIterator := clientMessage.FrameIterator()
	// empty initial frame
	frameIterator.Next()

	return internal.CodecUtil.DecodeNullableForData(frameIterator)
}