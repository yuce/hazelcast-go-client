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
	// hex: 0x013A00
	MapAggregateWithPredicateCodecRequestMessageType = int32(80384)
	// hex: 0x013A01
	MapAggregateWithPredicateCodecResponseMessageType = int32(80385)

	MapAggregateWithPredicateCodecRequestInitialFrameSize = proto.PartitionIDOffset + proto.IntSizeInBytes
)

// Applies the aggregation logic on map entries filtered with the Predicate and returns the result
type mapAggregateWithPredicateCodec struct{}

var MapAggregateWithPredicateCodec mapAggregateWithPredicateCodec

func (mapAggregateWithPredicateCodec) EncodeRequest(name string, aggregator serialization.Data, predicate serialization.Data) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(true)

	initialFrame := proto.NewFrame(make([]byte, MapAggregateWithPredicateCodecRequestInitialFrameSize))
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(MapAggregateWithPredicateCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	internal.StringCodec.Encode(clientMessage, name)
	internal.DataCodec.Encode(clientMessage, aggregator)
	internal.DataCodec.Encode(clientMessage, predicate)

	return clientMessage
}

func (mapAggregateWithPredicateCodec) DecodeResponse(clientMessage *proto.ClientMessage) serialization.Data {
	frameIterator := clientMessage.FrameIterator()
	// empty initial frame
	frameIterator.Next()

	return internal.CodecUtil.DecodeNullableForData(frameIterator)
}
