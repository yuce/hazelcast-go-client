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
	// hex: 0x012600
	MapKeySetWithPredicateCodecRequestMessageType = int32(75264)
	// hex: 0x012601
	MapKeySetWithPredicateCodecResponseMessageType = int32(75265)

	MapKeySetWithPredicateCodecRequestInitialFrameSize = proto.PartitionIDOffset + proto.IntSizeInBytes
)

// Queries the map based on the specified predicate and returns the keys of matching entries. Specified predicate
// runs on all members in parallel.The set is NOT backed by the map, so changes to the map are NOT reflected in the
// set, and vice-versa. This method is always executed by a distributed query, so it may throw a
// QueryResultSizeExceededException if query result size limit is configured.
type mapKeySetWithPredicateCodec struct{}

var MapKeySetWithPredicateCodec mapKeySetWithPredicateCodec

func (mapKeySetWithPredicateCodec) EncodeRequest(name string, predicate serialization.Data) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(true)

	initialFrame := proto.NewFrame(make([]byte, MapKeySetWithPredicateCodecRequestInitialFrameSize))
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(MapKeySetWithPredicateCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	internal.StringCodec.Encode(clientMessage, name)
	internal.DataCodec.Encode(clientMessage, predicate)

	return clientMessage
}

func (mapKeySetWithPredicateCodec) DecodeResponse(clientMessage *proto.ClientMessage) []serialization.Data {
	frameIterator := clientMessage.FrameIterator()
	// empty initial frame
	frameIterator.Next()

	return internal.ListMultiFrameCodec.DecodeForData(frameIterator)
}