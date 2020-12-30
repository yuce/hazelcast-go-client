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
)

const (
	// hex: 0x012A00
	MapSizeCodecRequestMessageType = int32(76288)
	// hex: 0x012A01
	MapSizeCodecResponseMessageType = int32(76289)

	MapSizeCodecRequestInitialFrameSize = proto.PartitionIDOffset + proto.IntSizeInBytes

	MapSizeResponseResponseOffset = proto.ResponseBackupAcksOffset + proto.ByteSizeInBytes
)

// Returns the number of key-value mappings in this map.  If the map contains more than Integer.MAX_VALUE elements,
// returns Integer.MAX_VALUE
type mapSizeCodec struct{}

var MapSizeCodec mapSizeCodec

func (mapSizeCodec) EncodeRequest(name string) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(true)

	initialFrame := proto.NewFrame(make([]byte, MapSizeCodecRequestInitialFrameSize))
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(MapSizeCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	internal.StringCodec.Encode(clientMessage, name)

	return clientMessage
}

func (mapSizeCodec) DecodeResponse(clientMessage *proto.ClientMessage) int32 {
	frameIterator := clientMessage.FrameIterator()
	initialFrame := frameIterator.Next()

	return internal.FixSizedTypesCodec.DecodeInt(initialFrame.Content, MapSizeResponseResponseOffset)
}
