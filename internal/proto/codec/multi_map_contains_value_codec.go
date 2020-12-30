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
	// hex: 0x020800
	MultiMapContainsValueCodecRequestMessageType = int32(133120)
	// hex: 0x020801
	MultiMapContainsValueCodecResponseMessageType = int32(133121)

	MultiMapContainsValueCodecRequestInitialFrameSize = proto.PartitionIDOffset + proto.IntSizeInBytes

	MultiMapContainsValueResponseResponseOffset = proto.ResponseBackupAcksOffset + proto.ByteSizeInBytes
)

// Returns whether the multimap contains an entry with the value.
type multimapContainsValueCodec struct{}

var MultiMapContainsValueCodec multimapContainsValueCodec

func (multimapContainsValueCodec) EncodeRequest(name string, value serialization.Data) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(true)

	initialFrame := proto.NewFrame(make([]byte, MultiMapContainsValueCodecRequestInitialFrameSize))
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(MultiMapContainsValueCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	internal.StringCodec.Encode(clientMessage, name)
	internal.DataCodec.Encode(clientMessage, value)

	return clientMessage
}

func (multimapContainsValueCodec) DecodeResponse(clientMessage *proto.ClientMessage) bool {
	frameIterator := clientMessage.FrameIterator()
	initialFrame := frameIterator.Next()

	return internal.FixSizedTypesCodec.DecodeBoolean(initialFrame.Content, MultiMapContainsValueResponseResponseOffset)
}