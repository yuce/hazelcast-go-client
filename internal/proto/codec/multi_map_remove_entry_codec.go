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
	// hex: 0x021500
	MultiMapRemoveEntryCodecRequestMessageType = int32(136448)
	// hex: 0x021501
	MultiMapRemoveEntryCodecResponseMessageType = int32(136449)

	MultiMapRemoveEntryCodecRequestThreadIdOffset   = proto.PartitionIDOffset + proto.IntSizeInBytes
	MultiMapRemoveEntryCodecRequestInitialFrameSize = MultiMapRemoveEntryCodecRequestThreadIdOffset + proto.LongSizeInBytes

	MultiMapRemoveEntryResponseResponseOffset = proto.ResponseBackupAcksOffset + proto.ByteSizeInBytes
)

// Removes all the entries with the given key. The collection is NOT backed by the map, so changes to the map are
// NOT reflected in the collection, and vice-versa.
type multimapRemoveEntryCodec struct{}

var MultiMapRemoveEntryCodec multimapRemoveEntryCodec

func (multimapRemoveEntryCodec) EncodeRequest(name string, key serialization.Data, value serialization.Data, threadId int64) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(false)

	initialFrame := proto.NewFrame(make([]byte, MultiMapRemoveEntryCodecRequestInitialFrameSize))
	internal.FixSizedTypesCodec.EncodeLong(initialFrame.Content, MultiMapRemoveEntryCodecRequestThreadIdOffset, threadId)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(MultiMapRemoveEntryCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	internal.StringCodec.Encode(clientMessage, name)
	internal.DataCodec.Encode(clientMessage, key)
	internal.DataCodec.Encode(clientMessage, value)

	return clientMessage
}

func (multimapRemoveEntryCodec) DecodeResponse(clientMessage *proto.ClientMessage) bool {
	frameIterator := clientMessage.FrameIterator()
	initialFrame := frameIterator.Next()

	return internal.FixSizedTypesCodec.DecodeBoolean(initialFrame.Content, MultiMapRemoveEntryResponseResponseOffset)
}
