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
	"github.com/hazelcast/hazelcast-go-client/core"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/internal/proto/codec/internal"
)

const (
	// hex: 0x011A00
	MapRemoveEntryListenerCodecRequestMessageType = int32(72192)
	// hex: 0x011A01
	MapRemoveEntryListenerCodecResponseMessageType = int32(72193)

	MapRemoveEntryListenerCodecRequestRegistrationIdOffset = proto.PartitionIDOffset + proto.IntSizeInBytes
	MapRemoveEntryListenerCodecRequestInitialFrameSize     = MapRemoveEntryListenerCodecRequestRegistrationIdOffset + proto.UuidSizeInBytes

	MapRemoveEntryListenerResponseResponseOffset = proto.ResponseBackupAcksOffset + proto.ByteSizeInBytes
)

// Removes the specified entry listener. If there is no such listener added before, this call does no change in the
// cluster and returns false.
type mapRemoveEntryListenerCodec struct{}

var MapRemoveEntryListenerCodec mapRemoveEntryListenerCodec

func (mapRemoveEntryListenerCodec) EncodeRequest(name string, registrationId core.UUID) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(true)

	initialFrame := proto.NewFrame(make([]byte, MapRemoveEntryListenerCodecRequestInitialFrameSize))
	internal.FixSizedTypesCodec.EncodeUUID(initialFrame.Content, MapRemoveEntryListenerCodecRequestRegistrationIdOffset, registrationId)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(MapRemoveEntryListenerCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	internal.StringCodec.Encode(clientMessage, name)

	return clientMessage
}

func (mapRemoveEntryListenerCodec) DecodeResponse(clientMessage *proto.ClientMessage) bool {
	frameIterator := clientMessage.FrameIterator()
	initialFrame := frameIterator.Next()

	return internal.FixSizedTypesCodec.DecodeBoolean(initialFrame.Content, MapRemoveEntryListenerResponseResponseOffset)
}