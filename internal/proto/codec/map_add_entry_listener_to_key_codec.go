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
	"github.com/hazelcast/hazelcast-go-client/v4/hazelcast"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/v4/serialization"
)

const (
	// hex: 0x011800
	MapAddEntryListenerToKeyCodecRequestMessageType = int32(71680)
	// hex: 0x011801
	MapAddEntryListenerToKeyCodecResponseMessageType = int32(71681)

	// hex: 0x011802
	MapAddEntryListenerToKeyCodecEventEntryMessageType = int32(71682)

	MapAddEntryListenerToKeyCodecRequestIncludeValueOffset  = proto.PartitionIDOffset + proto.IntSizeInBytes
	MapAddEntryListenerToKeyCodecRequestListenerFlagsOffset = MapAddEntryListenerToKeyCodecRequestIncludeValueOffset + proto.BooleanSizeInBytes
	MapAddEntryListenerToKeyCodecRequestLocalOnlyOffset     = MapAddEntryListenerToKeyCodecRequestListenerFlagsOffset + proto.IntSizeInBytes
	MapAddEntryListenerToKeyCodecRequestInitialFrameSize    = MapAddEntryListenerToKeyCodecRequestLocalOnlyOffset + proto.BooleanSizeInBytes

	MapAddEntryListenerToKeyResponseResponseOffset                  = proto.ResponseBackupAcksOffset + proto.ByteSizeInBytes
	MapAddEntryListenerToKeyEventEntryEventTypeOffset               = proto.PartitionIDOffset + proto.IntSizeInBytes
	MapAddEntryListenerToKeyEventEntryUuidOffset                    = MapAddEntryListenerToKeyEventEntryEventTypeOffset + proto.IntSizeInBytes
	MapAddEntryListenerToKeyEventEntryNumberOfAffectedEntriesOffset = MapAddEntryListenerToKeyEventEntryUuidOffset + proto.UuidSizeInBytes
)

// Adds a MapListener for this map. To receive an event, you should implement a corresponding MapListener
// sub-interface for that event.

func EncodeMapAddEntryListenerToKeyRequest(name string, key serialization.Data, includeValue bool, listenerFlags int32, localOnly bool) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(false)

	initialFrame := proto.NewFrame(make([]byte, MapAddEntryListenerToKeyCodecRequestInitialFrameSize))
	FixSizedTypesCodec.EncodeBoolean(initialFrame.Content, MapAddEntryListenerToKeyCodecRequestIncludeValueOffset, includeValue)
	FixSizedTypesCodec.EncodeInt(initialFrame.Content, MapAddEntryListenerToKeyCodecRequestListenerFlagsOffset, listenerFlags)
	FixSizedTypesCodec.EncodeBoolean(initialFrame.Content, MapAddEntryListenerToKeyCodecRequestLocalOnlyOffset, localOnly)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(MapAddEntryListenerToKeyCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	EncodeString(clientMessage, name)
	EncodeData(clientMessage, key)

	return clientMessage
}

func DecodeMapAddEntryListenerToKeyResponse(clientMessage *proto.ClientMessage) hazelcast.UUID {
	frameIterator := clientMessage.FrameIterator()
	initialFrame := frameIterator.Next()

	return FixSizedTypesCodec.DecodeUUID(initialFrame.Content, MapAddEntryListenerToKeyResponseResponseOffset)
}

func HandleMapAddEntryListenerToKey(clientMessage *proto.ClientMessage, handleEntryEvent func(key serialization.Data, value serialization.Data, oldValue serialization.Data, mergingValue serialization.Data, eventType int32, uuid hazelcast.UUID, numberOfAffectedEntries int32)) {
	messageType := clientMessage.GetMessageType()
	frameIterator := clientMessage.FrameIterator()
	if messageType == MapAddEntryListenerToKeyCodecEventEntryMessageType {
		initialFrame := frameIterator.Next()
		eventType := FixSizedTypesCodec.DecodeInt(initialFrame.Content, MapAddEntryListenerToKeyEventEntryEventTypeOffset)
		uuid := FixSizedTypesCodec.DecodeUUID(initialFrame.Content, MapAddEntryListenerToKeyEventEntryUuidOffset)
		numberOfAffectedEntries := FixSizedTypesCodec.DecodeInt(initialFrame.Content, MapAddEntryListenerToKeyEventEntryNumberOfAffectedEntriesOffset)
		key := CodecUtil.DecodeNullableForData(frameIterator)
		value := CodecUtil.DecodeNullableForData(frameIterator)
		oldValue := CodecUtil.DecodeNullableForData(frameIterator)
		mergingValue := CodecUtil.DecodeNullableForData(frameIterator)
		handleEntryEvent(key, value, oldValue, mergingValue, eventType, uuid, numberOfAffectedEntries)
		return
	}
}
