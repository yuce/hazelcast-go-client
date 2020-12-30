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
	"github.com/hazelcast/hazelcast-go-client/serialization"
)

const (
	// hex: 0x013F00
	MapAddNearCacheInvalidationListenerCodecRequestMessageType = int32(81664)
	// hex: 0x013F01
	MapAddNearCacheInvalidationListenerCodecResponseMessageType = int32(81665)

	// hex: 0x013F02
	MapAddNearCacheInvalidationListenerCodecEventIMapInvalidationMessageType = int32(81666)

	// hex: 0x013F03
	MapAddNearCacheInvalidationListenerCodecEventIMapBatchInvalidationMessageType = int32(81667)

	MapAddNearCacheInvalidationListenerCodecRequestListenerFlagsOffset = proto.PartitionIDOffset + proto.IntSizeInBytes
	MapAddNearCacheInvalidationListenerCodecRequestLocalOnlyOffset     = MapAddNearCacheInvalidationListenerCodecRequestListenerFlagsOffset + proto.IntSizeInBytes
	MapAddNearCacheInvalidationListenerCodecRequestInitialFrameSize    = MapAddNearCacheInvalidationListenerCodecRequestLocalOnlyOffset + proto.BooleanSizeInBytes

	MapAddNearCacheInvalidationListenerResponseResponseOffset                   = proto.ResponseBackupAcksOffset + proto.ByteSizeInBytes
	MapAddNearCacheInvalidationListenerEventIMapInvalidationSourceUuidOffset    = proto.PartitionIDOffset + proto.IntSizeInBytes
	MapAddNearCacheInvalidationListenerEventIMapInvalidationPartitionUuidOffset = MapAddNearCacheInvalidationListenerEventIMapInvalidationSourceUuidOffset + proto.UuidSizeInBytes
	MapAddNearCacheInvalidationListenerEventIMapInvalidationSequenceOffset      = MapAddNearCacheInvalidationListenerEventIMapInvalidationPartitionUuidOffset + proto.UuidSizeInBytes
)

// Adds listener to map. This listener will be used to listen near cache invalidation events.
type mapAddNearCacheInvalidationListenerCodec struct{}

var MapAddNearCacheInvalidationListenerCodec mapAddNearCacheInvalidationListenerCodec

func (mapAddNearCacheInvalidationListenerCodec) EncodeRequest(name string, listenerFlags int32, localOnly bool) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(false)

	initialFrame := proto.NewFrame(make([]byte, MapAddNearCacheInvalidationListenerCodecRequestInitialFrameSize))
	internal.FixSizedTypesCodec.EncodeInt(initialFrame.Content, MapAddNearCacheInvalidationListenerCodecRequestListenerFlagsOffset, listenerFlags)
	internal.FixSizedTypesCodec.EncodeBoolean(initialFrame.Content, MapAddNearCacheInvalidationListenerCodecRequestLocalOnlyOffset, localOnly)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(MapAddNearCacheInvalidationListenerCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	internal.StringCodec.Encode(clientMessage, name)

	return clientMessage
}

func (mapAddNearCacheInvalidationListenerCodec) DecodeResponse(clientMessage *proto.ClientMessage) core.UUID {
	frameIterator := clientMessage.FrameIterator()
	initialFrame := frameIterator.Next()

	return internal.FixSizedTypesCodec.DecodeUUID(initialFrame.Content, MapAddNearCacheInvalidationListenerResponseResponseOffset)
}

func (mapAddNearCacheInvalidationListenerCodec) Handle(clientMessage *proto.ClientMessage, handleIMapInvalidationEvent func(key serialization.Data, sourceUuid core.UUID, partitionUuid core.UUID, sequence int64), handleIMapBatchInvalidationEvent func(keys []serialization.Data, sourceUuids []core.UUID, partitionUuids []core.UUID, sequences []int64)) {
	messageType := clientMessage.GetMessageType()
	frameIterator := clientMessage.FrameIterator()
	if messageType == MapAddNearCacheInvalidationListenerCodecEventIMapInvalidationMessageType {
		initialFrame := frameIterator.Next()
		sourceUuid := internal.FixSizedTypesCodec.DecodeUUID(initialFrame.Content, MapAddNearCacheInvalidationListenerEventIMapInvalidationSourceUuidOffset)
		partitionUuid := internal.FixSizedTypesCodec.DecodeUUID(initialFrame.Content, MapAddNearCacheInvalidationListenerEventIMapInvalidationPartitionUuidOffset)
		sequence := internal.FixSizedTypesCodec.DecodeLong(initialFrame.Content, MapAddNearCacheInvalidationListenerEventIMapInvalidationSequenceOffset)
		key := internal.CodecUtil.DecodeNullableForData(frameIterator)
		handleIMapInvalidationEvent(key, sourceUuid, partitionUuid, sequence)
		return
	}
	if messageType == MapAddNearCacheInvalidationListenerCodecEventIMapBatchInvalidationMessageType {
		//empty initial frame
		frameIterator.Next()
		keys := internal.ListMultiFrameCodec.DecodeForData(frameIterator)
		sourceUuids := internal.ListUUIDCodec.Decode(frameIterator)
		partitionUuids := internal.ListUUIDCodec.Decode(frameIterator)
		sequences := internal.ListLongCodec.Decode(frameIterator)
		handleIMapBatchInvalidationEvent(keys, sourceUuids, partitionUuids, sequences)
		return
	}
}
