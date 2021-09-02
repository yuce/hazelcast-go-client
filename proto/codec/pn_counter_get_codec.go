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
	proto2 "github.com/hazelcast/hazelcast-go-client/proto"
	"github.com/hazelcast/hazelcast-go-client/types"
)

const (
	// hex: 0x1D0100
	PNCounterGetCodecRequestMessageType = int32(1900800)
	// hex: 0x1D0101
	PNCounterGetCodecResponseMessageType = int32(1900801)

	PNCounterGetCodecRequestTargetReplicaUUIDOffset = proto2.PartitionIDOffset + proto2.IntSizeInBytes
	PNCounterGetCodecRequestInitialFrameSize        = PNCounterGetCodecRequestTargetReplicaUUIDOffset + proto2.UuidSizeInBytes

	PNCounterGetResponseValueOffset        = proto2.ResponseBackupAcksOffset + proto2.ByteSizeInBytes
	PNCounterGetResponseReplicaCountOffset = PNCounterGetResponseValueOffset + proto2.LongSizeInBytes
)

// Query operation to retrieve the current value of the PNCounter.
// <p>
// The invocation will return the replica timestamps (vector clock) which
// can then be sent with the next invocation to keep session consistency
// guarantees.
// The target replica is determined by the {@code targetReplica} parameter.
// If smart routing is disabled, the actual member processing the client
// message may act as a proxy.

func EncodePNCounterGetRequest(name string, replicaTimestamps []proto2.Pair, targetReplicaUUID types.UUID) *proto2.ClientMessage {
	clientMessage := proto2.NewClientMessageForEncode()
	clientMessage.SetRetryable(true)

	initialFrame := proto2.NewFrameWith(make([]byte, PNCounterGetCodecRequestInitialFrameSize), proto2.UnfragmentedMessage)
	FixSizedTypesCodec.EncodeUUID(initialFrame.Content, PNCounterGetCodecRequestTargetReplicaUUIDOffset, targetReplicaUUID)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(PNCounterGetCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	EncodeString(clientMessage, name)
	EncodeEntryListUUIDLong(clientMessage, replicaTimestamps)

	return clientMessage
}

func DecodePNCounterGetResponse(clientMessage *proto2.ClientMessage) (value int64, replicaTimestamps []proto2.Pair, replicaCount int32) {
	frameIterator := clientMessage.FrameIterator()
	initialFrame := frameIterator.Next()

	value = FixSizedTypesCodec.DecodeLong(initialFrame.Content, PNCounterGetResponseValueOffset)
	replicaCount = FixSizedTypesCodec.DecodeInt(initialFrame.Content, PNCounterGetResponseReplicaCountOffset)
	replicaTimestamps = DecodeEntryListUUIDLong(frameIterator)

	return value, replicaTimestamps, replicaCount
}
