/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License")
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package codec

import (
	"github.com/hazelcast/hazelcast-go-client/internal/cp/types"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	iserialization "github.com/hazelcast/hazelcast-go-client/internal/serialization"
)

const (
	AtomicRefCompareAndSetCodecRequestMessageType  = int32(0x0A0200)
	AtomicRefCompareAndSetCodecResponseMessageType = int32(0x0A0201)

	AtomicRefCompareAndSetCodecRequestInitialFrameSize = proto.PartitionIDOffset + proto.IntSizeInBytes

	AtomicRefCompareAndSetResponseResponseOffset = proto.ResponseBackupAcksOffset + proto.ByteSizeInBytes
)

// Alters the currently stored value by applying a function on it.

func EncodeAtomicRefCompareAndSetRequest(groupId types.RaftGroupID, name string, oldValue iserialization.Data, newValue iserialization.Data) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(false)

	initialFrame := proto.NewFrameWith(make([]byte, AtomicRefCompareAndSetCodecRequestInitialFrameSize), proto.UnfragmentedMessage)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(AtomicRefCompareAndSetCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	EncodeRaftGroupId(clientMessage, groupId)
	EncodeString(clientMessage, name)
	EncodeNullableData(clientMessage, oldValue)
	EncodeNullableData(clientMessage, newValue)

	return clientMessage
}

func DecodeAtomicRefCompareAndSetResponse(clientMessage *proto.ClientMessage) bool {
	frameIterator := clientMessage.FrameIterator()
	initialFrame := frameIterator.Next()

	return DecodeBoolean(initialFrame.Content, AtomicRefCompareAndSetResponseResponseOffset)
}
