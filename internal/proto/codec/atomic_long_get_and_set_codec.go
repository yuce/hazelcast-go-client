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
	// hex: 0x090700
	AtomicLongGetAndSetCodecRequestMessageType = int32(591616)
	// hex: 0x090701
	AtomicLongGetAndSetCodecResponseMessageType = int32(591617)

	AtomicLongGetAndSetCodecRequestNewValueOffset   = proto.PartitionIDOffset + proto.IntSizeInBytes
	AtomicLongGetAndSetCodecRequestInitialFrameSize = AtomicLongGetAndSetCodecRequestNewValueOffset + proto.LongSizeInBytes

	AtomicLongGetAndSetResponseResponseOffset = proto.ResponseBackupAcksOffset + proto.ByteSizeInBytes
)

// Atomically sets the given value and returns the old value.
type atomiclongGetAndSetCodec struct{}

var AtomicLongGetAndSetCodec atomiclongGetAndSetCodec

func (atomiclongGetAndSetCodec) EncodeRequest(groupId proto.RaftGroupId, name string, newValue int64) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(false)

	initialFrame := proto.NewFrame(make([]byte, AtomicLongGetAndSetCodecRequestInitialFrameSize))
	internal.FixSizedTypesCodec.EncodeLong(initialFrame.Content, AtomicLongGetAndSetCodecRequestNewValueOffset, newValue)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(AtomicLongGetAndSetCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	internal.RaftGroupIdCodec.Encode(clientMessage, groupId)
	internal.StringCodec.Encode(clientMessage, name)

	return clientMessage
}

func (atomiclongGetAndSetCodec) DecodeResponse(clientMessage *proto.ClientMessage) int64 {
	frameIterator := clientMessage.FrameIterator()
	initialFrame := frameIterator.Next()

	return internal.FixSizedTypesCodec.DecodeLong(initialFrame.Content, AtomicLongGetAndSetResponseResponseOffset)
}