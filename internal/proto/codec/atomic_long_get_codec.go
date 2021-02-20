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
	"github.com/hazelcast/hazelcast-go-client/v4/internal/proto"
)

const (
	// hex: 0x090500
	AtomicLongGetCodecRequestMessageType = int32(591104)
	// hex: 0x090501
	AtomicLongGetCodecResponseMessageType = int32(591105)

	AtomicLongGetCodecRequestInitialFrameSize = proto.PartitionIDOffset + proto.IntSizeInBytes

	AtomicLongGetResponseResponseOffset = proto.ResponseBackupAcksOffset + proto.ByteSizeInBytes
)

// Gets the current value.
type atomiclongGetCodec struct{}

var AtomicLongGetCodec atomiclongGetCodec

func (atomiclongGetCodec) EncodeRequest(groupId proto.RaftGroupId, name string) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(true)

	initialFrame := proto.NewFrame(make([]byte, AtomicLongGetCodecRequestInitialFrameSize))
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(AtomicLongGetCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	RaftGroupIdCodec.Encode(clientMessage, groupId)
	StringCodec.Encode(clientMessage, name)

	return clientMessage
}

func (atomiclongGetCodec) DecodeResponse(clientMessage *proto.ClientMessage) int64 {
	frameIterator := clientMessage.FrameIterator()
	initialFrame := frameIterator.Next()

	return FixSizedTypesCodec.DecodeLong(initialFrame.Content, AtomicLongGetResponseResponseOffset)
}
