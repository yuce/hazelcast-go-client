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
	// hex: 0x0D0300
	ReplicatedMapIsEmptyCodecRequestMessageType = int32(852736)
	// hex: 0x0D0301
	ReplicatedMapIsEmptyCodecResponseMessageType = int32(852737)

	ReplicatedMapIsEmptyCodecRequestInitialFrameSize = proto.PartitionIDOffset + proto.IntSizeInBytes

	ReplicatedMapIsEmptyResponseResponseOffset = proto.ResponseBackupAcksOffset + proto.ByteSizeInBytes
)

// Return true if this map contains no key-value mappings
type replicatedmapIsEmptyCodec struct{}

var ReplicatedMapIsEmptyCodec replicatedmapIsEmptyCodec

func (replicatedmapIsEmptyCodec) EncodeRequest(name string) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(true)

	initialFrame := proto.NewFrame(make([]byte, ReplicatedMapIsEmptyCodecRequestInitialFrameSize))
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(ReplicatedMapIsEmptyCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	internal.StringCodec.Encode(clientMessage, name)

	return clientMessage
}

func (replicatedmapIsEmptyCodec) DecodeResponse(clientMessage *proto.ClientMessage) bool {
	frameIterator := clientMessage.FrameIterator()
	initialFrame := frameIterator.Next()

	return internal.FixSizedTypesCodec.DecodeBoolean(initialFrame.Content, ReplicatedMapIsEmptyResponseResponseOffset)
}