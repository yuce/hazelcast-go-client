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
	// hex: 0x170800
	RingbufferAddAllCodecRequestMessageType = int32(1509376)
	// hex: 0x170801
	RingbufferAddAllCodecResponseMessageType = int32(1509377)

	RingbufferAddAllCodecRequestOverflowPolicyOffset = proto.PartitionIDOffset + proto.IntSizeInBytes
	RingbufferAddAllCodecRequestInitialFrameSize     = RingbufferAddAllCodecRequestOverflowPolicyOffset + proto.IntSizeInBytes

	RingbufferAddAllResponseResponseOffset = proto.ResponseBackupAcksOffset + proto.ByteSizeInBytes
)

// Adds all the items of a collection to the tail of the Ringbuffer. A addAll is likely to outperform multiple calls
// to add(Object) due to better io utilization and a reduced number of executed operations. If the batch is empty,
// the call is ignored. When the collection is not empty, the content is copied into a different data-structure.
// This means that: after this call completes, the collection can be re-used. the collection doesn't need to be serializable.
// If the collection is larger than the capacity of the ringbuffer, then the items that were written first will be
// overwritten. Therefor this call will not block. The items are inserted in the order of the Iterator of the collection.
// If an addAll is executed concurrently with an add or addAll, no guarantee is given that items are contiguous.
// The result of the future contains the sequenceId of the last written item
type ringbufferAddAllCodec struct{}

var RingbufferAddAllCodec ringbufferAddAllCodec

func (ringbufferAddAllCodec) EncodeRequest(name string, valueList []serialization.Data, overflowPolicy int32) *proto.ClientMessage {
	clientMessage := proto.NewClientMessageForEncode()
	clientMessage.SetRetryable(false)

	initialFrame := proto.NewFrame(make([]byte, RingbufferAddAllCodecRequestInitialFrameSize))
	internal.FixSizedTypesCodec.EncodeInt(initialFrame.Content, RingbufferAddAllCodecRequestOverflowPolicyOffset, overflowPolicy)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(RingbufferAddAllCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	internal.StringCodec.Encode(clientMessage, name)
	internal.ListMultiFrameCodec.EncodeForData(clientMessage, valueList)

	return clientMessage
}

func (ringbufferAddAllCodec) DecodeResponse(clientMessage *proto.ClientMessage) int64 {
	frameIterator := clientMessage.FrameIterator()
	initialFrame := frameIterator.Next()

	return internal.FixSizedTypesCodec.DecodeLong(initialFrame.Content, RingbufferAddAllResponseResponseOffset)
}