/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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
	proto2 "github.com/hazelcast/hazelcast-go-client/proto"
)

const (
	// hex: 0x013800
	MapFetchEntriesCodecRequestMessageType = int32(79872)
	// hex: 0x013801
	MapFetchEntriesCodecResponseMessageType = int32(79873)

	MapFetchEntriesCodecRequestBatchOffset      = proto2.PartitionIDOffset + proto2.IntSizeInBytes
	MapFetchEntriesCodecRequestInitialFrameSize = MapFetchEntriesCodecRequestBatchOffset + proto2.IntSizeInBytes
)

// Fetches specified number of entries from the specified partition starting from specified table index.

func EncodeMapFetchEntriesRequest(name string, iterationPointers []proto2.Pair, batch int32) *proto2.ClientMessage {
	clientMessage := proto2.NewClientMessageForEncode()
	clientMessage.SetRetryable(true)

	initialFrame := proto2.NewFrameWith(make([]byte, MapFetchEntriesCodecRequestInitialFrameSize), proto2.UnfragmentedMessage)
	FixSizedTypesCodec.EncodeInt(initialFrame.Content, MapFetchEntriesCodecRequestBatchOffset, batch)
	clientMessage.AddFrame(initialFrame)
	clientMessage.SetMessageType(MapFetchEntriesCodecRequestMessageType)
	clientMessage.SetPartitionId(-1)

	EncodeString(clientMessage, name)
	EncodeEntryListIntegerInteger(clientMessage, iterationPointers)

	return clientMessage
}

func DecodeMapFetchEntriesResponse(clientMessage *proto2.ClientMessage) (iterationPointers []proto2.Pair, entries []proto2.Pair) {
	frameIterator := clientMessage.FrameIterator()
	frameIterator.Next()

	iterationPointers = DecodeEntryListIntegerInteger(frameIterator)
	entries = DecodeEntryListForDataAndData(frameIterator)

	return iterationPointers, entries
}
