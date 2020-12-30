/*
* Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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
package internal

import (
	"github.com/hazelcast/hazelcast-go-client/core"
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
)

const (
	AddressCodecPortFieldOffset      = 0
	AddressCodecPortInitialFrameSize = AddressCodecPortFieldOffset + proto.IntSizeInBytes
)

type addressCodec struct{}

var AddressCodec addressCodec

func (addressCodec) Encode(clientMessage *proto.ClientMessage, address core.Address) {
	clientMessage.AddFrame(proto.BeginFrame.Copy())
	initialFrame := proto.NewFrame(make([]byte, AddressCodecPortInitialFrameSize))
	FixSizedTypesCodec.EncodeInt(initialFrame.Content, AddressCodecPortFieldOffset, address.GetPort())
	clientMessage.AddFrame(initialFrame)

	StringCodec.Encode(clientMessage, address.GetHost())

	clientMessage.AddFrame(proto.EndFrame.Copy())
}

func (addressCodec) Decode(frameIterator *proto.ForwardFrameIterator) core.Address {
	// begin frame
	frameIterator.Next()
	initialFrame := frameIterator.Next()
	port := FixSizedTypesCodec.DecodeInt(initialFrame.Content, AddressCodecPortFieldOffset)

	host := StringCodec.Decode(frameIterator)
	CodecUtil.FastForwardToEndFrame(frameIterator)

	return proto.NewAddress(host, port)
}
