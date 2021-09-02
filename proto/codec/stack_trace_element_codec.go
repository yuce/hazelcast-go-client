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
	ihzerrors "github.com/hazelcast/hazelcast-go-client/internal/hzerrors"
	proto2 "github.com/hazelcast/hazelcast-go-client/proto"
)

const (
	StackTraceElementCodecLineNumberFieldOffset      = 0
	StackTraceElementCodecLineNumberInitialFrameSize = StackTraceElementCodecLineNumberFieldOffset + proto2.IntSizeInBytes
)

func EncodeStackTraceElement(clientMessage *proto2.ClientMessage, stackTraceElement ihzerrors.StackTraceElement) {
	clientMessage.AddFrame(proto2.BeginFrame.Copy())
	initialFrame := proto2.NewFrame(make([]byte, StackTraceElementCodecLineNumberInitialFrameSize))
	FixSizedTypesCodec.EncodeInt(initialFrame.Content, StackTraceElementCodecLineNumberFieldOffset, int32(stackTraceElement.LineNumber))
	clientMessage.AddFrame(initialFrame)

	EncodeString(clientMessage, stackTraceElement.ClassName)
	EncodeString(clientMessage, stackTraceElement.MethodName)
	CodecUtil.EncodeNullableForString(clientMessage, stackTraceElement.FileName)

	clientMessage.AddFrame(proto2.EndFrame.Copy())
}

func DecodeStackTraceElement(frameIterator *proto2.ForwardFrameIterator) ihzerrors.StackTraceElement {
	// begin frame
	frameIterator.Next()
	initialFrame := frameIterator.Next()
	lineNumber := FixSizedTypesCodec.DecodeInt(initialFrame.Content, StackTraceElementCodecLineNumberFieldOffset)

	className := DecodeString(frameIterator)
	methodName := DecodeString(frameIterator)
	fileName := CodecUtil.DecodeNullableForString(frameIterator)
	CodecUtil.FastForwardToEndFrame(frameIterator)
	return proto2.NewStackTraceElement(className, methodName, fileName, lineNumber)
}
