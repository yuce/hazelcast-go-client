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
	"encoding/binary"
	"testing"

	proto2 "github.com/hazelcast/hazelcast-go-client/proto"

	"github.com/hazelcast/hazelcast-go-client/types"

	"github.com/stretchr/testify/assert"
)

func TestCodecUtil_FastForwardToEndFrame(t *testing.T) {
	// given
	frame1 := proto2.NewFrameWith([]byte("value-1"), proto2.BeginDataStructureFlag)
	frame2 := proto2.NewFrameWith([]byte("value-2"), proto2.EndDataStructureFlag)
	frame3 := proto2.NewFrameWith([]byte("value-3"), proto2.EndDataStructureFlag)

	message := proto2.NewClientMessage(frame1)
	message.AddFrame(frame2)
	message.AddFrame(frame3)

	//when
	iterator := message.FrameIterator()
	CodecUtil.FastForwardToEndFrame(iterator)

	//then
	assert.False(t, iterator.HasNext())
}

func TestCodecUtil_EncodeNullable(t *testing.T) {
	//given
	frame1 := proto2.NewFrame([]byte("value-0"))
	message := proto2.NewClientMessage(frame1)

	//when
	CodecUtil.EncodeNullable(message, "encode-value-1", EncodeString)

	//then
	iterator := message.FrameIterator()
	assert.Equal(t, string(iterator.Next().Content), "value-0")
	assert.Equal(t, string(iterator.Next().Content), "encode-value-1")
	assert.Equal(t, string(message.Frames[len(message.Frames)-1].Content), "encode-value-1")
}

func TestCodecUtil_NextFrameIsDataStructureEndFrame(t *testing.T) {
	//given
	byteValue := []byte("value-0")
	flags := uint16(proto2.EndDataStructureFlag)
	frame := proto2.NewFrameWith(byteValue, flags)
	message := proto2.NewClientMessage(frame)
	//when
	frameIterator := message.FrameIterator()
	isEndFrame := CodecUtil.NextFrameIsDataStructureEndFrame(frameIterator)

	//then
	assert.True(t, isEndFrame)
}

func TestCodecUtil_NextFrameIsNullFrame(t *testing.T) {
	//given
	nullFrame := proto2.NullFrame
	message := proto2.NewClientMessage(nullFrame)
	frame := proto2.NewFrame([]byte("value-0"))
	message.AddFrame(frame)
	frameIterator := message.FrameIterator()

	//when
	isNextFrameIsNull := CodecUtil.NextFrameIsNullFrame(frameIterator)

	//then
	assert.True(t, isNextFrameIsNull)
}

func TestCodecUtil_NextFrameIsNullFrame_Return_False_When_Next_Frame_Is_Not_Null(t *testing.T) {
	//given
	frame1 := proto2.NewFrame([]byte("value-1"))
	frame2 := proto2.NewFrame([]byte("value-2"))
	message := proto2.NewClientMessage(frame1)
	message.AddFrame(frame2)
	frameIterator := message.FrameIterator()

	//when
	isNextFrameIsNull := CodecUtil.NextFrameIsNullFrame(frameIterator)

	//then
	assert.False(t, isNextFrameIsNull)
}

func TestByteArrayCodec_Encode(t *testing.T) {
	//given
	value := []byte("value-1")
	message := proto2.NewClientMessageForEncode()

	//when
	EncodeByteArray(message, value)

	//then
	iterator := message.FrameIterator()
	assert.Equal(t, string(iterator.Next().Content), "value-1")
}

func TestByteArrayCodec_Decode(t *testing.T) {
	//given
	value := []byte("value-1")
	message := proto2.NewClientMessage(proto2.NewFrame(value))

	//when
	decode := DecodeByteArray(message.FrameIterator())

	//then
	assert.Equal(t, string(decode), "value-1")
}

func TestCodecUtil_EncodeNullable_If_Value_Is_Null_Add_Null_Frame_To_Message(t *testing.T) {
	//given
	frame1 := proto2.NewFrame([]byte("value-0"))
	message := proto2.NewClientMessage(frame1)

	//when
	CodecUtil.EncodeNullable(message, nil, EncodeString)

	//then
	iterator := message.FrameIterator()
	assert.Equal(t, string(iterator.Next().Content), "value-0")
	assert.True(t, iterator.Next().IsNullFrame())
}

func TestDataCodec_EncodeNullable_When_Data_Is_Nil(t *testing.T) {
	//given
	message := proto2.NewClientMessageForEncode()

	//when
	EncodeNullableData(message, nil)

	//then
	iterator := message.FrameIterator()
	assert.Len(t, iterator.Next().Content, 0)
}

func TestDataCodec_Decode(t *testing.T) {
	//given
	bytes := []byte("value-0")
	frame := proto2.NewFrame(bytes)
	message := proto2.NewClientMessageForDecode(frame)
	frameIterator := message.FrameIterator()

	//when
	decode := DecodeNullableData(frameIterator)

	//then
	assert.Equal(t, decode.ToByteArray(), bytes)
}

func TestDataCodec_Decode_When_Data_Is_Nil(t *testing.T) {
	//given
	nullFrame := proto2.NullFrame
	message := proto2.NewClientMessage(nullFrame)
	frameIterator := message.FrameIterator()

	//when
	decode := DecodeNullableData(frameIterator)

	//then
	assert.Nil(t, decode)
}

func TestEntryListCodec_Encode(t *testing.T) {
	//given
	bytes := []byte("value-0")
	frame := proto2.NewFrame(bytes)
	message := proto2.NewClientMessageForDecode(frame)
	pairs := make([]proto2.Pair, 0)
	pairs = append(pairs, proto2.NewPair("key", "value"))

	//when
	EncodeEntryList(message, pairs, EncodeString, EncodeString)

	//then
	frameIterator := message.FrameIterator()
	assert.Equal(t, string(frameIterator.Next().Content), "value-0")

	beginFrame := frameIterator.Next()
	assert.Empty(t, beginFrame.Content)
	assert.True(t, beginFrame.IsBeginFrame())

	assert.Equal(t, string(frameIterator.Next().Content), "key")
	assert.Equal(t, string(frameIterator.Next().Content), "value")

	endFrame := frameIterator.Next()
	assert.Empty(t, endFrame.Content)
	assert.True(t, endFrame.IsEndFrame())
}

func TestEntryListCodec_Encode_When_Entries_Is_Empty(t *testing.T) {
	//given
	bytes := []byte("value-0")
	frame := proto2.NewFrame(bytes)
	message := proto2.NewClientMessageForDecode(frame)
	pairs := make([]proto2.Pair, 0)

	//when
	EncodeEntryList(message, pairs, EncodeString, EncodeString)

	//then
	frameIterator := message.FrameIterator()
	assert.Equal(t, string(frameIterator.Next().Content), "value-0")

	beginFrame := frameIterator.Next()
	assert.Empty(t, beginFrame.Content)
	assert.True(t, beginFrame.IsBeginFrame())

	endFrame := frameIterator.Next()
	assert.Empty(t, endFrame.Content)
	assert.True(t, endFrame.IsEndFrame())
}

func TestEntryListCodec_EncodeNullable(t *testing.T) {
	//given
	bytes := []byte("value-0")
	frame := proto2.NewFrame(bytes)
	message := proto2.NewClientMessageForDecode(frame)
	pairs := make([]proto2.Pair, 0)
	pairs = append(pairs, proto2.NewPair("key", "value"))

	//when
	EncodeNullableEntryList(message, pairs, EncodeString, EncodeString)

	//then
	frameIterator := message.FrameIterator()
	assert.Equal(t, string(frameIterator.Next().Content), "value-0")

	beginFrame := frameIterator.Next()
	assert.Empty(t, beginFrame.Content)
	assert.True(t, beginFrame.IsBeginFrame())

	assert.Equal(t, string(frameIterator.Next().Content), "key")
	assert.Equal(t, string(frameIterator.Next().Content), "value")

	endFrame := frameIterator.Next()
	assert.Empty(t, endFrame.Content)
	assert.True(t, endFrame.IsEndFrame())
}

func TestEntryListCodec_EncodeNullable_When_Entries_Is_Empty(t *testing.T) {
	//given
	message := proto2.NewClientMessageForEncode()
	pairs := make([]proto2.Pair, 0)

	//when
	EncodeNullableEntryList(message, pairs, EncodeString, EncodeString)

	//then
	frameIterator := message.FrameIterator()
	assert.True(t, frameIterator.Next().IsNullFrame())
}

func TestEntryListCodec_DecodeNullable(t *testing.T) {
	//given
	nullFrame := proto2.NullFrame
	message := proto2.NewClientMessageForDecode(nullFrame)
	iterator := message.FrameIterator()

	//when
	results := DecodeNullableEntryList(iterator, DecodeData, DecodeData)

	//then
	assert.Nil(t, results)
}

func TestEntryListCodec_DecodeNullable_When_Next_Frame_Is_Null_Frame(t *testing.T) {
	//given
	message := proto2.NewClientMessageForDecode(proto2.NullFrame)
	iterator := message.FrameIterator()

	//when
	results := DecodeNullableEntryList(iterator, DecodeData, DecodeData)

	//then
	assert.Empty(t, results)
}

func TestFixSizedTypesCodec_EncodeInt(t *testing.T) {
	//given
	buffer := make([]byte, 4)
	offset := int32(0)
	value := int32(100)

	//when
	FixSizedTypesCodec.EncodeInt(buffer, offset, value)

	//then
	assert.Equal(t, binary.LittleEndian.Uint32(buffer), uint32(100))
}

func TestFixSizedTypesCodec_DecodeInt(t *testing.T) {
	//given
	buffer := make([]byte, 4)
	offset := int32(0)
	value := int32(100)
	FixSizedTypesCodec.EncodeInt(buffer, offset, value)

	//when
	decodeInt := FixSizedTypesCodec.DecodeInt(buffer, offset)

	//then
	assert.Equal(t, decodeInt, value)
}

func TestFixSizedTypesCodec_EncodeLong(t *testing.T) {
	//given
	buffer := make([]byte, 8)
	offset := int32(0)
	value := int64(100000000000000000)

	//when
	FixSizedTypesCodec.EncodeLong(buffer, offset, value)

	//then
	assert.Equal(t, int64(binary.LittleEndian.Uint64(buffer[offset:])), value)
}

func TestFixSizedTypesCodec_DecodeLong(t *testing.T) {
	//given
	buffer := make([]byte, 8)
	offset := int32(0)
	value := int64(100000000000000000)
	FixSizedTypesCodec.EncodeLong(buffer, offset, value)

	//when
	decodeLong := FixSizedTypesCodec.DecodeLong(buffer, offset)

	//then
	assert.Equal(t, decodeLong, value)
}

func TestFixSizedTypesCodec_EncodeBool(t *testing.T) {
	//given
	buffer := make([]byte, 1)
	offset := int32(0)

	//when
	FixSizedTypesCodec.EncodeBoolean(buffer, offset, true)

	//then
	assert.True(t, buffer[offset] == 1)
}

func TestFixSizedTypesCodec_EncodeBool_When_Value_Is_False(t *testing.T) {
	//given
	buffer := make([]byte, 1)
	offset := int32(0)

	//when
	FixSizedTypesCodec.EncodeBoolean(buffer, offset, false)

	//then
	assert.True(t, buffer[offset] == 0)
}

func TestFixSizedTypesCodec_EncodeByte(t *testing.T) {
	//given
	buffer := make([]byte, 1)
	offset := int32(0)

	//when
	FixSizedTypesCodec.EncodeByte(buffer, offset, 'b')

	//then
	assert.Equal(t, string(buffer[0]), "b")
}

func TestFixSizedTypesCodec_DecodeByte(t *testing.T) {
	//given
	buffer := make([]byte, 1)
	offset := int32(0)
	FixSizedTypesCodec.EncodeByte(buffer, offset, 'b')

	//when
	decodeByte := FixSizedTypesCodec.DecodeByte(buffer, offset)

	//then
	assert.Equal(t, string(decodeByte), "b")
}

func TestFixSizedTypesCodec_EncodeUUID(t *testing.T) {
	//given
	buffer := make([]byte, proto2.UUIDSizeInBytes)
	offset := int32(0)
	uuid := types.NewUUID()

	//when
	FixSizedTypesCodec.EncodeUUID(buffer, offset, uuid)

	//then
	assert.Equal(t, FixSizedTypesCodec.DecodeBoolean(buffer, offset), false)
	assert.Equal(t, FixSizedTypesCodec.DecodeLong(buffer, offset+proto2.BooleanSizeInBytes), int64(uuid.MostSignificantBits()))
	assert.Equal(t, FixSizedTypesCodec.DecodeLong(buffer, offset+proto2.BooleanSizeInBytes+proto2.LongSizeInBytes), int64(uuid.LeastSignificantBits()))
}

func TestFixSizedTypesCodec_EncodeUUID_When_UUID_Is_Nil(t *testing.T) {
	//given
	buffer := make([]byte, proto2.UUIDSizeInBytes)
	offset := int32(0)

	//when
	FixSizedTypesCodec.EncodeUUID(buffer, offset, types.UUID{})

	//then
	assert.Equal(t, FixSizedTypesCodec.DecodeBoolean(buffer, offset), true)
}

func TestFixSizedTypesCodec_DecodeUUID(t *testing.T) {
	//given
	buffer := make([]byte, proto2.UUIDSizeInBytes)
	offset := int32(0)
	uuid := types.NewUUID()
	FixSizedTypesCodec.EncodeUUID(buffer, offset, uuid)

	//when
	decodeUUID := FixSizedTypesCodec.DecodeUUID(buffer, offset)

	//then
	assert.Equal(t, FixSizedTypesCodec.DecodeBoolean(buffer, offset), false)
	assert.Equal(t, uuid, decodeUUID)
	assert.Equal(t, uuid.String(), decodeUUID.String())
	assert.Equal(t, uuid.MostSignificantBits(), decodeUUID.MostSignificantBits())
	assert.Equal(t, uuid.LeastSignificantBits(), decodeUUID.LeastSignificantBits())
}

func TestEntryListUUIDLongCodec_Encode(t *testing.T) {
	// given
	message := proto2.NewClientMessageForEncode()
	key := types.NewUUID()
	value := int64(100)
	pairs := make([]proto2.Pair, 0)
	pairs = append(pairs, proto2.NewPair(key, value))
	EncodeEntryListUUIDLong(message, pairs)

	// when
	pairs = DecodeEntryListUUIDLong(message.FrameIterator())

	// then
	frame := pairs[0]
	assert.Equal(t, frame.Key().(types.UUID).String(), key.String())
	assert.Equal(t, frame.Value().(int64), value)
}

func TestListUUIDCodec_Encode(t *testing.T) {
	// given
	message := proto2.NewClientMessageForEncode()
	entries := make([]types.UUID, 0)
	value1 := types.NewUUID()
	value2 := types.NewUUID()
	entries = append(entries, value1, value2)

	// when
	EncodeListUUID(message, entries)

	// then
	frame := message.FrameIterator().Next()
	decodeUUID1 := FixSizedTypesCodec.DecodeUUID(frame.Content, 0)
	assert.Equal(t, value1.String(), decodeUUID1.String())
	decodeUUID2 := FixSizedTypesCodec.DecodeUUID(frame.Content, 17)
	assert.Equal(t, value2.String(), decodeUUID2.String())
}

func TestListIntegerCodec_Encode(t *testing.T) {
	// given
	clientMessage := proto2.NewClientMessageForEncode()
	entries := make([]int32, 0)
	entries = append(entries, 1, 2, 3)

	// when
	EncodeListInteger(clientMessage, entries)

	// then
	frame := clientMessage.FrameIterator().Next()
	assert.Equal(t, FixSizedTypesCodec.DecodeInt(frame.Content, 0), int32(1))
	assert.Equal(t, FixSizedTypesCodec.DecodeInt(frame.Content, 4), int32(2))
	assert.Equal(t, FixSizedTypesCodec.DecodeInt(frame.Content, 8), int32(3))
}

func TestListIntegerCodec_Decode(t *testing.T) {
	// given
	clientMessage := proto2.NewClientMessageForEncode()
	entries := make([]int32, 0)
	entries = append(entries, 1, 2, 3)
	EncodeListInteger(clientMessage, entries)

	// when
	decodeEntries := DecodeListInteger(clientMessage.FrameIterator())

	// then
	assert.Equal(t, decodeEntries[0], int32(1))
	assert.Equal(t, decodeEntries[1], int32(2))
	assert.Equal(t, decodeEntries[2], int32(3))
}

func TestListLongCodec_Encode(t *testing.T) {
	// given
	message := proto2.NewClientMessageForEncode()
	entries := make([]int64, 0)
	entries = append(entries, 1, 2, 3)

	// when
	EncodeListLong(message, entries)

	// then
	frame := message.FrameIterator().Next()
	assert.Equal(t, FixSizedTypesCodec.DecodeLong(frame.Content, 0), int64(1))
	assert.Equal(t, FixSizedTypesCodec.DecodeLong(frame.Content, 8), int64(2))
	assert.Equal(t, FixSizedTypesCodec.DecodeLong(frame.Content, 16), int64(3))
}

func TestListLongCodec_Decode(t *testing.T) {
	// given
	message := proto2.NewClientMessageForEncode()
	entries := make([]int64, 0)
	entries = append(entries, 1, 2, 3)
	EncodeListLong(message, entries)

	// when
	result := DecodeListLong(message.FrameIterator())

	// then
	assert.Equal(t, result, entries)
}

func TestEntryListUUIDListIntegerCodec_Encode(t *testing.T) {
	// given
	clientMessage := proto2.NewClientMessageForEncode()
	key := types.NewUUID()
	value := make([]int32, 0)
	value = append(value, 1, 2, 3)
	pair := proto2.NewPair(key, value)
	entries := make([]proto2.Pair, 0)
	entries = append(entries, pair)

	// when
	EncodeEntryListUUIDListInteger(clientMessage, entries)

	// then
	iterator := clientMessage.FrameIterator()
	assert.Equal(t, iterator.Next().IsBeginFrame(), true)
	integerValues := iterator.Next()
	assert.Equal(t, FixSizedTypesCodec.DecodeInt(integerValues.Content, 0), int32(1))
	assert.Equal(t, FixSizedTypesCodec.DecodeInt(integerValues.Content, 4), int32(2))
	assert.Equal(t, FixSizedTypesCodec.DecodeInt(integerValues.Content, 8), int32(3))
	assert.Equal(t, iterator.Next().IsEndFrame(), true)
	uuid := FixSizedTypesCodec.DecodeUUID(iterator.Next().Content, 0)
	assert.Equal(t, uuid.String(), key.String())
}

func TestEntryListUUIDListIntegerCodec_Decode(t *testing.T) {
	// given
	clientMessage := proto2.NewClientMessageForEncode()
	key := types.NewUUID()
	value := make([]int32, 0)
	value = append(value, 1, 2, 3)
	pair := proto2.NewPair(key, value)
	entries := make([]proto2.Pair, 0)
	entries = append(entries, pair)
	EncodeEntryListUUIDListInteger(clientMessage, entries)

	// when
	result := DecodeEntryListUUIDListInteger(clientMessage.FrameIterator())

	// then
	assert.Equal(t, len(result), 1)
	assert.Equal(t, result[0].Key().([]types.UUID)[0].String(), key.String())
	assert.EqualValues(t, result[0].Value().([]int32), value)
}

func TestLongArrayCodec_Encode(t *testing.T) {
	// given
	clientMessage := proto2.NewClientMessageForEncode()
	entries := make([]int64, 0)
	entries = append(entries, 1, 2, 3)

	// when
	EncodeLongArray(clientMessage, entries)

	// then
	frame := clientMessage.FrameIterator().Next()
	assert.Equal(t, FixSizedTypesCodec.DecodeLong(frame.Content, 0), int64(1))
	assert.Equal(t, FixSizedTypesCodec.DecodeLong(frame.Content, 8), int64(2))
	assert.Equal(t, FixSizedTypesCodec.DecodeLong(frame.Content, 16), int64(3))
}

func TestLongArrayCodec_Decode(t *testing.T) {
	// given
	clientMessage := proto2.NewClientMessageForEncode()
	entries := make([]int64, 0)
	entries = append(entries, 1, 2, 3)
	EncodeLongArray(clientMessage, entries)

	// when
	result := DecodeLongArray(clientMessage.FrameIterator())

	// then
	assert.Equal(t, result[0], int64(1))
	assert.Equal(t, result[1], int64(2))
	assert.Equal(t, result[2], int64(3))
}

func TestStringCodec_Encode(t *testing.T) {
	//given

	value := "value-encode"
	frame := proto2.NewFrame([]byte(""))
	clientMessage := proto2.NewClientMessage(frame)

	//when
	EncodeString(clientMessage, value)

	//then
	content := clientMessage.Frames[len(clientMessage.Frames)-1].Content
	assert.Equal(t, value, string(content))
}
