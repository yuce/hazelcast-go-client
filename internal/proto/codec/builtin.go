package codec

import (
	"encoding/binary"
	"github.com/hazelcast/hazelcast-go-client/v4/hazelcast/hzerror"
	"github.com/hazelcast/hazelcast-go-client/v4/hazelcast/hztypes"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/sql"
	"math"
	"strings"

	pubcluster "github.com/hazelcast/hazelcast-go-client/v4/hazelcast/cluster"
	"github.com/hazelcast/hazelcast-go-client/v4/internal"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/serialization"
	"github.com/hazelcast/hazelcast-go-client/v4/internal/serialization/spi"
)

// Encoder for ClientMessage and value
type Encoder func(message *proto.ClientMessage, value interface{})

// Decoder create serialization.Data
type Decoder func(frameIterator *proto.ForwardFrameIterator) serialization.Data

// CodecUtil
type codecUtil struct{}

var CodecUtil codecUtil

func (codecUtil) DecodeNullableForSqlError(frameIterator *proto.ForwardFrameIterator) sql.Error {
	if CodecUtil.NextFrameIsNullFrame(frameIterator) {
		return sql.Error{}
	}
	return DecodeSqlError(frameIterator)
}

func (codecUtil) DecodeNullableForSqlPage(frameIterator *proto.ForwardFrameIterator) sql.Page {
	if CodecUtil.NextFrameIsNullFrame(frameIterator) {
		return sql.Page{}
	}
	return DecodeSqlPage(frameIterator)
}

func (codecUtil) FastForwardToEndFrame(frameIterator *proto.ForwardFrameIterator) {
	numberOfExpectedEndFrames := 1
	var frame *proto.Frame
	for numberOfExpectedEndFrames != 0 {
		frame = frameIterator.Next()
		if frame.IsEndFrame() {
			numberOfExpectedEndFrames--
		} else if frame.IsBeginFrame() {
			numberOfExpectedEndFrames++
		}
	}
}

func (codecUtil) EncodeNullable(message *proto.ClientMessage, value interface{}, encoder Encoder) {
	if value == nil {
		message.AddFrame(proto.NullFrame.Copy())
	} else {
		encoder(message, value)
	}
}

func (codecUtil) EncodeNullableForString(message *proto.ClientMessage, value string) {
	if strings.TrimSpace(value) == "" {
		message.AddFrame(proto.NullFrame.Copy())
	} else {
		EncodeString(message, value)
	}
}

func (codecUtil) EncodeNullableForBitmapIndexOptions(message *proto.ClientMessage, options hztypes.BitmapIndexOptions) {
	if options.IsDefault() {
		message.AddFrame(proto.NullFrame.Copy())
	} else {
		EncodeBitmapIndexOptions(message, options)
	}
}

func (codecUtil) EncodeNullableForData(message *proto.ClientMessage, data serialization.Data) {
	if data == nil {
		message.AddFrame(proto.NullFrame.Copy())
	} else {
		EncodeData(message, data)
	}
}

func (codecUtil) DecodeNullableForData(frameIterator *proto.ForwardFrameIterator) serialization.Data {
	if CodecUtil.NextFrameIsNullFrame(frameIterator) {
		return nil
	}
	return DecodeData(frameIterator)
}

func (codecUtil) DecodeNullableForAddress(frameIterator *proto.ForwardFrameIterator) pubcluster.Address {
	if CodecUtil.NextFrameIsNullFrame(frameIterator) {
		return nil
	}
	return DecodeAddress(frameIterator)
}

func (codecUtil) DecodeNullableForLongArray(frameIterator *proto.ForwardFrameIterator) []int64 {
	if CodecUtil.NextFrameIsNullFrame(frameIterator) {
		return nil
	}
	return DecodeLongArray(frameIterator)
}

func (codecUtil) DecodeNullableForString(frameIterator *proto.ForwardFrameIterator) string {
	if CodecUtil.NextFrameIsNullFrame(frameIterator) {
		return ""
	}
	return DecodeString(frameIterator)
}

func (codecUtil) NextFrameIsDataStructureEndFrame(frameIterator *proto.ForwardFrameIterator) bool {
	return frameIterator.PeekNext().IsEndFrame()
}

func (codecUtil) NextFrameIsNullFrame(frameIterator *proto.ForwardFrameIterator) bool {
	isNullFrame := frameIterator.PeekNext().IsNullFrame()
	if isNullFrame {
		frameIterator.Next()
	}
	return isNullFrame
}

func (codecUtil) DecodeNullableForBitmapIndexOptions(frameIterator *proto.ForwardFrameIterator) hztypes.BitmapIndexOptions {
	isNullFrame := frameIterator.PeekNext().IsNullFrame()
	if isNullFrame {
		frameIterator.Next()
	}
	return DecodeBitmapIndexOptions(frameIterator)
}

func (codecUtil) DecodeNullableForSimpleEntryView(frameIterator *proto.ForwardFrameIterator) *internal.SimpleEntryView {
	isNullFrame := frameIterator.PeekNext().IsNullFrame()
	if isNullFrame {
		frameIterator.Next()
	}
	return DecodeSimpleEntryView(frameIterator)
}

func EncodeByteArray(message *proto.ClientMessage, value []byte) {
	message.AddFrame(proto.NewFrame(value))
}

func DecodeByteArray(frameIterator *proto.ForwardFrameIterator) []byte {
	return frameIterator.Next().Content
}

func EncodeData(message *proto.ClientMessage, value interface{}) {
	message.AddFrame(proto.NewFrame(value.(serialization.Data).ToByteArray()))
}

func EncodeNullableData(message *proto.ClientMessage, data interface{}) {
	if data == nil {
		message.AddFrame(proto.NullFrame.Copy())
	} else {
		message.AddFrame(proto.NewFrame(data.(serialization.Data).ToByteArray()))
	}
}

func DecodeData(frameIterator *proto.ForwardFrameIterator) serialization.Data {
	return spi.NewData(frameIterator.Next().Content)
}

func DecodeNullableData(frameIterator *proto.ForwardFrameIterator) serialization.Data {
	if CodecUtil.NextFrameIsNullFrame(frameIterator) {
		return nil
	}
	return DecodeData(frameIterator)
}

func EncodeEntryList(message *proto.ClientMessage, entries []proto.Pair, keyEncoder, valueEncoder Encoder) {
	message.AddFrame(proto.BeginFrame.Copy())
	for _, value := range entries {
		keyEncoder(message, value.Key())
		valueEncoder(message, value.Value())
	}
	message.AddFrame(proto.EndFrame.Copy())
}

func EncodeEntryListForStringAndString(message *proto.ClientMessage, entries []proto.Pair) {
	message.AddFrame(proto.BeginFrame.Copy())
	for _, value := range entries {
		EncodeString(message, value.Key())
		EncodeString(message, value.Value())
	}
	message.AddFrame(proto.EndFrame.Copy())
}

func EncodeEntryListForStringAndByteArray(message *proto.ClientMessage, entries []proto.Pair) {
	message.AddFrame(proto.BeginFrame.Copy())
	for _, value := range entries {
		EncodeString(message, value.Key())
		EncodeByteArray(message, value.Value().([]byte))
	}
	message.AddFrame(proto.EndFrame.Copy())

}

func EncodeEntryListForDataAndData(message *proto.ClientMessage, entries []proto.Pair) {
	message.AddFrame(proto.BeginFrame.Copy())
	for _, value := range entries {
		EncodeData(message, value.Key())
		EncodeData(message, value.Value())
	}
	message.AddFrame(proto.EndFrame.Copy())
}

func EncodeEntryListForDataAndListData(message *proto.ClientMessage, entries []proto.Pair) {
	message.AddFrame(proto.BeginFrame.Copy())
	for _, value := range entries {
		EncodeData(message, value.Key())
		EncodeListData(message, value.Value().([]serialization.Data))
	}
	message.AddFrame(proto.EndFrame.Copy())
}

func EncodeNullableEntryList(message *proto.ClientMessage, entries []proto.Pair, keyEncoder, valueEncoder Encoder) {
	if len(entries) == 0 {
		message.AddFrame(proto.NullFrame.Copy())
	} else {
		EncodeEntryList(message, entries, keyEncoder, valueEncoder)
	}
}

func DecodeEntryList(frameIterator *proto.ForwardFrameIterator, keyDecoder, valueDecoder Decoder) []proto.Pair {
	result := make([]proto.Pair, 0)
	frameIterator.Next()
	for !CodecUtil.NextFrameIsDataStructureEndFrame(frameIterator) {
		key := keyDecoder(frameIterator)
		value := valueDecoder(frameIterator)
		result = append(result, proto.NewPair(key, value))
	}
	frameIterator.Next()
	return result
}

func DecodeNullableEntryList(frameIterator *proto.ForwardFrameIterator, keyDecoder, valueDecoder Decoder) []proto.Pair {
	if CodecUtil.NextFrameIsNullFrame(frameIterator) {
		return nil
	}
	return DecodeEntryList(frameIterator, keyDecoder, valueDecoder)
}

func DecodeEntryListForStringAndEntryListIntegerLong(frameIterator *proto.ForwardFrameIterator) []proto.Pair {
	result := make([]proto.Pair, 0)
	frameIterator.Next()
	for !CodecUtil.NextFrameIsDataStructureEndFrame(frameIterator) {
		key := DecodeString(frameIterator)
		value := DecodeEntryListIntegerLong(frameIterator)
		result = append(result, proto.NewPair(key, value))
	}
	frameIterator.Next()
	return result
}

func DecodeEntryListForDataAndData(frameIterator *proto.ForwardFrameIterator) []proto.Pair {
	result := make([]proto.Pair, 0)
	frameIterator.Next()
	for !CodecUtil.NextFrameIsDataStructureEndFrame(frameIterator) {
		key := DecodeData(frameIterator)
		value := DecodeData(frameIterator)
		result = append(result, proto.NewPair(key, value))
	}
	frameIterator.Next()
	return result
}

func EncodeListIntegerIntegerInteger(message *proto.ClientMessage, entries []proto.Pair) {
	entryCount := len(entries)
	frame := proto.NewFrame(make([]byte, entryCount*proto.EntrySizeInBytes))
	for i := 0; i < entryCount; i++ {
		FixSizedTypesCodec.EncodeInt(frame.Content, int32(i*proto.EntrySizeInBytes+proto.IntSizeInBytes), entries[i].Value().(int32))
		FixSizedTypesCodec.EncodeInt(frame.Content, int32(i*proto.EntrySizeInBytes), entries[i].Key().(int32))
	}
	message.AddFrame(frame)
}

func DecodeListIntegerIntegerInteger(frameIterator *proto.ForwardFrameIterator) []proto.Pair {
	frame := frameIterator.Next()
	itemCount := len(frame.Content) / proto.EntrySizeInBytes
	result := make([]proto.Pair, itemCount)
	for i := 0; i < itemCount; i++ {
		key := FixSizedTypesCodec.DecodeInt(frame.Content, int32(i*proto.EntrySizeInBytes))
		value := FixSizedTypesCodec.DecodeInt(frame.Content, int32(i*proto.EntrySizeInBytes+proto.IntSizeInBytes))
		result = append(result, proto.NewPair(key, value))
	}
	return result
}

func EncodeEntryListUUIDLong(message *proto.ClientMessage, entries []proto.Pair) {
	size := len(entries)
	content := make([]byte, size*proto.EntrySizeInBytes)
	newFrame := proto.NewFrame(content)
	for i, entry := range entries {
		key := entry.Key().(internal.UUID)
		value := entry.Value().(int64)
		FixSizedTypesCodec.EncodeUUID(content, int32(i*proto.EntrySizeInBytes), key)
		FixSizedTypesCodec.EncodeLong(content, int32(i*proto.EntrySizeInBytes+proto.UUIDSizeInBytes), value)
	}
	message.AddFrame(newFrame)
}

func EncodeEntryListIntegerInteger(message *proto.ClientMessage, entries []proto.Pair) {
	size := len(entries)
	content := make([]byte, size*proto.EntrySizeInBytes)
	newFrame := proto.NewFrame(content)
	for i, entry := range entries {
		key := entry.Key().(int32)
		value := entry.Value().(int32)
		FixSizedTypesCodec.EncodeInt(content, int32(i*proto.EntrySizeInBytes), key)
		FixSizedTypesCodec.EncodeInt(content, int32(i*proto.EntrySizeInBytes+proto.UUIDSizeInBytes), value)
	}
	message.AddFrame(newFrame)
}

func DecodeEntryListUUIDLong(frameIterator *proto.ForwardFrameIterator) []proto.Pair {
	nextFrame := frameIterator.Next()
	itemCount := len(nextFrame.Content) / proto.EntrySizeInBytes
	content := make([]proto.Pair, itemCount)
	for i := 0; i < itemCount; i++ {
		uuid := FixSizedTypesCodec.DecodeUUID(nextFrame.Content, int32(i*proto.EntrySizeInBytes))
		value := FixSizedTypesCodec.DecodeLong(nextFrame.Content, int32(i*proto.EntrySizeInBytes+proto.UUIDSizeInBytes))
		content[i] = proto.NewPair(uuid, value)
	}
	return content
}

func DecodeEntryListIntegerInteger(frameIterator *proto.ForwardFrameIterator) []proto.Pair {
	nextFrame := frameIterator.Next()
	itemCount := len(nextFrame.Content) / proto.EntrySizeInBytes
	content := make([]proto.Pair, itemCount)
	for i := 0; i < itemCount; i++ {
		key := FixSizedTypesCodec.DecodeInt(nextFrame.Content, int32(i*proto.EntrySizeInBytes))
		value := FixSizedTypesCodec.DecodeInt(nextFrame.Content, int32(i*proto.EntrySizeInBytes+proto.IntSizeInBytes))
		content[i] = proto.NewPair(key, value)
	}
	return content
}

func EncodeEntryListUUIDListInteger(message *proto.ClientMessage, entries []proto.Pair) {
	entryCount := len(entries)
	uuids := make([]internal.UUID, entryCount)
	message.AddFrame(proto.BeginFrame)
	for i := 0; i < entryCount; i++ {
		entry := entries[i]
		key := entry.Key().(internal.UUID)
		value := entry.Value().([]int32)
		uuids[i] = key
		EncodeListInteger(message, value)
	}
	message.AddFrame(proto.EndFrame)
	EncodeListUUID(message, uuids)
}

func DecodeEntryListUUIDListInteger(frameIterator *proto.ForwardFrameIterator) []proto.Pair {
	values := DecodeListMultiFrameWithListInteger(frameIterator)
	keys := DecodeListUUID(frameIterator)
	keySize := len(keys)
	result := make([]proto.Pair, keySize)
	for i := 0; i < keySize; i++ {
		result[i] = proto.NewPair(keys, values)
	}
	return result
}

func DecodeEntryListIntegerUUID(frameIterator *proto.ForwardFrameIterator) []proto.Pair {
	frame := frameIterator.Next()
	entryCount := len(frame.Content) / proto.EntrySizeInBytes
	result := make([]proto.Pair, entryCount)
	for i := 0; i < entryCount; i++ {
		key := FixSizedTypesCodec.DecodeInt(frame.Content, int32(i*proto.EntrySizeInBytes))
		value := FixSizedTypesCodec.DecodeUUID(frame.Content, int32(i*proto.EntrySizeInBytes+proto.IntSizeInBytes))
		result[i] = proto.NewPair(key, value)
	}
	return result
}

func DecodeEntryListIntegerLong(iterator *proto.ForwardFrameIterator) []proto.Pair {
	frame := iterator.Next()
	entryCount := len(frame.Content) / proto.EntrySizeInBytes
	result := make([]proto.Pair, entryCount)
	for i := 0; i < entryCount; i++ {
		key := FixSizedTypesCodec.DecodeInt(frame.Content, int32(i*proto.EntrySizeInBytes))
		value := FixSizedTypesCodec.DecodeLong(frame.Content, int32(i*proto.EntrySizeInBytes+proto.IntSizeInBytes))
		result[i] = proto.NewPair(key, value)
	}
	return result
}

// fixSizedTypesCodec
type fixSizedTypesCodec struct{}

var FixSizedTypesCodec fixSizedTypesCodec

func (fixSizedTypesCodec) EncodeInt(buffer []byte, offset int32, value interface{}) {
	binary.LittleEndian.PutUint32(buffer[offset:], uint32(value.(int32)))
}

func (fixSizedTypesCodec) DecodeInt(buffer []byte, offset int32) int32 {
	return int32(binary.LittleEndian.Uint32(buffer[offset:]))
}

func (fixSizedTypesCodec) EncodeShortInt(buffer []byte, offset int32, value interface{}) {
	binary.LittleEndian.PutUint16(buffer[offset:], uint16(value.(int16)))
}

func (fixSizedTypesCodec) DecodeShortInt(buffer []byte, offset int32) int16 {
	return int16(binary.LittleEndian.Uint16(buffer[offset:]))
}

func (fixSizedTypesCodec) EncodeFloat(buffer []byte, offset int32, value interface{}) {
	FixSizedTypesCodec.EncodeInt(buffer, offset, math.Float32bits(value.(float32)))
}

func (fixSizedTypesCodec) DecodeFloat(buffer []byte, offset int32) float32 {
	return math.Float32frombits(uint32(FixSizedTypesCodec.DecodeInt(buffer, offset)))
}

func (fixSizedTypesCodec) EncodeDouble(buffer []byte, offset int32, value interface{}) {
	FixSizedTypesCodec.EncodeLong(buffer, offset, math.Float64bits(value.(float64)))
}

func (fixSizedTypesCodec) DecodeDouble(buffer []byte, offset int32) float64 {
	return math.Float64frombits(uint64(FixSizedTypesCodec.DecodeLong(buffer, offset)))
}

func (fixSizedTypesCodec) EncodeLong(buffer []byte, offset int32, value interface{}) {
	binary.LittleEndian.PutUint64(buffer[offset:], uint64(value.(int64)))
}

func (fixSizedTypesCodec) DecodeLong(buffer []byte, offset int32) int64 {
	return int64(binary.LittleEndian.Uint64(buffer[offset:]))
}

func (fixSizedTypesCodec) EncodeBoolean(buffer []byte, offset int32, value interface{}) {
	if value.(bool) {
		buffer[offset] = 1
	} else {
		buffer[offset] = 0
	}
}

func (fixSizedTypesCodec) DecodeBoolean(buffer []byte, offset int32) bool {
	return buffer[offset] == 1
}

func (fixSizedTypesCodec) EncodeByte(buffer []byte, offset int32, value interface{}) {
	buffer[offset] = value.(byte)
}

func (fixSizedTypesCodec) DecodeByte(buffer []byte, offset int32) byte {
	return buffer[offset]
}

func (fixSizedTypesCodec) EncodeUUID(buffer []byte, offset int32, uuid internal.UUID) {
	isNullEncode := uuid == nil
	FixSizedTypesCodec.EncodeBoolean(buffer, offset, isNullEncode)
	if isNullEncode {
		return
	}
	bufferOffset := offset + proto.BooleanSizeInBytes
	FixSizedTypesCodec.EncodeLong(buffer, bufferOffset, int64(uuid.MostSignificantBits()))
	FixSizedTypesCodec.EncodeLong(buffer, bufferOffset+proto.LongSizeInBytes, int64(uuid.LeastSignificantBits()))
}

func (fixSizedTypesCodec) DecodeUUID(buffer []byte, offset int32) internal.UUID {
	isNull := FixSizedTypesCodec.DecodeBoolean(buffer, offset)
	if isNull {
		return nil
	}

	mostSignificantOffset := offset + proto.BooleanSizeInBytes
	leastSignificantOffset := mostSignificantOffset + proto.LongSizeInBytes
	mostSignificant := uint64(FixSizedTypesCodec.DecodeLong(buffer, mostSignificantOffset))
	leastSignificant := uint64(FixSizedTypesCodec.DecodeLong(buffer, leastSignificantOffset))

	return internal.NewUUIDWith(mostSignificant, leastSignificant)
}

func EncodeListInteger(message *proto.ClientMessage, entries []int32) {
	itemCount := len(entries)
	content := make([]byte, itemCount*proto.IntSizeInBytes)
	newFrame := proto.NewFrame(content)
	for i := 0; i < itemCount; i++ {
		FixSizedTypesCodec.EncodeInt(newFrame.Content, int32(i*proto.IntSizeInBytes), entries[i])
	}
	message.AddFrame(newFrame)
}

func DecodeListInteger(frameIterator *proto.ForwardFrameIterator) []int32 {
	frame := frameIterator.Next()
	itemCount := len(frame.Content) / proto.IntSizeInBytes
	result := make([]int32, itemCount)
	for i := 0; i < itemCount; i++ {
		result[i] = FixSizedTypesCodec.DecodeInt(frame.Content, int32(i*proto.IntSizeInBytes))
	}
	return result
}

func EncodeListLong(message *proto.ClientMessage, entries []int64) {
	itemCount := len(entries)
	frame := proto.NewFrame(make([]byte, itemCount*proto.LongSizeInBytes))
	for i := 0; i < itemCount; i++ {
		FixSizedTypesCodec.EncodeLong(frame.Content, int32(i*proto.LongSizeInBytes), entries[i])
	}
	message.AddFrame(frame)
}

func DecodeListLong(frameIterator *proto.ForwardFrameIterator) []int64 {
	frame := frameIterator.Next()
	itemCount := len(frame.Content) / proto.LongSizeInBytes
	result := make([]int64, itemCount)
	for i := 0; i < itemCount; i++ {
		result[i] = FixSizedTypesCodec.DecodeLong(frame.Content, int32(i*proto.LongSizeInBytes))
	}
	return result
}

func EncodeListMultiFrame(message *proto.ClientMessage, values []interface{}, encoder Encoder) {
	message.AddFrame(proto.BeginFrame)
	for i := 0; i < len(values); i++ {
		encoder(message, values[i])
	}
	message.AddFrame(proto.EndFrame)
}

func EncodeListMultiFrameForData(message *proto.ClientMessage, values []serialization.Data) {
	message.AddFrame(proto.BeginFrame)
	for i := 0; i < len(values); i++ {
		EncodeData(message, values[i])
	}
	message.AddFrame(proto.EndFrame)
}

func EncodeListMultiFrameForString(message *proto.ClientMessage, values []string) {
	message.AddFrame(proto.BeginFrame)
	for i := 0; i < len(values); i++ {
		EncodeString(message, values[i])
	}
	message.AddFrame(proto.EndFrame)
}

func EncodeListMultiFrameForStackTraceElement(message *proto.ClientMessage, values []proto.StackTraceElement) {
	message.AddFrame(proto.BeginFrame)
	for i := 0; i < len(values); i++ {
		EncodeStackTraceElement(message, values[i])
	}
	message.AddFrame(proto.EndFrame)
}

func EncodeListMultiFrameContainsNullable(message *proto.ClientMessage, values []interface{}, encoder Encoder) {
	message.AddFrame(proto.BeginFrame)
	for i := 0; i < len(values); i++ {
		if values[i] == nil {
			message.AddFrame(proto.NullFrame)
		} else {
			encoder(message, values[i])
		}
	}
	message.AddFrame(proto.EndFrame)
}

func EncodeListMultiFrameNullable(message *proto.ClientMessage, values []interface{}, encoder Encoder) {
	if len(values) == 0 {
		message.AddFrame(proto.NullFrame)
	} else {
		EncodeListMultiFrame(message, values, encoder)
	}
}

func DecodeListMultiFrameForData(frameIterator *proto.ForwardFrameIterator) []serialization.Data {
	result := make([]serialization.Data, 0)
	frameIterator.Next()
	for !CodecUtil.NextFrameIsDataStructureEndFrame(frameIterator) {
		result = append(result, DecodeData(frameIterator))
	}
	frameIterator.Next()
	return result
}

func DecodeListMultiFrameWithListInteger(frameIterator *proto.ForwardFrameIterator) []int32 {
	result := make([]int32, 0)
	frameIterator.Next()
	for !CodecUtil.NextFrameIsDataStructureEndFrame(frameIterator) {
		result = append(result, DecodeListInteger(frameIterator)...)
	}
	frameIterator.Next()
	return result
}

func DecodeListMultiFrameForDistributedObjectInfo(frameIterator *proto.ForwardFrameIterator) []internal.DistributedObjectInfo {
	result := make([]internal.DistributedObjectInfo, 0)
	frameIterator.Next()
	for !CodecUtil.NextFrameIsDataStructureEndFrame(frameIterator) {
		result = append(result, DecodeDistributedObjectInfo(frameIterator))
	}
	frameIterator.Next()
	return result
}

func DecodeListMultiFrameForMemberInfo(frameIterator *proto.ForwardFrameIterator) []pubcluster.MemberInfo {
	result := make([]pubcluster.MemberInfo, 0)
	frameIterator.Next()
	for !CodecUtil.NextFrameIsDataStructureEndFrame(frameIterator) {
		result = append(result, DecodeMemberInfo(frameIterator))
	}
	frameIterator.Next()
	return result
}

func DecodeListMultiFrameForStackTraceElement(frameIterator *proto.ForwardFrameIterator) []hzerror.StackTraceElement {
	result := make([]hzerror.StackTraceElement, 0)
	frameIterator.Next()
	for !CodecUtil.NextFrameIsDataStructureEndFrame(frameIterator) {
		result = append(result, DecodeStackTraceElement(frameIterator))
	}
	frameIterator.Next()
	return result
}

func DecodeListMultiFrameForString(frameIterator *proto.ForwardFrameIterator) []string {
	result := make([]string, 0)
	frameIterator.Next()
	for !CodecUtil.NextFrameIsDataStructureEndFrame(frameIterator) {
		result = append(result, DecodeString(frameIterator))
	}
	frameIterator.Next()
	return result
}

func DecodeListMultiFrameForDataContainsNullable(frameIterator *proto.ForwardFrameIterator) []serialization.Data {
	result := make([]serialization.Data, 0)
	frameIterator.Next()
	for !CodecUtil.NextFrameIsDataStructureEndFrame(frameIterator) {
		if CodecUtil.NextFrameIsNullFrame(frameIterator) {
			result = append(result, nil)
		} else {
			result = append(result, DecodeData(frameIterator))
		}
	}
	frameIterator.Next()
	return result
}

func EncodeListData(message *proto.ClientMessage, entries []serialization.Data) {
	EncodeListMultiFrameForData(message, entries)
}

func DecodeListData(frameIterator *proto.ForwardFrameIterator) []serialization.Data {
	return DecodeListMultiFrameForData(frameIterator)
}

func EncodeListUUID(message *proto.ClientMessage, entries []internal.UUID) {
	itemCount := len(entries)
	content := make([]byte, itemCount*proto.UUIDSizeInBytes)
	newFrame := proto.NewFrame(content)
	for i := 0; i < itemCount; i++ {
		FixSizedTypesCodec.EncodeUUID(content, int32(i*proto.UUIDSizeInBytes), entries[i])
	}
	message.AddFrame(newFrame)
}

func DecodeListUUID(frameIterator *proto.ForwardFrameIterator) []internal.UUID {
	frame := frameIterator.Next()
	itemCount := len(frame.Content) / proto.UUIDSizeInBytes
	result := make([]internal.UUID, itemCount)
	for i := 0; i < itemCount; i++ {
		result[i] = FixSizedTypesCodec.DecodeUUID(frame.Content, int32(i*proto.UUIDSizeInBytes))
	}
	return result
}

func EncodeLongArray(message *proto.ClientMessage, entries []int64) {
	itemCount := len(entries)
	frame := proto.NewFrame(make([]byte, itemCount*proto.LongSizeInBytes))
	for i := 0; i < itemCount; i++ {
		FixSizedTypesCodec.EncodeLong(frame.Content, int32(i*proto.LongSizeInBytes), entries[i])
	}
	message.AddFrame(frame)
}

func DecodeLongArray(frameIterator *proto.ForwardFrameIterator) []int64 {
	frame := frameIterator.Next()
	itemCount := len(frame.Content) / proto.LongSizeInBytes
	result := make([]int64, itemCount)
	for i := 0; i < itemCount; i++ {
		result[i] = FixSizedTypesCodec.DecodeLong(frame.Content, int32(i*proto.LongSizeInBytes))
	}
	return result
}

func EncodeMapForStringAndString(message *proto.ClientMessage, values map[string]string) {
	message.AddFrame(proto.BeginFrame.Copy())
	for key, value := range values {
		EncodeString(message, key)
		EncodeString(message, value)
	}
	message.AddFrame(proto.EndFrame.Copy())
}

func EncodeMapForEndpointQualifierAndAddress(message *proto.ClientMessage, values map[internal.EndpointQualifier]pubcluster.Address) {
	message.AddFrame(proto.BeginFrame.Copy())
	for key, value := range values {
		EncodeEndpointQualifier(message, key)
		EncodeAddress(message, value)
	}
	message.AddFrame(proto.EndFrame.Copy())
}

func DecodeMapForStringAndString(iterator *proto.ForwardFrameIterator) map[string]string {
	result := map[string]string{}
	iterator.Next()
	for !iterator.PeekNext().IsEndFrame() {
		key := DecodeString(iterator)
		value := DecodeString(iterator)
		result[key] = value
	}
	iterator.Next()
	return result
}

func DecodeMapForEndpointQualifierAndAddress(iterator *proto.ForwardFrameIterator) interface{} {
	result := map[internal.EndpointQualifier]pubcluster.Address{}
	iterator.Next()
	for !iterator.PeekNext().IsEndFrame() {
		key := DecodeEndpointQualifier(iterator)
		value := DecodeAddress(iterator)
		result[key] = value
	}
	iterator.Next()
	return result
}

func EncodeString(message *proto.ClientMessage, value interface{}) {
	message.AddFrame(proto.NewFrame([]byte(value.(string))))
}

func DecodeString(frameIterator *proto.ForwardFrameIterator) string {
	return string(frameIterator.Next().Content)
}

const (
	HEADER_SIZE             = proto.ByteSizeInBytes + proto.IntSizeInBytes
	ITEMS_PER_BITMASK       = 8
	TYPE_NULL_ONLY     byte = 1
	TYPE_NOT_NULL_ONLY byte = 2
	TYPE_MIXED         byte = 3
)

func GetFrameSizeForCNFixed(nonNullCount int, totalCount int, itemSizeInBytes int) int {
	var payload int
	if nonNullCount == 0 {
		// Only nulls. Write only size.
		payload = 0
	} else if nonNullCount == totalCount {
		// Only non-nulls. Write without a bitmask.
		payload = totalCount * itemSizeInBytes
	} else {
		// Mixed null and non-nulls. Write with a bitmask.
		nonNullItemCumulativeSize := nonNullCount * itemSizeInBytes
		bitmaskCumulativeSize := totalCount / ITEMS_PER_BITMASK
		if totalCount%ITEMS_PER_BITMASK > 0 {
			bitmaskCumulativeSize += 1
		}
		payload = nonNullItemCumulativeSize + bitmaskCumulativeSize
	}
	return payload + HEADER_SIZE
}

func EncodeCNFixedSizeHeader(frame *proto.Frame, _type byte, size int) {
	FixSizedTypesCodec.EncodeByte(frame.Content, 0, _type)
	FixSizedTypesCodec.EncodeInt(frame.Content, 1, size)
}

func EncodeListCNFixedSize(message *proto.ClientMessage, items []interface{}, itemSizeInBytes int, encoder func([]byte, int32, interface{})) {
	total := 0
	nonNull := 0

	for _, v := range items {
		if v != nil {
			nonNull += 1
		}
		total += 1
	}
	frameSize := GetFrameSizeForCNFixed(nonNull, total, itemSizeInBytes)

	frame := proto.NewFrame(make([]byte, frameSize))

	if nonNull == 0 {
		EncodeCNFixedSizeHeader(frame, TYPE_NULL_ONLY, total)
	} else if nonNull == total {
		EncodeCNFixedSizeHeader(frame, TYPE_NOT_NULL_ONLY, total)
		for i, v := range items {
			encoder(frame.Content, int32(HEADER_SIZE+i*itemSizeInBytes), v)
		}
	} else {
		EncodeCNFixedSizeHeader(frame, TYPE_MIXED, total)

		bitmaskPosition := HEADER_SIZE
		nextItemPosition := bitmaskPosition + proto.ByteSizeInBytes

		bitmask := 0
		trackedItems := 0

		for _, item := range items {
			if item != nil {
				bitmask = bitmask | 1<<trackedItems
				encoder(frame.Content, int32(nextItemPosition), item)
				nextItemPosition += itemSizeInBytes
			}
			trackedItems += 1
			if trackedItems == ITEMS_PER_BITMASK {
				FixSizedTypesCodec.EncodeByte(frame.Content, int32(bitmaskPosition), bitmask)
				bitmaskPosition = nextItemPosition
				nextItemPosition = bitmaskPosition + proto.ByteSizeInBytes
				bitmask = 0
				trackedItems = 0
			}
		}
		if trackedItems != 0 {
			FixSizedTypesCodec.EncodeByte(frame.Content, int32(bitmaskPosition), bitmask)
		}
	}
	message.AddFrame(frame)
}

func EncodeListCNBoolean(message *proto.ClientMessage, items []interface{}) {
	EncodeListCNFixedSize(message, items, proto.BooleanSizeInBytes, FixSizedTypesCodec.EncodeBoolean)
}

func EncodeListCNByte(message *proto.ClientMessage, items []interface{}) {
	EncodeListCNFixedSize(message, items, proto.ByteSizeInBytes, FixSizedTypesCodec.EncodeByte)
}

func EncodeListCNShort(message *proto.ClientMessage, items []interface{}) {
	EncodeListCNFixedSize(message, items, proto.ShortSizeInBytes, FixSizedTypesCodec.EncodeShortInt)
}

func EncodeListCNInteger(message *proto.ClientMessage, items []interface{}) {
	EncodeListCNFixedSize(message, items, proto.IntSizeInBytes, FixSizedTypesCodec.EncodeInt)
}

func EncodeListCNLong(message *proto.ClientMessage, items []interface{}) {
	EncodeListCNFixedSize(message, items, proto.LongSizeInBytes, FixSizedTypesCodec.EncodeLong)
}

func EncodeListCNFloat(message *proto.ClientMessage, items []interface{}) {
	EncodeListCNFixedSize(message, items, proto.FloatSizeInBytes, FixSizedTypesCodec.EncodeFloat)
}

func EncodeListCNDouble(message *proto.ClientMessage, items []interface{}) {
	EncodeListCNFixedSize(message, items, proto.DoubleSizeInBytes, FixSizedTypesCodec.EncodeDouble)
}

func DecodeListCNFixedSize(frame *proto.Frame, itemSizeInBytes int) []serialization.Data {
	_type := FixSizedTypesCodec.DecodeByte(frame.Content, 0)
	count := int(FixSizedTypesCodec.DecodeInt(frame.Content, 1))
	res := make([]serialization.Data, count)
	switch _type {
	case TYPE_NULL_ONLY:
		for i := 0; i < int(count); i++ {
			res = append(res, nil)
		}
	case TYPE_NOT_NULL_ONLY:
		for i := 0; i < int(count); i++ {
			res = append(res, serialization.NewSerializationData(frame.Content[HEADER_SIZE+i*itemSizeInBytes:HEADER_SIZE+(i+1)*itemSizeInBytes]))
		}
	default:
		if _type != TYPE_MIXED {
			panic("Unexpected header type")
		}
		position := HEADER_SIZE
		readCount := 0

		for readCount < count {
			bitmask := FixSizedTypesCodec.DecodeByte(frame.Content, int32(position))
			position++
			for i := 0; i < ITEMS_PER_BITMASK && readCount < count; i++ {
				mask := 1 << i
				if (bitmask & mask) == mask {
					res = append(res, serialization.NewSerializationData(frame.Content[position:position+itemSizeInBytes]))
					position += itemSizeInBytes
				} else {
					res = append(res, nil)
				}
				readCount++
			}
		}
		if readCount != len(res) {
			panic("Reading error")
		}
	}
	return res
}

func DecodeListCNBoolean(frameIterator *proto.ForwardFrameIterator) []serialization.Data {
	return DecodeListCNFixedSize(frameIterator.Next(), proto.BooleanSizeInBytes)
}

func DecodeListCNByte(frameIterator *proto.ForwardFrameIterator) []serialization.Data {
	return DecodeListCNFixedSize(frameIterator.Next(), proto.ByteSizeInBytes)
}

func DecodeListCNShort(frameIterator *proto.ForwardFrameIterator) []serialization.Data {
	return DecodeListCNFixedSize(frameIterator.Next(), proto.ShortSizeInBytes)
}

func DecodeListCNInteger(frameIterator *proto.ForwardFrameIterator) []serialization.Data {
	return DecodeListCNFixedSize(frameIterator.Next(), proto.IntSizeInBytes)
}

func DecodeListCNLong(frameIterator *proto.ForwardFrameIterator) []serialization.Data {
	return DecodeListCNFixedSize(frameIterator.Next(), proto.LongSizeInBytes)
}

func DecodeListCNFloat(frameIterator *proto.ForwardFrameIterator) []serialization.Data {
	return DecodeListCNFixedSize(frameIterator.Next(), proto.FloatSizeInBytes)
}

func DecodeListCNDouble(frameIterator *proto.ForwardFrameIterator) []serialization.Data {
	return DecodeListCNFixedSize(frameIterator.Next(), proto.DoubleSizeInBytes)
}

func EncodeSqlPage(message *proto.ClientMessage, sqlPage sql.Page) {
	message.AddFrame(proto.BeginFrame.Copy())

	// Write the last flag
	content := []byte{0}
	if sqlPage.Last() {
		content = []byte{1}
	}
	message.AddFrame(proto.NewFrame(content))

	// Write column types
	columnTypes := sqlPage.ColumnTypes()
	var columnTypeIds []int32

	for _, columnTypeId := range columnTypes {
		columnTypeIds = append(columnTypeIds, int32(columnTypeId))
	}

	EncodeListInteger(message, columnTypeIds)

	// Write columns
	for i := 0; i < sqlPage.ColumnCount(); i++ {
		columnType := columnTypes[i]
		column := sqlPage.ColumnValuesForServer(i)
		switch columnType {
		case sql.VARCHAR:
			EncodeListMultiFrameContainsNullable(message, column, EncodeString)
		case sql.BOOLEAN:
			EncodeListCNBoolean(message, column)
		case sql.TINYINT:
			EncodeListCNByte(message, column)
		case sql.SMALLINT:
			EncodeListCNShort(message, column)
		case sql.INTEGER:
			EncodeListCNInteger(message, column)
		case sql.BIGINT:
			EncodeListCNLong(message, column)
		case sql.REAL:
			EncodeListCNFloat(message, column)
		case sql.DOUBLE:
			EncodeListCNDouble(message, column)
		case sql.DATE:
			// todo
		case sql.TIME:
			// todo
		case sql.TIMESTAMP:
			// todo
		case sql.TIMESTAMP_WITH_TIME_ZONE:
			// todo
		case sql.DECIMAL:
			// todo
		case sql.NULL:
			size := 0
			for _ = range column {
				size += 1
			}

			sizeBuffer := make([]byte, proto.IntSizeInBytes)
			FixSizedTypesCodec.EncodeInt(sizeBuffer, 0, size)
			message.AddFrame(proto.NewFrame(sizeBuffer))

		case sql.OBJECT:
			EncodeListMultiFrame(message, column, EncodeNullableData)

		default:
			panic("Unknown column type id " + columnType.String())
		}
	}

}

func DecodeSqlPage(frameIterator *proto.ForwardFrameIterator) sql.Page {
	// Begin frame
	frameIterator.Next()

	// Read the "last" flag
	isLast := frameIterator.Next().Content[0] == 1

	// Read column types
	columnTypeIds := DecodeListInteger(frameIterator)
	columnTypes := make([]sql.ColumnType, len(columnTypeIds))

	// Read columns

	columns := make([][]serialization.Data, len(columnTypeIds))

	for _, v := range columnTypeIds {
		columnType := sql.ColumnType(v)
		switch columnType {
		case sql.VARCHAR:
			columns = append(columns, DecodeListMultiFrameForDataContainsNullable(frameIterator))
		case sql.BOOLEAN:
			columns = append(columns, DecodeListCNBoolean(frameIterator))
		case sql.TINYINT:
			columns = append(columns, DecodeListCNByte(frameIterator))

		case sql.SMALLINT:
			columns = append(columns, DecodeListCNShort(frameIterator))

		case sql.INTEGER:
			columns = append(columns, DecodeListCNInteger(frameIterator))

		case sql.BIGINT:
			columns = append(columns, DecodeListCNLong(frameIterator))

		case sql.REAL:
			columns = append(columns, DecodeListCNFloat(frameIterator))

		case sql.DOUBLE:
			columns = append(columns, DecodeListCNDouble(frameIterator))

		case sql.DATE:
			// todo
		case sql.TIME:
			// todo

		case sql.TIMESTAMP:
			// todo

		case sql.TIMESTAMP_WITH_TIME_ZONE:
			// todo

		case sql.DECIMAL:
			// todo

		case sql.NULL:
			frame := frameIterator.Next()
			size := FixSizedTypesCodec.DecodeInt(frame.Content, 0)

			column := make([]serialization.Data, size)

			for i := 0; i < int(size); i++ {
				column = append(column, nil)
			}

			columns = append(columns, column)

		case sql.OBJECT:

			columns = append(columns, DecodeListMultiFrameForDataContainsNullable(frameIterator))

		default:
			panic("Unknown type " + columnType.String())
		}

		columnTypes = append(columnTypes, columnType)

	}
	CodecUtil.FastForwardToEndFrame(frameIterator)

	return sql.NewPageFromColumns(columnTypes, columns, isLast)
}
