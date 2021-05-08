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

package serialization

import (
	"github.com/hazelcast/hazelcast-go-client/serialization"
)

type DefaultPortableWriter struct {
	serializer      *PortableSerializer
	output          *PositionalObjectDataOutput
	classDefinition serialization.ClassDefinition
	begin           int32
	offset          int32
}

func NewDefaultPortableWriter(serializer *PortableSerializer, output *PositionalObjectDataOutput,
	classDefinition serialization.ClassDefinition) *DefaultPortableWriter {
	begin := output.Position()
	output.WriteZeroBytes(4)
	output.WriteInt32(int32(classDefinition.FieldCount()))
	offset := output.Position()
	fieldIndexesLength := (classDefinition.FieldCount() + 1) * Int32SizeInBytes
	output.WriteZeroBytes(fieldIndexesLength)
	return &DefaultPortableWriter{serializer, output, classDefinition, begin, offset}
}

func (pw *DefaultPortableWriter) WriteByte(fieldName string, value byte) {
	pw.setPosition(fieldName, TypeByte)
	pw.output.WriteByte(value)
}

func (pw *DefaultPortableWriter) WriteBool(fieldName string, value bool) {
	pw.setPosition(fieldName, TypeBool)
	pw.output.WriteBool(value)
}

func (pw *DefaultPortableWriter) WriteUInt16(fieldName string, value uint16) {
	pw.setPosition(fieldName, TypeUint16)
	pw.output.WriteUInt16(value)
}

func (pw *DefaultPortableWriter) WriteInt16(fieldName string, value int16) {
	pw.setPosition(fieldName, TypeInt16)
	pw.output.WriteInt16(value)
}

func (pw *DefaultPortableWriter) WriteInt32(fieldName string, value int32) {
	pw.setPosition(fieldName, TypeInt32)
	pw.output.WriteInt32(value)
}

func (pw *DefaultPortableWriter) WriteInt64(fieldName string, value int64) {
	pw.setPosition(fieldName, TypeInt64)
	pw.output.WriteInt64(value)
}

func (pw *DefaultPortableWriter) WriteFloat32(fieldName string, value float32) {
	pw.setPosition(fieldName, TypeFloat32)
	pw.output.WriteFloat32(value)
}

func (pw *DefaultPortableWriter) WriteFloat64(fieldName string, value float64) {
	pw.setPosition(fieldName, TypeFloat64)
	pw.output.WriteFloat64(value)
}

func (pw *DefaultPortableWriter) WriteString(fieldName string, value string) {
	pw.setPosition(fieldName, TypeUTF)
	pw.output.WriteString(value)
}

func (pw *DefaultPortableWriter) WritePortable(fieldName string, portable serialization.Portable) error {
	fieldDefinition := pw.setPosition(fieldName, TypePortable)
	isNullPortable := portable == nil
	pw.output.WriteBool(isNullPortable)
	pw.output.WriteInt32(fieldDefinition.FactoryID())
	pw.output.WriteInt32(fieldDefinition.ClassID())
	if !isNullPortable {
		err := pw.serializer.WriteObject(pw.output, portable)
		if err != nil {
			return err
		}
	}
	return nil
}

func (pw *DefaultPortableWriter) WriteNilPortable(fieldName string, factoryID int32, classID int32) error {
	pw.setPosition(fieldName, TypePortable)
	pw.output.WriteBool(true)
	pw.output.WriteInt32(factoryID)
	pw.output.WriteInt32(classID)
	return nil
}

func (pw *DefaultPortableWriter) WriteByteArray(fieldName string, array []byte) {
	pw.setPosition(fieldName, TypeByteArray)
	pw.output.WriteByteArray(array)
}

func (pw *DefaultPortableWriter) WriteBoolArray(fieldName string, array []bool) {
	pw.setPosition(fieldName, TypeBoolArray)
	pw.output.WriteBoolArray(array)
}

func (pw *DefaultPortableWriter) WriteUInt16Array(fieldName string, array []uint16) {
	pw.setPosition(fieldName, TypeUint16Array)
	pw.output.WriteUInt16Array(array)
}

func (pw *DefaultPortableWriter) WriteInt16Array(fieldName string, array []int16) {
	pw.setPosition(fieldName, TypeInt16Array)
	pw.output.WriteInt16Array(array)
}

func (pw *DefaultPortableWriter) WriteInt32Array(fieldName string, array []int32) {
	pw.setPosition(fieldName, TypeInt32Array)
	pw.output.WriteInt32Array(array)
}

func (pw *DefaultPortableWriter) WriteInt64Array(fieldName string, array []int64) {
	pw.setPosition(fieldName, TypeInt64Array)
	pw.output.WriteInt64Array(array)
}

func (pw *DefaultPortableWriter) WriteFloat32Array(fieldName string, array []float32) {
	pw.setPosition(fieldName, TypeFloat32Array)
	pw.output.WriteFloat32Array(array)
}

func (pw *DefaultPortableWriter) WriteFloat64Array(fieldName string, array []float64) {
	pw.setPosition(fieldName, TypeFloat64Array)
	pw.output.WriteFloat64Array(array)
}

func (pw *DefaultPortableWriter) WriteStringArray(fieldName string, array []string) {
	pw.setPosition(fieldName, TypeUTFArray)
	pw.output.WriteStringArray(array)
}

func (pw *DefaultPortableWriter) WritePortableArray(fieldName string, portableArray []serialization.Portable) error {
	fieldDefinition := pw.setPosition(fieldName, TypePortableArray)
	length := len(portableArray)
	if portableArray == nil {
		length = nilArrayLength
	}
	pw.output.WriteInt32(int32(length))
	pw.output.WriteInt32(fieldDefinition.FactoryID())
	pw.output.WriteInt32(fieldDefinition.ClassID())
	if length <= 0 || portableArray == nil {
		// portableArray nil check is required just to avoid the warning about nil portableArray index
		return nil
	}
	innerOffset := pw.output.Position()
	pw.output.WriteZeroBytes(length * 4)
	for i := 0; i < length; i++ {
		sample := portableArray[i]
		posVal := pw.output.Position()
		pw.output.PWriteInt32(innerOffset+int32(i)*Int32SizeInBytes, posVal)
		if err := pw.serializer.WriteObject(pw.output, sample); err != nil {
			return err
		}
	}
	return nil
}

func (pw *DefaultPortableWriter) setPosition(fieldName string, fieldType int32) serialization.FieldDefinition {
	field := pw.classDefinition.Field(fieldName)
	pos := pw.output.Position()
	index := field.Index()
	pw.output.PWriteInt32(pw.offset+index*Int32SizeInBytes, pos)
	pw.output.WriteInt16(int16(len(fieldName)))
	pw.output.WriteBytes(fieldName)
	pw.output.WriteByte(byte(fieldType))
	return field
}

func (pw *DefaultPortableWriter) End() {
	position := pw.output.Position()
	pw.output.PWriteInt32(pw.begin, position)
}
