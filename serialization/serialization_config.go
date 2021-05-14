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

import "reflect"

// Config contains the serialization configuration of a Hazelcast instance.
type Config struct {
	// GlobalSerializer is the fall back serializer that will be used if no other serializer is applicable.
	GlobalSerializer Serializer
	// IdentifiedDataSerializableFactories contains IdentifiedDataSerializable factories.
	IdentifiedDataSerializableFactories map[int32]IdentifiedDataSerializableFactory
	// PortableFactories contains Portable factories.
	PortableFactories map[int32]PortableFactory
	// CustomSerializers contains custom serializers.
	CustomSerializers map[reflect.Type]Serializer
	// ClassDefinitions contains ClassDefinitions for portable structs.
	ClassDefinitions []ClassDefinition
	// PortableVersion will be used to differentiate two versions of the same struct that have changes on the struct,
	PortableVersion int32
	// BigEndian specify big endian byte order if true.
	BigEndian bool
}

func (c Config) Clone() Config {
	idFactories := map[int32]IdentifiedDataSerializableFactory{}
	for k, v := range c.IdentifiedDataSerializableFactories {
		idFactories[k] = v
	}
	pFactories := map[int32]PortableFactory{}
	for k, v := range c.PortableFactories {
		pFactories[k] = v
	}
	serializers := map[reflect.Type]Serializer{}
	for k, v := range c.CustomSerializers {
		serializers[k] = v
	}
	defs := make([]ClassDefinition, len(c.ClassDefinitions))
	copy(defs, c.ClassDefinitions)
	return Config{
		BigEndian:                           c.BigEndian,
		IdentifiedDataSerializableFactories: idFactories,
		PortableFactories:                   pFactories,
		PortableVersion:                     c.PortableVersion,
		CustomSerializers:                   serializers,
		GlobalSerializer:                    c.GlobalSerializer,
		ClassDefinitions:                    defs,
	}
}
