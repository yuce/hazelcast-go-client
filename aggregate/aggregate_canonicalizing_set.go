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

package aggregate

import "github.com/hazelcast/hazelcast-go-client/serialization"

type aggregateCanonicalizingSet struct {
	// TODO: change this to a proper set type
	values map[interface{}]struct{}
}

func (a aggregateCanonicalizingSet) FactoryID() (factoryID int32) {
	return factoryID
}

func (a aggregateCanonicalizingSet) ClassID() (classID int32) {
	return canonicalizingSetClassID

}

func (a aggregateCanonicalizingSet) WriteData(output serialization.DataOutput) (err error) {
	output.WriteInt32(int32(len(a.values)))
	for k := range a.values {
		if err := output.WriteObject(k); err != nil {
			return err
		}
	}
	return nil
}

func (a aggregateCanonicalizingSet) ReadData(input serialization.DataInput) (err error) {
	size := int(input.ReadInt32())
	values := map[interface{}]struct{}{}
	for i := 0; i < size; i++ {
		value := input.ReadObject()
		values[value] = struct{}{}
	}
	a.values = values
	return nil
}

func (a aggregateCanonicalizingSet) String() string {
	return "CanonicalizingSet()"
}

func (a aggregateCanonicalizingSet) Aggregate() interface{} {
	return a.values
}

func (a aggregateCanonicalizingSet) enforceAggregate() {
	//
}
