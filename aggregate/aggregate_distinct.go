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

import (
	"fmt"

	"github.com/hazelcast/hazelcast-go-client/serialization"
)

func DistinctValues(attr string) *aggDistinct {
	return &aggDistinct{attrPath: attr}
}

type aggDistinct struct {
	attrPath string
}

func (a aggDistinct) FactoryID() int32 {
	return factoryID
}

func (a aggDistinct) ClassID() (classID int32) {
	return 5
}

func (a aggDistinct) WriteData(output serialization.DataOutput) {
	output.WriteString(a.attrPath)
	// member side field, not used in client
	output.WriteInt64(0)
}

func (a *aggDistinct) ReadData(input serialization.DataInput) {
	a.attrPath = input.ReadString()
	// member side field, not used in client
	input.ReadInt64()
}

func (a aggDistinct) String() string {
	return fmt.Sprintf("DistinctValues(%s)", a.attrPath)
}
