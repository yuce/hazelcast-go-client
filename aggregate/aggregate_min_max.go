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

func Min(attr string) *aggMin {
	return &aggMin{attrPath: attr}
}

func Max(attr string) *aggMax {
	return &aggMax{attrPath: attr}
}

type aggMin struct {
	attrPath string
}

func (a aggMin) FactoryID() int32 {
	return factoryID
}

func (a aggMin) ClassID() (classID int32) {
	return 15
}

func (a aggMin) WriteData(output serialization.DataOutput) {
	output.WriteString(a.attrPath)
	// member side, not used in client
	output.WriteObject(nil)
}

func (a *aggMin) ReadData(input serialization.DataInput) {
	a.attrPath = input.ReadString()
	// member side, not used in client
	input.ReadObject()
}

func (a aggMin) String() string {
	return fmt.Sprintf("Min(%s)", a.attrPath)
}

type aggMax struct {
	attrPath string
}

func (a aggMax) FactoryID() int32 {
	return factoryID
}

func (a aggMax) ClassID() (classID int32) {
	return 14
}

func (a aggMax) WriteData(output serialization.DataOutput) {
	output.WriteString(a.attrPath)
	// member side, not used in client
	output.WriteObject(nil)
}

func (a *aggMax) ReadData(input serialization.DataInput) {
	a.attrPath = input.ReadString()
	// member side, not used in client
	input.ReadObject()
}

func (a aggMax) String() string {
	return fmt.Sprintf("Max(%s)", a.attrPath)
}
