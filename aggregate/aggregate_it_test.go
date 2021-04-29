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

package aggregate_test

import (
	"fmt"
	"testing"

	"github.com/hazelcast/hazelcast-go-client/aggregate"

	hz "github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/internal/it"
)

func TestAggregateDistinctValues_Aggregate(t *testing.T) {
	cbCallback := func(cb *hz.ConfigBuilder) {
		cb.Serialization().AddPortableFactory(it.SamplePortableFactory{})
	}
	it.MapTesterWithConfigBuilder(t, cbCallback, func(t *testing.T, m *hz.Map) {
		it.MustValue(m.Put("k1", &it.SamplePortable{A: "foo", B: 10}))
		it.MustValue(m.Put("k2", &it.SamplePortable{A: "qoo", B: 10}))
		it.MustValue(m.Put("k3", &it.SamplePortable{A: "foo", B: 10}))
		distinctNames := aggregate.DistinctValues("A")
		if values, err := m.Aggregate(distinctNames); err != nil {
			t.Fatal(err)
		} else {
			fmt.Println("values", values)
		}
	})
}
