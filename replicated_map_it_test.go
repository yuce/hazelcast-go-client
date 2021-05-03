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

package hazelcast_test

import (
	"fmt"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	hz "github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/internal/it"
	"github.com/hazelcast/hazelcast-go-client/predicate"
	"github.com/hazelcast/hazelcast-go-client/types"
)

func TestReplicatedMap_Put(t *testing.T) {
	it.ReplicatedMapTesterWithConfigBuilder(t, nil, func(t *testing.T, m *hz.ReplicatedMap) {
		targetValue := "value"
		if _, err := m.Put("key", targetValue); err != nil {
			t.Fatal(err)
		}
		if value, err := m.Get("key"); err != nil {
			t.Fatal(err)
		} else if targetValue != value {
			t.Fatalf("target %v != %v", targetValue, value)
		}
	})
}

func TestReplicatedMap_Clear(t *testing.T) {
	it.ReplicatedMapTesterWithConfigBuilder(t, nil, func(t *testing.T, m *hz.ReplicatedMap) {
		targetValue := "value"
		it.MustValue(m.Put("key", targetValue))
		if ok := it.MustBool(m.ContainsKey("key")); !ok {
			t.Fatalf("key not found")
		}
		if ok := it.MustBool(m.ContainsValue("value")); !ok {
			t.Fatalf("value not found")
		}
		if value := it.MustValue(m.Get("key")); targetValue != value {
			t.Fatalf("target %v != %v", targetValue, value)
		}
		if err := m.Clear(); err != nil {
			t.Fatal(err)
		}
		if value := it.MustValue(m.Get("key")); nil != value {
			t.Fatalf("target nil!= %v", value)
		}
	})
}

func TestReplicatedMap_Remove(t *testing.T) {
	it.ReplicatedMapTesterWithConfigBuilder(t, nil, func(t *testing.T, m *hz.ReplicatedMap) {
		targetValue := "value"
		it.MustValue(m.Put("key", targetValue))
		if !it.MustBool(m.ContainsKey("key")) {
			t.Fatalf("key not found")
		}
		if value, err := m.Remove("key"); err != nil {
			t.Fatal(err)
		} else if targetValue != value {
			t.Fatalf("target nil != %v", value)
		}
		if it.MustBool(m.ContainsKey("key")) {
			t.Fatalf("key found")
		}
	})
}

func TestReplicatedMap_GetEntrySet(t *testing.T) {
	it.ReplicatedMapTesterWithConfigBuilder(t, nil, func(t *testing.T, m *hz.ReplicatedMap) {
		target := []types.Entry{
			types.NewEntry("k1", "v1"),
			types.NewEntry("k2", "v2"),
			types.NewEntry("k3", "v3"),
		}
		if err := m.PutAll(target); err != nil {
			t.Fatal(err)
		}
		time.Sleep(1 * time.Second)
		if entries, err := m.GetEntrySet(); err != nil {
			t.Fatal(err)
		} else if !entriesEqualUnordered(target, entries) {
			t.Fatalf("target: %#v != %#v", target, entries)
		}
	})
}

func TestReplicatedMap_GetKeySet(t *testing.T) {
	it.ReplicatedMapTesterWithConfigBuilder(t, nil, func(t *testing.T, m *hz.ReplicatedMap) {
		targetKeySet := []interface{}{"k1", "k2", "k3"}
		it.MustValue(m.Put("k1", "v1"))
		it.MustValue(m.Put("k2", "v2"))
		it.MustValue(m.Put("k3", "v3"))
		time.Sleep(1 * time.Second)
		it.AssertEquals(t, "v1", it.MustValue(m.Get("k1")))
		it.AssertEquals(t, "v2", it.MustValue(m.Get("k2")))
		it.AssertEquals(t, "v3", it.MustValue(m.Get("k3")))
		if keys, err := m.GetKeySet(); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(makeStringSet(targetKeySet), makeStringSet(keys)) {
			t.Fatalf("target: %#v != %#v", targetKeySet, keys)
		}
	})
}

func TestReplicatedMap_IsEmptySize(t *testing.T) {
	it.ReplicatedMapTesterWithConfigBuilder(t, nil, func(t *testing.T, m *hz.ReplicatedMap) {
		if value, err := m.IsEmpty(); err != nil {
			t.Fatal(err)
		} else if !value {
			t.Fatalf("target: true != false")
		}
		targetSize := 0
		if value, err := m.Size(); err != nil {
			t.Fatal(err)
		} else if targetSize != value {
			t.Fatalf("target: %d != %d", targetSize, value)
		}
		it.MustValue(m.Put("k1", "v1"))
		it.MustValue(m.Put("k2", "v2"))
		it.MustValue(m.Put("k3", "v3"))
		if value, err := m.IsEmpty(); err != nil {
			t.Fatal(err)
		} else if value {
			t.Fatalf("target: false != true")
		}
		targetSize = 3
		if value, err := m.Size(); err != nil {
			t.Fatal(err)
		} else if targetSize != value {
			t.Fatalf("target: %d != %d", targetSize, value)
		}
	})
}

func TestReplicatedMap_AddEntryListener_EntryNotifiedEvent(t *testing.T) {
	it.ReplicatedMapTesterWithConfigBuilder(t, nil, func(t *testing.T, m *hz.ReplicatedMap) {
		const targetCallCount = int32(10)
		callCount := int32(0)
		handler := func(event *hz.EntryNotified) {
			atomic.AddInt32(&callCount, 1)
		}
		subscriptionID, err := m.AddEntryListener(handler)
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < int(targetCallCount); i++ {
			key := fmt.Sprintf("key-%d", i)
			value := fmt.Sprintf("value-%d", i)
			it.MustValue(m.Put(key, value))
		}
		time.Sleep(1 * time.Second)
		if !assert.Equal(t, targetCallCount, atomic.LoadInt32(&callCount)) {
			t.FailNow()
		}
		atomic.StoreInt32(&callCount, 0)
		if err := m.RemoveEntryListener(subscriptionID); err != nil {
			t.Fatal(err)
		}
		if _, err := m.Put("k1", "v1"); err != nil {
			t.Fatal(err)
		}
		if !assert.Equal(t, int32(0), atomic.LoadInt32(&callCount)) {
			t.FailNow()
		}
		/*
			time.Sleep(1 * time.Second)
			if atomic.LoadInt32(&handlerCalled) != 1 {
				t.Fatalf("handler was not called")
			}
			atomic.StoreInt32(&handlerCalled, 0)
			if err := m.RemoveEntryListener(subscriptionID); err != nil {
				t.Fatal(err)
			}
			if _, err := m.Put("k1", "v1"); err != nil {
				t.Fatal(err)
			}
			time.Sleep(1 * time.Second)
			if atomic.LoadInt32(&handlerCalled) != 0 {
				t.Fatalf("handler was called")
			}

		*/
	})
}

func TestReplicatedMap_AddEntryListener_EntryNotifiedEventWithKey(t *testing.T) {
	t.SkipNow()
	it.ReplicatedMapTesterWithConfigBuilder(t, nil, func(t *testing.T, m *hz.ReplicatedMap) {
		const targetCallCount = 10
		callCount := int32(0)
		handler := func(event *hz.EntryNotified) {
			atomic.AddInt32(&callCount, 1)
		}
		if _, err := m.AddEntryListenerToKey("k1", handler); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < targetCallCount; i++ {
			key := fmt.Sprintf("key-%d", i)
			value := fmt.Sprintf("value-%d", i)
			if _, err := m.Put(key, value); err != nil {
				t.Fatal(err)
			}
		}
		if !assert.Equal(t, targetCallCount, atomic.LoadInt32(&callCount)) {
			t.FailNow()
		}
		/*
			atomic.StoreInt32(&callCount, 0)
			it.MustValue(m.Put("k2", "v1"))
			time.Sleep(2 * time.Second)
			if atomic.LoadInt32(&callCount) != 0 {
				t.Fatalf("handler was called")
			}
		*/
	})
}

func TestReplicatedMap_AddEntryListener_EntryNotifiedEventWithPredicate(t *testing.T) {
	t.SkipNow()
	cbCallback := func(cb *hz.ConfigBuilder) {
		cb.Serialization().AddPortableFactory(it.SamplePortableFactory{})
	}
	it.ReplicatedMapTesterWithConfigBuilder(t, cbCallback, func(t *testing.T, m *hz.ReplicatedMap) {
		handlerCalled := int32(0)
		handler := func(event *hz.EntryNotified) {
			atomic.StoreInt32(&handlerCalled, 1)
		}
		if _, err := m.AddEntryListenerWithPredicate(predicate.Equal("A", "foo"), handler); err != nil {
			t.Fatal(err)
		}
		it.MustValue(m.Put("k1", &it.SamplePortable{A: "foo", B: 10}))
		time.Sleep(2 * time.Second)
		if atomic.LoadInt32(&handlerCalled) != 1 {
			t.Fatalf("handler was not called")
		}
		atomic.StoreInt32(&handlerCalled, 0)
		it.MustValue(m.Put("k1", &it.SamplePortable{A: "bar", B: 10}))
		time.Sleep(2 * time.Second)
		if atomic.LoadInt32(&handlerCalled) != 0 {
			t.Fatalf("handler was called")
		}
	})
}

func TestReplicatedMap_AddEntryListener_EntryNotifiedEventToKeyAndPredicate(t *testing.T) {
	t.SkipNow()
	cbCallback := func(cb *hz.ConfigBuilder) {
		cb.Serialization().AddPortableFactory(it.SamplePortableFactory{})
	}
	it.ReplicatedMapTesterWithConfigBuilder(t, cbCallback, func(t *testing.T, m *hz.ReplicatedMap) {
		handlerCalled := int32(0)
		handler := func(event *hz.EntryNotified) {
			atomic.StoreInt32(&handlerCalled, 1)
		}
		if _, err := m.AddEntryListenerToKeyWithPredicate("k1", predicate.Equal("A", "foo"), handler); err != nil {
			t.Fatal(err)
		}
		it.MustValue(m.Put("k1", &it.SamplePortable{A: "foo", B: 10}))
		time.Sleep(1 * time.Second)
		if atomic.LoadInt32(&handlerCalled) != 1 {
			t.Fatalf("handler was not called")
		}
		atomic.StoreInt32(&handlerCalled, 0)
		it.MustValue(m.Put("k2", &it.SamplePortable{A: "foo", B: 10}))
		time.Sleep(1 * time.Second)
		if atomic.LoadInt32(&handlerCalled) != 0 {
			t.Fatalf("handler was called")
		}
		it.MustValue(m.Put("k1", &it.SamplePortable{A: "bar", B: 10}))
		time.Sleep(1 * time.Second)
		if atomic.LoadInt32(&handlerCalled) != 0 {
			t.Fatalf("handler was called")
		}
	})
}
