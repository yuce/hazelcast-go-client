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
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	hz "github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/internal/it"
	"github.com/hazelcast/hazelcast-go-client/predicate"
	"github.com/hazelcast/hazelcast-go-client/types"
)

func TestPutGetReplicatedMap(t *testing.T) {
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

func TestReplicatedMapClearSetGet(t *testing.T) {
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

func TestReplicatedMapRemove(t *testing.T) {
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

func TestReplicatedMapGetEntrySet(t *testing.T) {
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

func TestReplicatedMapGetKeySet(t *testing.T) {
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

func TestReplicatedMapIsEmptySize(t *testing.T) {
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

func TestReplicatedMapEntryNotifiedEvent(t *testing.T) {
	// This test sometimes fails. Skipping it for now...
	t.SkipNow()
	it.ReplicatedMapTesterWithConfigBuilder(t, nil, func(t *testing.T, m *hz.ReplicatedMap) {
		handlerCalled := int32(0)
		handler := func(event *hz.EntryNotified) {
			atomic.StoreInt32(&handlerCalled, 1)
		}
		subscriptionID, err := m.ListenEntryNotification(handler)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := m.Put("k1", "v1"); err != nil {
			t.Fatal(err)
		}
		time.Sleep(2 * time.Second)
		if atomic.LoadInt32(&handlerCalled) != 1 {
			t.Fatalf("handler was not called")
		}
		atomic.StoreInt32(&handlerCalled, 0)
		if err := m.UnlistenEntryNotification(subscriptionID); err != nil {
			t.Fatal(err)
		}
		if _, err := m.Put("k1", "v1"); err != nil {
			t.Fatal(err)
		}
		time.Sleep(1 * time.Second)
		if atomic.LoadInt32(&handlerCalled) != 0 {
			t.Fatalf("handler was called")
		}
	})
}

func TestReplicatedMapEntryNotifiedEventWithKey(t *testing.T) {
	// This test sometimes fails. Skipping it for now...
	t.SkipNow()
	it.ReplicatedMapTesterWithConfigBuilder(t, nil, func(t *testing.T, m *hz.ReplicatedMap) {
		handlerCalled := int32(0)
		handler := func(event *hz.EntryNotified) {
			atomic.StoreInt32(&handlerCalled, 1)
		}
		if err := m.ListenEntryNotificationToKey("k1", 1, handler); err != nil {
			t.Fatal(err)
		}
		if _, err := m.Put("k1", "v1"); err != nil {
			t.Fatal(err)
		}
		time.Sleep(2 * time.Second)
		if atomic.LoadInt32(&handlerCalled) != 1 {
			t.Fatalf("handler was not called")
		}
		atomic.StoreInt32(&handlerCalled, 0)
		it.MustValue(m.Put("k2", "v1"))
		time.Sleep(2 * time.Second)
		if atomic.LoadInt32(&handlerCalled) != 0 {
			t.Fatalf("handler was called")
		}
	})
}

func TestReplicatedMapEntryNotifiedEventWithPredicate(t *testing.T) {
	t.SkipNow()
	cbCallback := func(cb *hz.ConfigBuilder) {
		cb.Serialization().AddPortableFactory(it.SamplePortableFactory{})
	}
	it.ReplicatedMapTesterWithConfigBuilder(t, cbCallback, func(t *testing.T, m *hz.ReplicatedMap) {
		time.Sleep(1 * time.Second)
		handlerCalled := int32(0)
		handler := func(event *hz.EntryNotified) {
			atomic.StoreInt32(&handlerCalled, 1)
		}
		if _, err := m.ListenEntryNotificationWithPredicate(predicate.Equal("A", "foo"), handler); err != nil {
			t.Fatal(err)
		}
		it.MustValue(m.Put("k1", &it.SamplePortable{A: "foo", B: 10}))
		it.AssertEquals(t, &it.SamplePortable{A: "foo", B: 10}, it.MustValue(m.Get("k1")))
		time.Sleep(1 * time.Second)
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

func TestReplicatedMapEntryNotifiedEventToKeyAndPredicate(t *testing.T) {
	t.SkipNow()
	cbCallback := func(cb *hz.ConfigBuilder) {
		cb.Serialization().AddPortableFactory(it.SamplePortableFactory{})
	}
	it.ReplicatedMapTesterWithConfigBuilder(t, cbCallback, func(t *testing.T, m *hz.ReplicatedMap) {
		handlerCalled := int32(0)
		handler := func(event *hz.EntryNotified) {
			atomic.StoreInt32(&handlerCalled, 1)
		}
		if _, err := m.ListenEntryNotificationToKeyWithPredicate("k1", predicate.Equal("A", "foo"), handler); err != nil {
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