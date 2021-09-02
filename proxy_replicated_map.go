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

package hazelcast

import (
	"context"
	"fmt"

	proto2 "github.com/hazelcast/hazelcast-go-client/proto"
	codec2 "github.com/hazelcast/hazelcast-go-client/proto/codec"

	"github.com/hazelcast/hazelcast-go-client/internal/cb"
	iproxy "github.com/hazelcast/hazelcast-go-client/internal/proxy"
	iserialization "github.com/hazelcast/hazelcast-go-client/internal/serialization"
	"github.com/hazelcast/hazelcast-go-client/predicate"
	"github.com/hazelcast/hazelcast-go-client/types"
)

/*
ReplicatedMap is a distributed key-value data structure where the data is replicated to all members in the cluster.
It provides full replication of entries to all members for high speed access.

See https://docs.hazelcast.com/imdg/latest/data-structures/replicated-map.html for details.
*/
type ReplicatedMap struct {
	*proxy
	refIDGenerator *iproxy.ReferenceIDGenerator
	partitionID    int32
}

func newReplicatedMap(p *proxy, refIDGenerator *iproxy.ReferenceIDGenerator) (*ReplicatedMap, error) {
	nameData, err := p.validateAndSerialize(p.name)
	if err != nil {
		return nil, err
	}
	partitionID, err := p.partitionService.GetPartitionID(nameData)
	if err != nil {
		panic(fmt.Sprintf("error getting partition id with key: %s", p.name))
	}
	rp := &ReplicatedMap{
		proxy:          p,
		refIDGenerator: refIDGenerator,
		partitionID:    partitionID,
	}
	return rp, nil
}

// AddEntryListener adds a continuous entry listener to this map.
func (m *ReplicatedMap) AddEntryListener(ctx context.Context, handler EntryNotifiedHandler) (types.UUID, error) {
	return m.addEntryListener(ctx, nil, nil, handler)
}

// AddEntryListenerToKey adds a continuous entry listener to this map.
func (m *ReplicatedMap) AddEntryListenerToKey(ctx context.Context, key interface{}, handler EntryNotifiedHandler) (types.UUID, error) {
	return m.addEntryListener(ctx, key, nil, handler)
}

// AddEntryListenerWithPredicate adds a continuous entry listener to this map.
func (m *ReplicatedMap) AddEntryListenerWithPredicate(ctx context.Context, predicate predicate.Predicate, handler EntryNotifiedHandler) (types.UUID, error) {
	return m.addEntryListener(ctx, nil, predicate, handler)
}

// AddEntryListenerToKeyWithPredicate adds a continuous entry listener to this map.
func (m *ReplicatedMap) AddEntryListenerToKeyWithPredicate(ctx context.Context, key interface{}, predicate predicate.Predicate, handler EntryNotifiedHandler) (types.UUID, error) {
	return m.addEntryListener(ctx, key, predicate, handler)
}

// Clear deletes all entries one by one and fires related events
func (m *ReplicatedMap) Clear(ctx context.Context) error {
	request := codec2.EncodeReplicatedMapClearRequest(m.name)
	_, err := m.invokeOnRandomTarget(ctx, request, nil)
	return err
}

// ContainsKey returns true if the map contains an entry with the given key
func (m *ReplicatedMap) ContainsKey(ctx context.Context, key interface{}) (bool, error) {
	if keyData, err := m.validateAndSerialize(key); err != nil {
		return false, err
	} else {
		request := codec2.EncodeReplicatedMapContainsKeyRequest(m.name, keyData)
		if response, err := m.invokeOnKey(ctx, request, keyData); err != nil {
			return false, err
		} else {
			return codec2.DecodeReplicatedMapContainsKeyResponse(response), nil
		}
	}
}

// ContainsValue returns true if the map contains an entry with the given value
func (m *ReplicatedMap) ContainsValue(ctx context.Context, value interface{}) (bool, error) {
	if valueData, err := m.validateAndSerialize(value); err != nil {
		return false, err
	} else {
		request := codec2.EncodeReplicatedMapContainsValueRequest(m.name, valueData)
		if response, err := m.invokeOnPartition(ctx, request, m.partitionID); err != nil {
			return false, err
		} else {
			return codec2.DecodeReplicatedMapContainsValueResponse(response), nil
		}
	}
}

// Get returns the value for the specified key, or nil if this map does not contain this key.
// This function returns a clone of original value, modifying the returned value does not change the  actual value in the map.
// One should put modified value back to make changes visible to all nodes.
func (m *ReplicatedMap) Get(ctx context.Context, key interface{}) (interface{}, error) {
	if keyData, err := m.validateAndSerialize(key); err != nil {
		return nil, err
	} else {
		request := codec2.EncodeReplicatedMapGetRequest(m.name, keyData)
		if response, err := m.invokeOnKey(ctx, request, keyData); err != nil {
			return nil, err
		} else {
			return m.convertToObject(codec2.DecodeReplicatedMapGetResponse(response))
		}
	}
}

// GetEntrySet returns a clone of the mappings contained in this map.
func (m *ReplicatedMap) GetEntrySet(ctx context.Context) ([]types.Entry, error) {
	request := codec2.EncodeReplicatedMapEntrySetRequest(m.name)
	if response, err := m.invokeOnPartition(ctx, request, m.partitionID); err != nil {
		return nil, err
	} else {
		return m.convertPairsToEntries(codec2.DecodeReplicatedMapEntrySetResponse(response))
	}
}

// GetKeySet returns keys contained in this map
func (m *ReplicatedMap) GetKeySet(ctx context.Context) ([]interface{}, error) {
	request := codec2.EncodeReplicatedMapKeySetRequest(m.name)
	if response, err := m.invokeOnPartition(ctx, request, m.partitionID); err != nil {
		return nil, err
	} else {
		keyDatas := codec2.DecodeReplicatedMapKeySetResponse(response)
		keys := make([]interface{}, len(keyDatas))
		for i, keyData := range keyDatas {
			if key, err := m.convertToObject(keyData); err != nil {
				return nil, err
			} else {
				keys[i] = key
			}
		}
		return keys, nil
	}
}

// GetValues returns a list clone of the values contained in this map
func (m *ReplicatedMap) GetValues(ctx context.Context) ([]interface{}, error) {
	request := codec2.EncodeReplicatedMapValuesRequest(m.name)
	if response, err := m.invokeOnPartition(ctx, request, m.partitionID); err != nil {
		return nil, err
	} else {
		valueDatas := codec2.DecodeReplicatedMapValuesResponse(response)
		values := make([]interface{}, len(valueDatas))
		for i, valueData := range valueDatas {
			if value, err := m.convertToObject(valueData); err != nil {
				return nil, err
			} else {
				values[i] = value
			}
		}
		return values, nil
	}
}

// IsEmpty returns true if this map contains no key-value mappings.
func (m *ReplicatedMap) IsEmpty(ctx context.Context) (bool, error) {
	request := codec2.EncodeReplicatedMapIsEmptyRequest(m.name)
	if response, err := m.invokeOnPartition(ctx, request, m.partitionID); err != nil {
		return false, err
	} else {
		return codec2.DecodeReplicatedMapIsEmptyResponse(response), nil
	}
}

// Put sets the value for the given key and returns the old value.
func (m *ReplicatedMap) Put(ctx context.Context, key interface{}, value interface{}) (interface{}, error) {
	if keyData, valueData, err := m.validateAndSerialize2(key, value); err != nil {
		return nil, err
	} else {
		request := codec2.EncodeReplicatedMapPutRequest(m.name, keyData, valueData, ttlUnlimited)
		if response, err := m.invokeOnKey(ctx, request, keyData); err != nil {
			return nil, err
		} else {
			return m.convertToObject(codec2.DecodeReplicatedMapPutResponse(response))
		}
	}
}

// PutAll copies all of the mappings from the specified map to this map.
// No atomicity guarantees are given. In the case of a failure, some of the key-value tuples may get written,
// while others are not.
func (m *ReplicatedMap) PutAll(ctx context.Context, keyValuePairs ...types.Entry) error {
	if ctx == nil {
		ctx = context.Background()
	}
	f := func(partitionID int32, entries []proto2.Pair) cb.Future {
		request := codec2.EncodeReplicatedMapPutAllRequest(m.name, entries)
		return m.cb.TryContextFuture(ctx, func(ctx context.Context, attempt int) (interface{}, error) {
			if attempt > 0 {
				request = request.Copy()
			}
			if inv, err := m.invokeOnPartitionAsync(ctx, request, partitionID); err != nil {
				return nil, err
			} else {
				return inv.GetWithContext(ctx)
			}
		})
	}
	return m.putAll(keyValuePairs, f)
}

// Remove deletes the value for the given key and returns it.
func (m *ReplicatedMap) Remove(ctx context.Context, key interface{}) (interface{}, error) {
	if keyData, err := m.validateAndSerialize(key); err != nil {
		return nil, err
	} else {
		request := codec2.EncodeReplicatedMapRemoveRequest(m.name, keyData)
		if response, err := m.invokeOnKey(ctx, request, keyData); err != nil {
			return nil, err
		} else {
			return m.convertToObject(codec2.DecodeReplicatedMapRemoveResponse(response))
		}
	}
}

// RemoveEntryListener removes the specified entry listener.
func (m *ReplicatedMap) RemoveEntryListener(ctx context.Context, subscriptionID types.UUID) error {
	return m.listenerBinder.Remove(ctx, subscriptionID)
}

// Size returns the number of entries in this map.
func (m *ReplicatedMap) Size(ctx context.Context) (int, error) {
	request := codec2.EncodeReplicatedMapSizeRequest(m.name)
	if response, err := m.invokeOnPartition(ctx, request, m.partitionID); err != nil {
		return 0, err
	} else {
		return int(codec2.DecodeReplicatedMapSizeResponse(response)), nil
	}
}

func (m *ReplicatedMap) addEntryListener(ctx context.Context, key interface{}, predicate predicate.Predicate, handler EntryNotifiedHandler) (types.UUID, error) {
	var err error
	var keyData *iserialization.Data
	var predicateData *iserialization.Data
	if key != nil {
		if keyData, err = m.validateAndSerialize(key); err != nil {
			return types.UUID{}, err
		}
	}
	if predicate != nil {
		if predicateData, err = m.validateAndSerialize(predicate); err != nil {
			return types.UUID{}, err
		}
	}
	subscriptionID := types.NewUUID()
	addRequest := m.makeListenerRequest(keyData, predicateData, m.smart)
	removeRequest := codec2.EncodeReplicatedMapRemoveEntryListenerRequest(m.name, subscriptionID)
	listenerHandler := func(msg *proto2.ClientMessage) {
		m.makeListenerDecoder(msg, keyData, predicateData, m.makeEntryNotifiedListenerHandler(handler))
	}
	err = m.listenerBinder.Add(ctx, subscriptionID, addRequest, removeRequest, listenerHandler)
	return subscriptionID, err
}

func (m *ReplicatedMap) makeListenerRequest(keyData, predicateData *iserialization.Data, smart bool) *proto2.ClientMessage {
	if keyData != nil {
		if predicateData != nil {
			return codec2.EncodeReplicatedMapAddEntryListenerToKeyWithPredicateRequest(m.name, keyData, predicateData, smart)
		} else {
			return codec2.EncodeReplicatedMapAddEntryListenerToKeyRequest(m.name, keyData, smart)
		}
	} else if predicateData != nil {
		return codec2.EncodeReplicatedMapAddEntryListenerWithPredicateRequest(m.name, predicateData, smart)
	} else {
		return codec2.EncodeReplicatedMapAddEntryListenerRequest(m.name, smart)
	}
}

func (m *ReplicatedMap) makeListenerDecoder(msg *proto2.ClientMessage, keyData, predicateData *iserialization.Data, handler entryNotifiedHandler) {
	if keyData != nil {
		if predicateData != nil {
			codec2.HandleReplicatedMapAddEntryListenerToKeyWithPredicate(msg, handler)
		} else {
			codec2.HandleReplicatedMapAddEntryListenerToKey(msg, handler)
		}
	} else if predicateData != nil {
		codec2.HandleReplicatedMapAddEntryListenerWithPredicate(msg, handler)
	} else {
		codec2.HandleReplicatedMapAddEntryListener(msg, handler)
	}
}
