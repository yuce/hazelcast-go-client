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

package proxy

import (
	"github.com/hazelcast/hazelcast-go-client/internal/proto"
	"github.com/hazelcast/hazelcast-go-client/internal/serialization"
	"github.com/hazelcast/hazelcast-go-client/types"
)

type LazyValueListDecoder struct {
	items []*serialization.Data
	ss    *serialization.Service
}

func NewLazyValueListDecoder(items []*serialization.Data, ss *serialization.Service) *LazyValueListDecoder {
	return &LazyValueListDecoder{
		items: items,
		ss:    ss,
	}
}

func (ll LazyValueListDecoder) Len() int {
	return len(ll.items)
}

func (ll LazyValueListDecoder) ValueAt(index int) (interface{}, error) {
	if index < 0 || index >= len(ll.items) {
		panic("index out of range")
	}
	return ll.ss.ToObject(ll.items[index])
}

type LazyEntryListDecoder struct {
	items []proto.Pair
	ss    *serialization.Service
}

func NewLazyEntryListDecoder(items []proto.Pair, ss *serialization.Service) *LazyEntryListDecoder {
	return &LazyEntryListDecoder{
		items: items,
		ss:    ss,
	}
}

func (ll LazyEntryListDecoder) Len() int {
	return len(ll.items)
}

func (ll LazyEntryListDecoder) EntryAt(index int) (types.Entry, error) {
	if index < 0 || index >= len(ll.items) {
		panic("index out of range")
	}
	p := ll.items[index]
	k, err := ll.ss.ToObject(p.Key().(*serialization.Data))
	if err != nil {
		return types.Entry{}, err
	}
	v, err := ll.ss.ToObject(p.Value().(*serialization.Data))
	if err != nil {
		return types.Entry{}, err
	}
	return types.Entry{Key: k, Value: v}, nil
}
