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

package types

// LazyValueListHolder contains values which can be accessed by an index.
// The values are decoded lazily when accessed.
type LazyValueListHolder interface {
	// Len returns the number of values.
	Len() int
	// ValueAt returns the value at the given index.
	// If index is not in the range, it panics.
	// The value is decoded on demand.
	ValueAt(index int) (interface{}, error)
}

// LazyEntryListHolder contains values which can be accessed by an index.
// The values are decoded lazily when accessed.
type LazyEntryListHolder interface {
	// Len returns the number of entries.
	Len() int
	// EntryAt returns the value at the given index.
	// If index is not in the range, it panics.
	// The entry is decoded on demand.
	EntryAt(index int) (Entry, error)
}
