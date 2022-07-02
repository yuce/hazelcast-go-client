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

package nearcache

import "time"

// Stats contains statistics for a Near Cache instance.
type Stats struct {
	InvalidationRequests int64
	// Misses is the number of times a key was not found in the Near Cache.
	Misses int64
	// Hits is the number of times a key was found in the Near Cache.
	Hits int64
	// Expirations is the number of expirations.
	Expirations int64
	// Evictions is the number of evictions.
	Evictions int64
	// OwnedEntryCount is the number of entries in the Near Cache.
	OwnedEntryCount int64
	// OwnedEntryMemoryCost is the estimated memory cost of the entries in the Near Cache.
	OwnedEntryMemoryCost int64
	// Invalidations is the number of successful invalidations.
	Invalidations int64
	// LastPersistenceKeyCount is the number of keys saved in the last persistence task.
	LastPersistenceKeyCount int64
	// LastPersistenceWrittenBytes is the size of the last persistence task.
	LastPersistenceWrittenBytes int64
	// PersistenceCount is the number of completed persistence tasks.
	PersistenceCount int64
	// CreationTime is the time the Near Cache was initialized.
	CreationTime time.Time
	// LastPersistenceTime is the time of the last completed persistence task.
	LastPersistenceTime time.Time
	// LastPersistenceFailure is the error message of the last completed persistence task.
	LastPersistenceFailure string
	// LastPersistenceDuration is the duration of the last completed persistence task.
	LastPersistenceDuration time.Duration
}