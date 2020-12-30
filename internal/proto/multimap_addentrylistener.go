// Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package proto

import (
	"github.com/hazelcast/hazelcast-go-client/serialization"

	"github.com/hazelcast/hazelcast-go-client/internal/proto/bufutil"
)

func multimapAddEntryListenerCalculateSize(name string, includeValue bool, localOnly bool) int {
	// Calculates the request payload size
	dataSize := 0
	dataSize += stringCalculateSize(name)
	dataSize += bufutil.BoolSizeInBytes
	dataSize += bufutil.BoolSizeInBytes
	return dataSize
}

// MultiMapAddEntryListenerEncodeRequest creates and encodes a client message
// with the given parameters.
// It returns the encoded client message.
func MultiMapAddEntryListenerEncodeRequest(name string, includeValue bool, localOnly bool) *ClientMessage {
	// Encode request into clientMessage
	//TODO
	return nil
}

// MultiMapAddEntryListenerDecodeResponse decodes the given client message.
// It returns a function which returns the response parameters.
func MultiMapAddEntryListenerDecodeResponse(clientMessage *ClientMessage) func() (response string) {
	// Decode response from client message
	//TODO
	return nil
}

// MultiMapAddEntryListenerHandleEventEntryFunc is the event handler function.
type MultiMapAddEntryListenerHandleEventEntryFunc func(serialization.Data, serialization.Data, serialization.Data, serialization.Data, int32, string, int32)

// MultiMapAddEntryListenerEventEntryDecode decodes the corresponding event
// from the given client message.
// It returns the result parameters for the event.
func MultiMapAddEntryListenerEventEntryDecode(clientMessage *ClientMessage) (
	key serialization.Data, value serialization.Data, oldValue serialization.Data, mergingValue serialization.Data, eventType int32, uuid string, numberOfAffectedEntries int32) {
	//TODO
	return nil, nil, nil, nil, 0, "", 0
}

// MultiMapAddEntryListenerHandle handles the event with the given
// event handler function.
func MultiMapAddEntryListenerHandle(clientMessage *ClientMessage,
	handleEventEntry MultiMapAddEntryListenerHandleEventEntryFunc) {
	//TODO
	return
}