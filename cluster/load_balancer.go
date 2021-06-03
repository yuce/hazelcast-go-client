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

package cluster

import (
	"math/rand"
)

type LoadBalancer interface {
	// OneOf returns one of the given addreses.
	// addrs contains at least one item.
	// Assume access to this function is synchronized.
	OneOf(addrs []Address) (addr Address, ok bool)
}

type RoundRobinLoadBalancer int

func (r *RoundRobinLoadBalancer) OneOf(addrs []Address) (addr Address, ok bool) {
	index := int(*r)
	if len(addrs) <= index {
		// the client lost some of the connections
		// reset it to the last index
		index = len(addrs) - 1
	}
	addr = addrs[index]
	(*r)++
	return addr, true
}

type RandomLoadBalancer struct{}

func (r RandomLoadBalancer) OneOf(addrs []Address) (addr Address, ok bool) {
	index := rand.Intn(len(addrs))
	return addrs[index], true
}
