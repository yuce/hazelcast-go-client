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
	"time"
)

type LoadBalancer interface {
	// OneOf returns one of the given addreses.
	// addrs contains at least one item.
	// Order of addresses may change between calls.
	// Assume access to this function is synchronized.
	// This function should return as soon as possible, should never block.
	OneOf(addrs []Address) Address
}

type RoundRobinLoadBalancer int

func NewRoundRobinLoadBalancer() *RoundRobinLoadBalancer {
	index := RoundRobinLoadBalancer(0)
	return &index
}

func (r *RoundRobinLoadBalancer) OneOf(addrs []Address) Address {
	index := int(*r)
	if len(addrs) <= index {
		// the client lost some of the connections
		// reset it to the last index
		index = len(addrs) - 1
	}
	addr := addrs[index]
	index = (index + 1) % len(addrs)
	*r = RoundRobinLoadBalancer(index)
	return addr
}

type RandomLoadBalancer rand.Rand

func NewRandomLoadBalancer() *RandomLoadBalancer {
	r := rand.New(rand.NewSource(time.Now().Unix()))
	lb := RandomLoadBalancer(*r)
	return &lb
}

func (lb *RandomLoadBalancer) OneOf(addrs []Address) Address {
	r := rand.Rand(*lb)
	index := (&r).Intn(len(addrs))
	return addrs[index]
}
