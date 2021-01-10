/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
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

package config

type ReconnectMode uint8

const (
	// OFF Prevent reconnect to cluster after a disconnect
	OFF = iota
	// ON Reconnect to cluster by blocking invocations
	ON
	// ASYNC Reconnect to cluster without blocking invocations. Invocations will receive HazelcastClientOfflineException
	ASYNC
)

type ConnectionStrategyConfig interface {
	IsAsyncStart() bool
}

type connectionStrategyConfig struct {
	AsyncStart            bool
	ReconnectMode         ReconnectMode
	ConnectionRetryConfig ConnectionRetryConfig
}

func NewConnectionStrategyConfig() ConnectionStrategyConfig {
	return &connectionStrategyConfig{}
}

func (c connectionStrategyConfig) IsAsyncStart() bool {
	return c.AsyncStart
}

type ConnectionRetryConfig struct {
	//TODO should implement it.
}
