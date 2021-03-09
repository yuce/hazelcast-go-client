// Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License")
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

// Package hazelcast provides methods for creating Hazelcast clients and client configurations.
package client

import (
    "github.com/hazelcast/hazelcast-go-client/v4/internal/client"
	"github.com/hazelcast/hazelcast-go-client/v4/config"
	"github.com/hazelcast/hazelcast-go-client/v4/hazelcast"
)

// NewClient creates and returns a new Client.
// Hazelcast client enables you to do all Hazelcast operations without
// being a member of the cluster. It connects to one of the
// cluster members and delegates all cluster wide operations to it.
// When the connected cluster member dies, client will
// automatically switch to another live member.
func New() (hazelcast.Client, error) {
	return NewWithConfig(config.New())
}

// NewClientWithConfig creates and returns a new Client with the given config.
// Hazelcast client enables you to do all Hazelcast operations without
// being a member of the cluster. It connects to one of the
// cluster members and delegates all cluster wide operations to it.
// When the connected cluster member dies, client will
// automatically switch to another live member.
func NewWithConfig(config *config.Config) (hazelcast.Client, error) {
	return client.NewImpl("dsda",config), nil
}

// NewConfig creates and returns a new config.
func NewConfig() *config.Config {
	return config.New()
}

