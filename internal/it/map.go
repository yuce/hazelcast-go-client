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

package it

import (
	"context"
	"fmt"
	"testing"

	"go.uber.org/goleak"

	hz "github.com/hazelcast/hazelcast-go-client"
)

type MapTestContext struct {
	T              *testing.T
	M              *hz.Map
	MapName        string
	Client         *hz.Client
	Config         *hz.Config
	ConfigCallback func(*hz.Config)
	NameMaker      func(...string) string
	Smart          bool
}

func MapTesterWithContext(tcx *MapTestContext, f func(*MapTestContext)) {
	ensureRemoteController(true)
	runner := func(tcx MapTestContext) {
		if LeakCheckEnabled() {
			tcx.T.Logf("enabled leak check")
			defer goleak.VerifyNone(tcx.T)
		}
		config := defaultTestCluster.DefaultConfig()
		if tcx.Config != nil {
			config = *tcx.Config
		}
		if tcx.ConfigCallback != nil {
			tcx.ConfigCallback(&config)
		}
		config.Cluster.Unisocket = !tcx.Smart
		tcx.Config = &config
		ls := "smart"
		if !tcx.Smart {
			ls = "unisocket"
		}
		if tcx.NameMaker == nil {
			tcx.NameMaker = func(labels ...string) string {
				return NewUniqueObjectName("map", labels...)
			}
		}
		if tcx.MapName == "" {
			tcx.MapName = tcx.NameMaker(ls)
		}
		if tcx.Client == nil {
			tcx.Client = getDefaultClient(tcx.Config)
		}
		if tcx.M == nil {
			m, err := tcx.Client.GetMap(context.Background(), tcx.MapName)
			if err != nil {
				panic(err)
			}
			tcx.M = m
		}
		defer func() {
			ctx := context.Background()
			if err := tcx.M.Destroy(ctx); err != nil {
				tcx.T.Logf("test warning, could not destroy map: %s", err.Error())
			}
			if err := tcx.Client.Shutdown(ctx); err != nil {
				tcx.T.Logf("Test warning, client not shutdown: %s", err.Error())
			}
		}()
		f(&tcx)
	}
	if SmartEnabled() {
		tcx.T.Run("Smart Client", func(t *testing.T) {
			tt := *tcx
			tt.Smart = true
			runner(tt)
		})
	}
	if NonSmartEnabled() {
		tcx.T.Run("Non-Smart Client", func(t *testing.T) {
			tt := *tcx
			tt.Smart = false
			runner(tt)
		})
	}
}

func MapTester(t *testing.T, f func(t *testing.T, m *hz.Map)) {
	MapTesterWithConfig(t, nil, f)
}

func MapTesterWithConfig(t *testing.T, configCallback func(*hz.Config), f func(t *testing.T, m *hz.Map)) {
	makeMapName := func(labels ...string) string {
		return NewUniqueObjectName("map", labels...)
	}
	MapTesterWithConfigAndName(t, makeMapName, configCallback, f)
}

func MapTesterWithConfigAndName(t *testing.T, makeMapName func(...string) string, configCallback func(*hz.Config), f func(t *testing.T, m *hz.Map)) {
	tcx := &MapTestContext{
		T:              t,
		NameMaker:      makeMapName,
		ConfigCallback: configCallback,
	}
	MapTesterWithContext(tcx, func(tcx *MapTestContext) {
		f(tcx.T, tcx.M)
	})
}

func GetClientMapWithConfig(mapName string, config *hz.Config) (*hz.Client, *hz.Map) {
	client := getDefaultClient(config)
	if m, err := client.GetMap(context.Background(), mapName); err != nil {
		panic(err)
	} else {
		return client, m
	}
}

func MapSetOnServer(clusterID string, mapName string, key, value string) *Response {
	script := fmt.Sprintf(`
		var map = instance_0.getMap("%s");
        map.set(%s, %s);
	`, mapName, key, value)
	resp, err := rc.ExecuteOnController(context.Background(), clusterID, script, Lang_JAVASCRIPT)
	if err != nil {
		panic(fmt.Errorf("executing on controller: %w", err))
	}
	return resp
}
