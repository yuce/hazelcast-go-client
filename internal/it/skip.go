/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
	"testing"

	"github.com/hazelcast/hazelcast-go-client/internal/skip"
)

/*
SkipIf can be used to skip a test case based on comma-separated conditions.
Deprecated: Use skip.If instead.
*/
func SkipIf(t *testing.T, conditions string) {
	skip.If(t, conditions)
}

// MarkSlow marks a test "slow", so it is run only when slow tests are enabled.
func MarkSlow(t *testing.T) {
	skip.If(t, "!slow")
}

// MarkFlaky marks a test "flaky", so it is run only when flaky tests are enabled.
func MarkFlaky(t *testing.T, see ...string) {
	t.Logf("Note: %s is a known flaky test, it will run only when enabled.", t.Name())
	for _, s := range see {
		t.Logf("See: %s", s)
	}
	skip.If(t, "!flaky")
}
