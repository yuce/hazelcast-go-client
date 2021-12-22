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

import (
	"math/big"
)

// Decimal is a wrapper for Hazelcast Decimal.
type Decimal struct {
	unscaledValue *big.Int
	scale         int32
}

// NewDecimal creates and returns a Decimal value with the given big int and scale.
func NewDecimal(unscaledValue *big.Int, scale int32) Decimal {
	return Decimal{
		unscaledValue: unscaledValue,
		scale:         scale,
	}
}

// UnscaledValue returns the unscaled value of the decimal.
func (d Decimal) UnscaledValue() *big.Int {
	return d.unscaledValue
}

// Scale returns the scale of the decimal.
func (d Decimal) Scale() int32 {
	return d.scale
}