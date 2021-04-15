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

package logger

import (
	"fmt"
	"strings"

	"github.com/hazelcast/hazelcast-go-client/internal/hzerror"
)

type Level string

const (
	// OffLevel disables logging.
	OffLevel Level = "off"
	// ErrorLevel level. Logs. Used for errors that should definitely be noted.
	// Commonly used for hooks to send errors to an error tracking service.
	ErrorLevel Level = "error"
	// WarnLevel level. Non-critical entries that deserve eyes.
	WarnLevel Level = "warn"
	// InfoLevel level. General operational entries about what's going on inside the
	// application.
	InfoLevel Level = "info"
	// DebugLevel level. Usually only enabled when debugging. Very verbose logging.
	DebugLevel Level = "debug"
	// TraceLevel level. Designates finer-grained informational events than the Debug.
	TraceLevel Level = "trace"
)
const (
	offLevel = iota * 100
	errorLevel
	warnLevel
	infoLevel
	debugLevel
	traceLevel
)

// nameToLevel is used to get corresponding level for log level strings.
var nameToLevel = map[Level]int{
	ErrorLevel: errorLevel,
	WarnLevel:  warnLevel,
	InfoLevel:  infoLevel,
	DebugLevel: debugLevel,
	TraceLevel: traceLevel,
	OffLevel:   offLevel,
}

// Logger is the interface that is used by client for logging.
type Logger interface {
	// Debug logs the given arg at debug level.
	Debug(f func() string)
	// Trace logs the given arg at trace level.
	Trace(f func() string)
	// Info logs the given args at info level.
	Infof(format string, values ...interface{})
	// Warnf logs the given args at warn level.
	Warnf(format string, values ...interface{})
	// Error logs the given args at error level.
	Error(err error)
	// Error logs the given args at error level with the given format
	Errorf(format string, values ...interface{})
}

// isValidLogLevel returns true if the given log level is valid.
// The check is done case insensitive.
func isValidLogLevel(logLevel Level) bool {
	logLevelStr := strings.ToLower(string(logLevel))
	_, found := nameToLevel[Level(logLevelStr)]
	return found
}

// GetLogLevel returns the corresponding log level with the given string if it exists, otherwise returns an error.
func GetLogLevel(logLevel Level) (int, error) {
	if !isValidLogLevel(logLevel) {
		return 0, hzerror.NewHazelcastIllegalArgumentError(fmt.Sprintf("no log level found for %s", logLevel), nil)
	}
	return nameToLevel[logLevel], nil
}
