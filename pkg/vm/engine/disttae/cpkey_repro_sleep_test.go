// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package disttae

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseCPKeyReproSleepConfigDisabledByDefault(t *testing.T) {
	cfg := parseCPKeyReproSleepConfig(func(string) (string, bool) {
		return "", false
	})
	require.False(t, cfg.enabled())
	require.False(t, cfg.enabledFor(cpkeyReproStageStatementBoundary))
}

func TestParseCPKeyReproSleepConfigEnabledWithStageFilter(t *testing.T) {
	env := map[string]string{
		envCPKeyReproRandomSleepMS:     "25",
		envCPKeyReproRandomSleepPct:    "40",
		envCPKeyReproRandomSleepStages: "stmt-boundary, persisted-delete",
	}
	cfg := parseCPKeyReproSleepConfig(func(key string) (string, bool) {
		v, ok := env[key]
		return v, ok
	})
	require.True(t, cfg.enabled())
	require.Equal(t, 25, cfg.maxSleepMilliseconds)
	require.Equal(t, 40, cfg.probabilityPercent)
	require.True(t, cfg.enabledFor(cpkeyReproStageStatementBoundary))
	require.True(t, cfg.enabledFor(cpkeyReproStagePersistedDelete))
	require.False(t, cfg.enabledFor(cpkeyReproStageObjectRewrite))
}

func TestParseCPKeyReproSleepConfigInvalidValuesDisableFeature(t *testing.T) {
	env := map[string]string{
		envCPKeyReproRandomSleepMS:  "bad",
		envCPKeyReproRandomSleepPct: "-1",
	}
	cfg := parseCPKeyReproSleepConfig(func(key string) (string, bool) {
		v, ok := env[key]
		return v, ok
	})
	require.False(t, cfg.enabled())
}

func TestCPKeyReproForceFlushEnabled(t *testing.T) {
	cpkeyReproForceFlushOnce = sync.Once{}
	cpkeyReproForceFlush = false
	t.Setenv(envCPKeyReproForceFlush, "true")
	require.True(t, cpkeyReproForceFlushEnabled())

	cpkeyReproForceFlushOnce = sync.Once{}
	cpkeyReproForceFlush = false
	t.Setenv(envCPKeyReproForceFlush, "0")
	require.False(t, cpkeyReproForceFlushEnabled())
}
