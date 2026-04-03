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
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"go.uber.org/zap"
)

const (
	envCPKeyReproRandomSleepMS     = "MO_CPKEY_REPRO_RANDOM_SLEEP_MS"
	envCPKeyReproRandomSleepPct    = "MO_CPKEY_REPRO_RANDOM_SLEEP_PCT"
	envCPKeyReproRandomSleepStages = "MO_CPKEY_REPRO_RANDOM_SLEEP_STAGES"

	cpkeyReproStageStatementBoundary = "stmt-boundary"
	cpkeyReproStagePersistedDelete   = "persisted-delete"
	cpkeyReproStageObjectRewrite     = "object-rewrite"
)

type cpkeyReproSleepConfig struct {
	maxSleepMilliseconds int
	probabilityPercent   int
	stages               map[string]struct{}
}

func (c cpkeyReproSleepConfig) enabled() bool {
	return c.maxSleepMilliseconds > 0 && c.probabilityPercent > 0
}

func (c cpkeyReproSleepConfig) enabledFor(stage string) bool {
	if !c.enabled() {
		return false
	}
	if len(c.stages) == 0 {
		return true
	}
	_, ok := c.stages[strings.ToLower(strings.TrimSpace(stage))]
	return ok
}

func (c cpkeyReproSleepConfig) stageNames() []string {
	if len(c.stages) == 0 {
		return nil
	}
	names := make([]string, 0, len(c.stages))
	for stage := range c.stages {
		names = append(names, stage)
	}
	sort.Strings(names)
	return names
}

func (c cpkeyReproSleepConfig) randomDuration() time.Duration {
	if !c.enabled() {
		return 0
	}
	if c.probabilityPercent < 100 && rand.Intn(100) >= c.probabilityPercent {
		return 0
	}
	return time.Duration(rand.Intn(c.maxSleepMilliseconds)+1) * time.Millisecond
}

func parseCPKeyReproSleepConfig(lookupEnv func(string) (string, bool)) cpkeyReproSleepConfig {
	cfg := cpkeyReproSleepConfig{}

	rawMS, ok := lookupEnv(envCPKeyReproRandomSleepMS)
	if !ok {
		return cfg
	}
	ms, err := strconv.Atoi(strings.TrimSpace(rawMS))
	if err != nil || ms <= 0 {
		return cfg
	}
	cfg.maxSleepMilliseconds = ms
	cfg.probabilityPercent = 100

	if rawPct, ok := lookupEnv(envCPKeyReproRandomSleepPct); ok {
		pct, err := strconv.Atoi(strings.TrimSpace(rawPct))
		if err == nil {
			switch {
			case pct <= 0:
				cfg.probabilityPercent = 0
			case pct >= 100:
				cfg.probabilityPercent = 100
			default:
				cfg.probabilityPercent = pct
			}
		}
	}

	if rawStages, ok := lookupEnv(envCPKeyReproRandomSleepStages); ok {
		stages := make(map[string]struct{})
		for _, stage := range strings.Split(rawStages, ",") {
			stage = strings.ToLower(strings.TrimSpace(stage))
			if stage == "" {
				continue
			}
			stages[stage] = struct{}{}
		}
		if len(stages) > 0 {
			cfg.stages = stages
		}
	}

	return cfg
}

var (
	cpkeyReproSleepOnce sync.Once
	cpkeyReproSleepCfg  cpkeyReproSleepConfig
)

func getCPKeyReproSleepConfig() cpkeyReproSleepConfig {
	cpkeyReproSleepOnce.Do(func() {
		rand.Seed(time.Now().UnixNano())
		cpkeyReproSleepCfg = parseCPKeyReproSleepConfig(os.LookupEnv)
		if cpkeyReproSleepCfg.enabled() {
			fields := []zap.Field{
				zap.Int("max-sleep-ms", cpkeyReproSleepCfg.maxSleepMilliseconds),
				zap.Int("probability-pct", cpkeyReproSleepCfg.probabilityPercent),
			}
			if stages := cpkeyReproSleepCfg.stageNames(); len(stages) > 0 {
				fields = append(fields, zap.Strings("stages", stages))
			}
			logutil.Info("CPKEY-REPRO-SLEEP-ENABLED", fields...)
		}
	})
	return cpkeyReproSleepCfg
}

func maybeCPKeyReproRandomSleep(stage string) {
	cfg := getCPKeyReproSleepConfig()
	if !cfg.enabledFor(stage) {
		return
	}
	if d := cfg.randomDuration(); d > 0 {
		time.Sleep(d)
	}
}
