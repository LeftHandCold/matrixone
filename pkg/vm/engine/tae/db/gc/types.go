// Copyright 2021 Matrix Origin
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

package gc

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

type GCEventT int8

const (
	GCEvent_Noop GCEventT = iota
	GCEvent_Checkpoint
	GCEvent_Resource
	GCEvent_Refresh
)

type GCEvent struct {
	Type    GCEventT
	Payload any
}

type GCStateT int8

const (
	GCState_NonEpoch GCStateT = iota
	GCState_InEpoch
)

func (state GCStateT) String() string {
	if state == GCState_NonEpoch {
		return "NonEpoch"
	}
	return "InEpoch"
}

type GCClient interface {
	RegisterCheckpoint(context.Context, types.TS) error
	AddResource(context.Context, string) error

	GetEpoch(context.Context) (err error, ts types.TS)
	Status(context.Context) (status string, err error)
}

type Stats struct {
	Epoch         types.TS
	MinCheckpoint types.TS
	MaxCheckpoint types.TS
	SafeTimestamp types.TS

	State GCStateT
}

func (stats *Stats) String() string {
	var buf bytes.Buffer
	_, _ = buf.WriteString(fmt.Sprintf("State:         %s\n", stats.State.String()))
	_, _ = buf.WriteString(fmt.Sprintf("Epoch:         %s\n", stats.Epoch.ToString()))
	_, _ = buf.WriteString(fmt.Sprintf("MinCheckpoint: %s\n", stats.MinCheckpoint.ToString()))
	_, _ = buf.WriteString(fmt.Sprintf("MaxCheckpoint: %s\n", stats.MaxCheckpoint.ToString()))
	_, _ = buf.WriteString(fmt.Sprintf("SafeTimestamp: %s\n", stats.SafeTimestamp.ToString()))
	return buf.String()
}

type gcResource struct {
	epoch types.TS
	item  string
}

type TTLFunc = func(time.Time)
