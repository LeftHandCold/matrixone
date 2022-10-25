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
	"context"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

type GCEventT int8

const (
	GCEvent_Noop GCEventT = iota
	GCEvent_Checkpoint
	GCEvent_Resource
)

type GCEvent struct {
	Type    GCEventT
	Payload any
}

type GCClient interface {
	RegisterCheckpoint(context.Context, types.TS) error
	AddResource(context.Context, string) error

	GetEpoch(context.Context) (err error, ts types.TS)
	Status(context.Context) (status string, err error)
}

type gcResource struct {
	epoch types.TS
	item  string
}
