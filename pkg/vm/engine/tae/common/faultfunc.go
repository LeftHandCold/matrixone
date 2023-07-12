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

package common

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"math/rand"
)

func WithFaultTriggered(name string, exec func(sarg string, iarg int64) error) error {
	iarg, sarg, exist := fault.TriggerFault(name)
	if exist {
		return exec(sarg, iarg)
	}
	return nil
}

func RandomTriggerFault(ctx context.Context, name string) error {
	return WithFaultTriggered(name, func(sarg string, iarg int64) error {
		if iarg == 0 || rand.Int63n(iarg) == 0 {
			return moerr.NewInternalError(ctx, sarg)
		}
		return nil
	})
}

func NonrandomTriggerFault(ctx context.Context, name string) error {
	return WithFaultTriggered(name, func(sarg string, iarg int64) error {
		return moerr.NewInternalError(ctx, sarg)
	})
}
