// Copyright 2026 Matrix Origin
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

package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	metricv2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
)

const (
	CoordinatorTaskID       = "tae_object_lifecycle"
	CoordinatorTaskCronExpr = "15 * * * * *"

	BindingStateActive = "ACTIVE"
	BindingStatePaused = "PAUSED"
)

func CoordinatorTaskMetadata() task.TaskMetadata {
	return task.TaskMetadata{
		ID:       CoordinatorTaskID,
		Executor: task.TaskCode_LifecycleCoordinator,
		Options:  task.TaskOptions{Concurrency: 1},
	}
}

type Binding struct {
	ID                    string
	AccountID             uint32
	DatabaseID            uint64
	LogicalTableID        uint64
	PhysicalTableID       uint64
	Generation            uint64
	Version               uint64
	SchemaDigest          string
	LifecycleColumnID     uint64
	Action                string
	ExpireAfterDays       uint32
	LateArrivalGraceDays  uint32
	EvaluationTimezone    string
	StageID               uint64
	StageIdentityDigest   string
	PurgeAfterDays        uint32
	ScanSnapshotHex       string
	ScanLastObjectNameHex string
	ScanWrapped           bool
	State                 string
}

type BindingCursor struct {
	AccountID uint32
	BindingID string
}

type BindingPager interface {
	NextActiveBindings(
		context.Context,
		BindingCursor,
		int,
	) ([]Binding, BindingCursor, bool, error)
}

type BindingChild func(context.Context, Binding) error

type CoordinatorConfig struct {
	Enabled             bool
	PageSize            int
	MaxBindingsPerRun   int
	MaxClusterChildren  int
	MaxAccountChildren  int
	MaxDatabaseChildren int
	MaxTableChildren    int
}

type Coordinator struct {
	config CoordinatorConfig
	pager  BindingPager
	child  BindingChild
}

func NewCoordinator(
	config CoordinatorConfig,
	pager BindingPager,
	child BindingChild,
) *Coordinator {
	return &Coordinator{config: config, pager: pager, child: child}
}

func (c *Coordinator) Run(ctx context.Context) error {
	if !c.config.Enabled {
		return nil
	}
	if err := c.validate(ctx); err != nil {
		return err
	}

	bindings, err := c.loadBindings(ctx)
	if err != nil {
		return err
	}
	if len(bindings) == 0 {
		return nil
	}
	bindings = fairBindingOrder(bindings)

	runCtx := ctx

	cluster := make(chan struct{}, c.config.MaxClusterChildren)
	account := make(map[uint32]chan struct{})
	database := make(map[string]chan struct{})
	table := make(map[string]chan struct{})
	for _, binding := range bindings {
		if account[binding.AccountID] == nil {
			account[binding.AccountID] = make(chan struct{}, c.config.MaxAccountChildren)
		}
		databaseKey := fmt.Sprintf("%d/%d", binding.AccountID, binding.DatabaseID)
		if database[databaseKey] == nil {
			database[databaseKey] = make(chan struct{}, c.config.MaxDatabaseChildren)
		}
		tableKey := fmt.Sprintf("%d/%d", binding.AccountID, binding.PhysicalTableID)
		if table[tableKey] == nil {
			table[tableKey] = make(chan struct{}, c.config.MaxTableChildren)
		}
	}

	var wait sync.WaitGroup
	errs := make(chan error, len(bindings))
	for _, binding := range bindings {
		binding := binding
		wait.Add(1)
		go func() {
			defer wait.Done()
			databaseKey := fmt.Sprintf("%d/%d", binding.AccountID, binding.DatabaseID)
			tableKey := fmt.Sprintf("%d/%d", binding.AccountID, binding.PhysicalTableID)
			semaphores := []chan struct{}{
				cluster,
				account[binding.AccountID],
				database[databaseKey],
				table[tableKey],
			}
			acquired := 0
			defer func() {
				for index := acquired - 1; index >= 0; index-- {
					<-semaphores[index]
				}
			}()
			for _, semaphore := range semaphores {
				select {
				case semaphore <- struct{}{}:
					acquired++
				case <-runCtx.Done():
					errs <- runCtx.Err()
					return
				}
			}
			mode := lifecycleMetricMode(binding.Action)
			if childErr := c.child(runCtx, binding); childErr != nil {
				metricv2.LifecycleJobCounter.WithLabelValues(mode, "error").Inc()
				errs <- childErr
			} else {
				metricv2.LifecycleJobCounter.WithLabelValues(mode, "success").Inc()
			}
		}()
	}
	wait.Wait()
	close(errs)

	if ctx.Err() != nil {
		return ctx.Err()
	}
	var result error
	for childErr := range errs {
		if errors.Is(childErr, context.Canceled) && result != nil {
			continue
		}
		result = errors.Join(result, childErr)
	}
	return result
}

func lifecycleMetricMode(action string) string {
	switch action {
	case "ARCHIVE":
		return "archive"
	case "DELETE":
		return "delete"
	default:
		return "unknown"
	}
}

func (c *Coordinator) validate(ctx context.Context) error {
	if c.pager == nil || c.child == nil {
		return moerr.NewInvalidInput(ctx, "Lifecycle coordinator dependencies are nil")
	}
	if c.config.PageSize <= 0 ||
		c.config.MaxBindingsPerRun <= 0 ||
		c.config.MaxClusterChildren <= 0 ||
		c.config.MaxAccountChildren <= 0 ||
		c.config.MaxDatabaseChildren <= 0 ||
		c.config.MaxTableChildren <= 0 {
		return moerr.NewInvalidInput(ctx, "Lifecycle coordinator limits must be positive")
	}
	return nil
}

func (c *Coordinator) loadBindings(ctx context.Context) ([]Binding, error) {
	bindings := make([]Binding, 0, min(c.config.PageSize, c.config.MaxBindingsPerRun))
	cursor := BindingCursor{}
	for len(bindings) < c.config.MaxBindingsPerRun {
		remaining := c.config.MaxBindingsPerRun - len(bindings)
		limit := min(c.config.PageSize, remaining)
		page, next, end, err := c.pager.NextActiveBindings(ctx, cursor, limit)
		if err != nil {
			return nil, err
		}
		if len(page) > limit {
			return nil, moerr.NewInternalError(ctx, "Lifecycle binding pager exceeded page limit")
		}
		for _, binding := range page {
			if binding.State == BindingStateActive {
				bindings = append(bindings, binding)
			}
		}
		if end {
			return bindings, nil
		}
		if next == cursor {
			return nil, moerr.NewInternalError(ctx, "Lifecycle binding pager made no progress")
		}
		cursor = next
	}
	return bindings, nil
}

func fairBindingOrder(bindings []Binding) []Binding {
	queues := make(map[uint32][]Binding)
	accountOrder := make([]uint32, 0)
	for _, binding := range bindings {
		if _, exists := queues[binding.AccountID]; !exists {
			accountOrder = append(accountOrder, binding.AccountID)
		}
		queues[binding.AccountID] = append(queues[binding.AccountID], binding)
	}
	result := make([]Binding, 0, len(bindings))
	for len(result) < len(bindings) {
		for _, accountID := range accountOrder {
			if len(queues[accountID]) == 0 {
				continue
			}
			result = append(result, queues[accountID][0])
			queues[accountID] = queues[accountID][1:]
		}
	}
	return result
}
