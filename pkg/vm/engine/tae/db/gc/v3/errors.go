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
	"fmt"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"go.uber.org/zap"
)

// GCErrorCode defines GC-specific error codes
type GCErrorCode int

const (
	ErrCodeUnknown GCErrorCode = iota
	ErrCodeCheckpointRead
	ErrCodeSnapshotRead
	ErrCodePITRRead
	ErrCodeFilterExecution
	ErrCodeFileDelete
	ErrCodeConfigValidation
	ErrCodeResourceExhaustion
	ErrCodeTimeout
	ErrCodeConcurrency
)

// GCError represents a structured GC error
type GCError struct {
	Code      GCErrorCode
	Message   string
	Cause     error
	Context   map[string]interface{}
	Timestamp time.Time
	TaskName  string
}

func (e *GCError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("GC Error [%d]: %s (caused by: %v)", e.Code, e.Message, e.Cause)
	}
	return fmt.Sprintf("GC Error [%d]: %s", e.Code, e.Message)
}

func (e *GCError) Unwrap() error {
	return e.Cause
}

// NewGCError creates a new GC error
func NewGCError(code GCErrorCode, message string, cause error) *GCError {
	return &GCError{
		Code:      code,
		Message:   message,
		Cause:     cause,
		Context:   make(map[string]interface{}),
		Timestamp: time.Now(),
	}
}

// WithContext adds context information to the error
func (e *GCError) WithContext(key string, value interface{}) *GCError {
	e.Context[key] = value
	return e
}

// WithTaskName sets the task name for the error
func (e *GCError) WithTaskName(taskName string) *GCError {
	e.TaskName = taskName
	return e
}

// ErrorHandler provides structured error handling for GC operations
type ErrorHandler struct {
	taskName string
	logger   *zap.Logger
}

// NewErrorHandler creates a new error handler
func NewErrorHandler(taskName string) *ErrorHandler {
	return &ErrorHandler{
		taskName: taskName,
		logger:   logutil.GetGlobalLogger(),
	}
}

// HandleError handles a GC error with appropriate logging and context
func (eh *ErrorHandler) HandleError(err error, operation string, extraContext ...zap.Field) error {
	if err == nil {
		return nil
	}

	gcErr, ok := err.(*GCError)
	if !ok {
		// Convert regular error to GC error
		gcErr = NewGCError(ErrCodeUnknown, err.Error(), err).WithTaskName(eh.taskName)
	}

	// Build log fields
	fields := []zap.Field{
		zap.String("task", eh.taskName),
		zap.String("operation", operation),
		zap.Int("error_code", int(gcErr.Code)),
		zap.String("error_message", gcErr.Message),
		zap.Time("timestamp", gcErr.Timestamp),
	}

	// Add context fields
	for key, value := range gcErr.Context {
		fields = append(fields, zap.Any(key, value))
	}

	// Add extra context
	fields = append(fields, extraContext...)

	eh.logger.Error("GC Operation Failed", fields...)

	return gcErr
}

// HandleWarning handles a warning with appropriate logging
func (eh *ErrorHandler) HandleWarning(message string, extraContext ...zap.Field) {
	fields := []zap.Field{
		zap.String("task", eh.taskName),
		zap.String("warning", message),
	}
	fields = append(fields, extraContext...)

	eh.logger.Warn("GC Warning", fields...)
}

// HandleInfo logs informational messages
func (eh *ErrorHandler) HandleInfo(message string, extraContext ...zap.Field) {
	fields := []zap.Field{
		zap.String("task", eh.taskName),
		zap.String("info", message),
	}
	fields = append(fields, extraContext...)

	eh.logger.Info("GC Info", fields...)
}

// WrapWithRecovery wraps a function with panic recovery and error handling
func (eh *ErrorHandler) WrapWithRecovery(operation string, fn func() error) error {
	defer func() {
		if r := recover(); r != nil {
			err := fmt.Errorf("panic in %s: %v", operation, r)
			gcErr := NewGCError(ErrCodeUnknown, "panic occurred", err).WithTaskName(eh.taskName)
			eh.HandleError(gcErr, operation)
		}
	}()

	return fn()
}

// MeasureExecutionTime measures and logs execution time for operations
func (eh *ErrorHandler) MeasureExecutionTime(operation string, fn func() error) error {
	start := time.Now()
	err := fn()
	duration := time.Since(start)

	if err != nil {
		eh.HandleError(err, operation, zap.Duration("duration", duration))
	} else {
		eh.HandleInfo(fmt.Sprintf("%s completed successfully", operation), zap.Duration("duration", duration))
	}

	return err
}

// Common error constructors for specific GC operations
func NewCheckpointReadError(cause error, checkpointName string) *GCError {
	return NewGCError(ErrCodeCheckpointRead, "failed to read checkpoint", cause).
		WithContext("checkpoint", checkpointName)
}

func NewSnapshotReadError(cause error, sid string) *GCError {
	return NewGCError(ErrCodeSnapshotRead, "failed to read snapshot", cause).
		WithContext("sid", sid)
}

func NewPITRReadError(cause error, timestamp time.Time) *GCError {
	return NewGCError(ErrCodePITRRead, "failed to read PITR", cause).
		WithContext("timestamp", timestamp)
}

func NewFilterExecutionError(cause error, filterName string) *GCError {
	return NewGCError(ErrCodeFilterExecution, "filter execution failed", cause).
		WithContext("filter", filterName)
}

func NewFileDeleteError(cause error, files []string) *GCError {
	return NewGCError(ErrCodeFileDelete, "failed to delete files", cause).
		WithContext("files", files).
		WithContext("file_count", len(files))
}

func NewConfigValidationError(cause error, configField string) *GCError {
	return NewGCError(ErrCodeConfigValidation, "configuration validation failed", cause).
		WithContext("field", configField)
}

func NewTimeoutError(operation string, timeout time.Duration) *GCError {
	return NewGCError(ErrCodeTimeout, fmt.Sprintf("operation %s timed out", operation), nil).
		WithContext("timeout", timeout).
		WithContext("operation", operation)
}

// ErrorCollector collects multiple errors during batch operations
type ErrorCollector struct {
	errors   []*GCError
	taskName string
}

func NewErrorCollector(taskName string) *ErrorCollector {
	return &ErrorCollector{
		errors:   make([]*GCError, 0),
		taskName: taskName,
	}
}

func (ec *ErrorCollector) Add(err error) {
	if err == nil {
		return
	}

	if gcErr, ok := err.(*GCError); ok {
		ec.errors = append(ec.errors, gcErr.WithTaskName(ec.taskName))
	} else {
		ec.errors = append(ec.errors, NewGCError(ErrCodeUnknown, err.Error(), err).WithTaskName(ec.taskName))
	}
}

func (ec *ErrorCollector) HasErrors() bool {
	return len(ec.errors) > 0
}

func (ec *ErrorCollector) GetErrors() []*GCError {
	return ec.errors
}

func (ec *ErrorCollector) Clear() {
	ec.errors = ec.errors[:0]
}

// ToAggregatedError converts collected errors to a single aggregated error
func (ec *ErrorCollector) ToAggregatedError() error {
	if !ec.HasErrors() {
		return nil
	}

	if len(ec.errors) == 1 {
		return ec.errors[0]
	}

	messages := make([]string, len(ec.errors))
	for i, err := range ec.errors {
		messages[i] = err.Error()
	}

	return NewGCError(
		ErrCodeUnknown,
		fmt.Sprintf("multiple errors occurred: %v", messages),
		nil,
	).WithTaskName(ec.taskName).WithContext("error_count", len(ec.errors))
}

// ContextualError wraps context cancellation and timeout errors
func ContextualError(ctx context.Context, operation string) error {
	if ctx.Err() == nil {
		return nil
	}

	if ctx.Err() == context.Canceled {
		return NewGCError(ErrCodeConcurrency, fmt.Sprintf("operation %s was canceled", operation), ctx.Err())
	}

	if ctx.Err() == context.DeadlineExceeded {
		return NewGCError(ErrCodeTimeout, fmt.Sprintf("operation %s timed out", operation), ctx.Err())
	}

	return NewGCError(ErrCodeUnknown, fmt.Sprintf("context error in operation %s", operation), ctx.Err())
}

// IsRetriableError determines if an error is retriable
func IsRetriableError(err error) bool {
	if gcErr, ok := err.(*GCError); ok {
		switch gcErr.Code {
		case ErrCodeResourceExhaustion, ErrCodeTimeout, ErrCodeConcurrency:
			return true
		default:
			return false
		}
	}
	return false
}
