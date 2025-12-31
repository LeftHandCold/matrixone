// Copyright 2021 - 2022 Matrix Origin
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

package function

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/agg"
)

var supportedWindowInNewFramework = []FuncNew{
	{
		functionId: RANK,
		class:      plan.Function_WIN_ORDER,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			if len(inputs) == 0 {
				return newCheckResultWithSuccess(0)
			}
			return newCheckResultWithFailure(failedFunctionParametersWrong)
		},
		Overloads: []overload{
			{
				overloadId: 0,
				isWin:      true,
				retType:    aggexec.SingleWindowReturnType,
				aggFramework: aggregationLogicOfOverload{
					str:         "rank",
					aggRegister: agg.RegisterRank,
				},
			},
		},
	},
	{
		functionId: ROW_NUMBER,
		class:      plan.Function_WIN_ORDER,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			if len(inputs) == 0 {
				return newCheckResultWithSuccess(0)
			}
			return newCheckResultWithFailure(failedFunctionParametersWrong)
		},
		Overloads: []overload{
			{
				overloadId: 0,
				isWin:      true,
				retType:    aggexec.SingleWindowReturnType,
				aggFramework: aggregationLogicOfOverload{
					str:         "row_number",
					aggRegister: agg.RegisterRowNumber,
				},
			},
		},
	},
	{
		functionId: DENSE_RANK,
		class:      plan.Function_WIN_ORDER,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			if len(inputs) == 0 {
				return newCheckResultWithSuccess(0)
			}
			return newCheckResultWithFailure(failedFunctionParametersWrong)
		},
		Overloads: []overload{
			{
				overloadId: 0,
				isWin:      true,
				retType:    aggexec.SingleWindowReturnType,
				aggFramework: aggregationLogicOfOverload{
					str:         "dense_rank",
					aggRegister: agg.RegisterDenseRank,
				},
			},
		},
	},
	{
		functionId: LAG,
		class:      plan.Function_WIN_VALUE,
		layout:     STANDARD_FUNCTION,
		checkFn: func(overloads []overload, inputs []types.Type) checkResult {
			// LAG(expr [, offset [, default]])
			// At least 1 argument (the expression), at most 3 arguments
			if len(inputs) >= 1 && len(inputs) <= 3 {
				return newCheckResultWithSuccess(0)
			}
			return newCheckResultWithFailure(failedFunctionParametersWrong)
		},
		Overloads: []overload{
			{
				overloadId: 0,
				isWin:      true,
				retType:    aggexec.LagReturnType,
				aggFramework: aggregationLogicOfOverload{
					str:         "lag",
					aggRegister: agg.RegisterLag,
				},
			},
		},
	},
}
