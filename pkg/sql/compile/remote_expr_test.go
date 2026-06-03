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

package compile

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/filter"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/group"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/lockop"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/onduplicatekey"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/projection"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestScopeContainsVarExpr(t *testing.T) {
	scope := newScope(Normal)
	proj := projection.NewArgument()
	proj.ProjectList = []*plan.Expr{makeTestVarExpr("sql_mode")}
	f := filter.NewArgument()
	f.FilterExprs = []*plan.Expr{makeTestConstBoolExpr(true)}
	f.AppendChild(proj)
	scope.setRootOperator(f)

	require.True(t, scopeContainsVarExpr(scope))
}

func TestScopeContainsVarExprInSource(t *testing.T) {
	scope := newScope(Normal)
	scope.DataSource = &Source{
		FilterList: []*plan.Expr{makeTestVarExpr("sql_mode")},
	}

	require.True(t, scopeContainsVarExpr(scope))
}

func TestScopeContainsVarExprInOperatorMap(t *testing.T) {
	scope := newScope(Normal)
	op := onduplicatekey.NewArgument()
	op.OnDuplicateExpr = map[string]*plan.Expr{
		"col": makeTestVarExpr("sql_mode"),
	}
	scope.setRootOperator(op)

	require.True(t, scopeContainsVarExpr(scope))
}

func TestScopeContainsVarExprInAggArguments(t *testing.T) {
	scope := newScope(Normal)
	op := group.NewArgument()
	op.Aggs = []aggexec.AggFuncExecExpression{
		aggexec.MakeAggFunctionExpression(function.AggSumOverloadID, false, []*plan.Expr{makeTestVarExpr("sql_mode")}, nil),
	}
	scope.setRootOperator(op)

	require.True(t, scopeContainsVarExpr(scope))
}

func TestScopeContainsVarExprInLockRows(t *testing.T) {
	scope := newScope(Normal)
	op := lockop.NewArgumentByEngine(nil)
	op.AddLockTarget(1, nil, 0, types.T_int64.ToType(), -1, -1, makeTestVarExpr("sql_mode"), false)
	scope.setRootOperator(op)

	require.True(t, scopeContainsVarExpr(scope))
}

func TestFoldVarExprsInScope(t *testing.T) {
	proc := testutil.NewProcess(t)
	proc.SetResolveVariableFunc(func(name string, system, global bool) (interface{}, error) {
		if name == "sql_mode" {
			return "STRICT_TRANS_TABLES", nil
		}
		return nil, moerr.NewInternalErrorNoCtx("variable not found")
	})

	scope := newScope(Normal)
	scope.DataSource = &Source{
		FilterList: []*plan.Expr{makeTestVarExpr("sql_mode")},
	}

	folded, err := foldVarExprsInScope(scope, proc)
	require.NoError(t, err)
	require.True(t, folded)
	require.False(t, scopeContainsVarExpr(scope))

	lit, ok := scope.DataSource.FilterList[0].Expr.(*plan.Expr_Lit)
	require.True(t, ok)
	require.Equal(t, "STRICT_TRANS_TABLES", lit.Lit.GetSval())
}

func TestScopeContainsVarExprReturnsFalseWithoutVar(t *testing.T) {
	scope := newScope(Normal)
	f := filter.NewArgument()
	f.FilterExprs = []*plan.Expr{makeTestConstBoolExpr(true)}
	scope.setRootOperator(f)

	require.False(t, scopeContainsVarExpr(scope))
}

func makeTestVarExpr(name string) *plan.Expr {
	typ := types.T_text.ToType()
	return &plan.Expr{
		Typ: plan2.MakePlan2Type(&typ),
		Expr: &plan.Expr_V{
			V: &plan.VarRef{
				Name:   name,
				System: true,
			},
		},
	}
}

func makeTestConstBoolExpr(v bool) *plan.Expr {
	typ := types.T_bool.ToType()
	return &plan.Expr{
		Typ: plan2.MakePlan2Type(&typ),
		Expr: &plan.Expr_Lit{
			Lit: &plan.Literal{
				Value: &plan.Literal_Bval{Bval: v},
			},
		},
	}
}
