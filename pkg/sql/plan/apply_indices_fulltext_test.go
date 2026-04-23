// Copyright 2025 Matrix Origin
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

package plan

import (
	"testing"

	"github.com/bytedance/sonic"
	"github.com/matrixorigin/matrixone/pkg/fulltext"
	pbplan "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestBuildFullTextScanParamsPreservesNonNativeOnlyForLegacyParams(t *testing.T) {
	builder := &QueryBuilder{}
	idx := &pbplan.IndexDef{
		Parts:           []string{"body"},
		IndexAlgoParams: `{"parser":"ngram"}`,
	}

	params, err := builder.buildFullTextScanParams(nil, idx)
	require.NoError(t, err)

	var param fulltext.FullTextParserParam
	require.NoError(t, sonic.Unmarshal([]byte(params), &param))
	require.Equal(t, fulltext.FullTextImplementationNative, param.Implementation)
	require.False(t, param.NativeOnlyMode)
	require.Equal(t, "ngram", param.Parser)
	require.Equal(t, []string{"body"}, param.Parts)
}

func TestBuildFullTextScanParamsPreservesExplicitNativeOnly(t *testing.T) {
	builder := &QueryBuilder{}
	idx := &pbplan.IndexDef{
		Parts:           []string{"body"},
		IndexAlgoParams: `{"parser":"ngram","native_only":true}`,
	}

	params, err := builder.buildFullTextScanParams(nil, idx)
	require.NoError(t, err)

	var param fulltext.FullTextParserParam
	require.NoError(t, sonic.Unmarshal([]byte(params), &param))
	require.Equal(t, fulltext.FullTextImplementationNative, param.Implementation)
	require.True(t, param.NativeOnlyMode)
	require.Equal(t, "ngram", param.Parser)
	require.Equal(t, []string{"body"}, param.Parts)
}

func TestCanElideFullTextSourceJoinForPkAndScoreProjection(t *testing.T) {
	scanNode := &pbplan.Node{
		BindingTags: []int32{1},
		TableDef: &pbplan.TableDef{
			Pkey: &pbplan.PrimaryKeyDef{PkeyColName: "id"},
			Name2ColIndex: map[string]int32{
				"id":      0,
				"content": 1,
			},
			Cols: []*pbplan.ColDef{
				{Name: "id", Typ: pbplan.Type{Id: 1}},
				{Name: "content", Typ: pbplan.Type{Id: 2}},
			},
		},
	}
	ftNode := &pbplan.Node{
		BindingTags: []int32{2},
		TableDef: &pbplan.TableDef{
			Cols: []*pbplan.ColDef{
				{Name: "doc_id", Typ: pbplan.Type{Id: 1}},
				{Name: "score", Typ: pbplan.Type{Id: 3}},
			},
		},
	}
	projNode := &pbplan.Node{
		ProjectList: []*pbplan.Expr{
			{
				Typ:  pbplan.Type{Id: 1},
				Expr: &pbplan.Expr_Col{Col: &pbplan.ColRef{RelPos: 1, ColPos: 0}},
			},
			{
				Typ:  pbplan.Type{Id: 3},
				Expr: &pbplan.Expr_Col{Col: &pbplan.ColRef{RelPos: 2, ColPos: 1}},
			},
		},
	}
	sortNode := &pbplan.Node{
		OrderBy: []*pbplan.OrderBySpec{{
			Expr: &pbplan.Expr{
				Typ:  pbplan.Type{Id: 3},
				Expr: &pbplan.Expr_Col{Col: &pbplan.ColRef{RelPos: 2, ColPos: 1}},
			},
		}},
	}

	require.True(t, canElideFullTextSourceJoin(projNode, sortNode, scanNode, ftNode))
	applyFullTextSourceJoinElision(projNode, sortNode, scanNode, ftNode)

	pkCol := projNode.ProjectList[0].GetCol()
	require.NotNil(t, pkCol)
	require.Equal(t, int32(2), pkCol.RelPos)
	require.Equal(t, int32(0), pkCol.ColPos)
}

func TestReplaceProjectionFullTextMatchWithScore(t *testing.T) {
	projNode := &pbplan.Node{
		ProjectList: []*pbplan.Expr{
			{
				Expr: &pbplan.Expr_F{F: &pbplan.Function{
					Func: &pbplan.ObjectRef{ObjName: "fulltext_match"},
				}},
			},
		},
	}
	nodes := []*pbplan.Node{
		nil,
		{
			BindingTags: []int32{7},
			TableDef: &pbplan.TableDef{
				Cols: []*pbplan.ColDef{
					{Name: "doc_id", Typ: pbplan.Type{Id: 1}},
					{Name: "score", Typ: pbplan.Type{Id: 3}},
				},
			},
		},
	}

	replaceProjectionFullTextMatchWithScore(projNode, []int32{0}, []int32{1}, nodes)

	scoreCol := projNode.ProjectList[0].GetCol()
	require.NotNil(t, scoreCol)
	require.Equal(t, int32(7), scoreCol.RelPos)
	require.Equal(t, int32(1), scoreCol.ColPos)
}

func TestCanElideFullTextSourceJoinRejectsOtherSourceColumns(t *testing.T) {
	scanNode := &pbplan.Node{
		BindingTags: []int32{1},
		TableDef: &pbplan.TableDef{
			Pkey: &pbplan.PrimaryKeyDef{PkeyColName: "id"},
			Name2ColIndex: map[string]int32{
				"id":      0,
				"content": 1,
			},
			Cols: []*pbplan.ColDef{
				{Name: "id", Typ: pbplan.Type{Id: 1}},
				{Name: "content", Typ: pbplan.Type{Id: 2}},
			},
		},
	}
	ftNode := &pbplan.Node{
		BindingTags: []int32{2},
		TableDef: &pbplan.TableDef{
			Cols: []*pbplan.ColDef{
				{Name: "doc_id", Typ: pbplan.Type{Id: 1}},
				{Name: "score", Typ: pbplan.Type{Id: 3}},
			},
		},
	}
	projNode := &pbplan.Node{
		ProjectList: []*pbplan.Expr{
			{
				Typ:  pbplan.Type{Id: 2},
				Expr: &pbplan.Expr_Col{Col: &pbplan.ColRef{RelPos: 1, ColPos: 1}},
			},
			{
				Typ:  pbplan.Type{Id: 3},
				Expr: &pbplan.Expr_Col{Col: &pbplan.ColRef{RelPos: 2, ColPos: 1}},
			},
		},
	}

	require.False(t, canElideFullTextSourceJoin(projNode, nil, scanNode, ftNode))
}

func TestCanElideFullTextCountStarJoin(t *testing.T) {
	scanNode := &pbplan.Node{}
	aggNode := &pbplan.Node{
		AggList: []*pbplan.Expr{{
			Expr: &pbplan.Expr_F{F: &pbplan.Function{
				Func: &pbplan.ObjectRef{ObjName: "starcount"},
			}},
		}},
	}
	require.True(t, canElideFullTextCountStarJoin(aggNode, scanNode))

	scanNode.FilterList = []*pbplan.Expr{{}}
	require.False(t, canElideFullTextCountStarJoin(aggNode, scanNode))
	scanNode.FilterList = nil

	aggNode.GroupBy = []*pbplan.Expr{{}}
	require.False(t, canElideFullTextCountStarJoin(aggNode, scanNode))
	aggNode.GroupBy = nil

	aggNode.AggList[0] = &pbplan.Expr{
		Expr: &pbplan.Expr_F{F: &pbplan.Function{
			Func: &pbplan.ObjectRef{ObjName: "count"},
		}},
	}
	require.False(t, canElideFullTextCountStarJoin(aggNode, scanNode))
}
