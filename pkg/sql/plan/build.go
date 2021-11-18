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

package plan

import (
	"fmt"
	"matrixone/pkg/errno"
	"matrixone/pkg/sql/errors"
	"matrixone/pkg/sql/parsers/tree"
	"matrixone/pkg/sql/rewrite"
	"matrixone/pkg/vm/engine"
)

func New(db string, sql string, e engine.Engine) *build {
	return &build{
		e:   e,
		db:  db,
		sql: sql,
	}
}

func (b *build) BuildStatement(stmt tree.Statement) (Plan, error) {
	stmt = rewrite.Rewrite(stmt)
	switch stmt := stmt.(type) {
	case *tree.Select:
		qry := &Query{
			Limit:   -1,
			Offset:  -1,
			RelsMap: make(map[string]*Relation),
		}
		if err := b.buildSelect(stmt, qry); err != nil {
			return nil, err
		}
		qry.backFill()
		return qry, nil
	case *tree.ParenSelect:
		qry := &Query{
			Limit:   -1,
			Offset:  -1,
			RelsMap: make(map[string]*Relation),
		}
		if err := b.buildSelect(stmt.Select, qry); err != nil {
			return nil, err
		}
		qry.backFill()
		return qry, nil
	}
	return nil, errors.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("unexpected statement: '%v'", stmt))
}
