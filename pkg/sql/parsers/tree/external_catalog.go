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

package tree

import "github.com/matrixorigin/matrixone/pkg/common/reuse"

func init() {
	reuse.CreatePool[DropExternalCatalog](
		func() *DropExternalCatalog { return &DropExternalCatalog{} },
		func(d *DropExternalCatalog) { d.reset() },
		reuse.DefaultOptions[DropExternalCatalog](),
	)

	reuse.CreatePool[CreateExternalCatalog](
		func() *CreateExternalCatalog { return &CreateExternalCatalog{} },
		func(c *CreateExternalCatalog) { c.reset() },
		reuse.DefaultOptions[CreateExternalCatalog](),
	)

	reuse.CreatePool[ExternalCatalogOption](
		func() *ExternalCatalogOption { return &ExternalCatalogOption{} },
		func(o *ExternalCatalogOption) { o.reset() },
		reuse.DefaultOptions[ExternalCatalogOption](),
	)
}

type DropExternalCatalog struct {
	statementImpl
	IfExists bool
	Name     Identifier
}

func NewDropExternalCatalog(ifExists bool, name Identifier) *DropExternalCatalog {
	stmt := reuse.Alloc[DropExternalCatalog](nil)
	stmt.IfExists = ifExists
	stmt.Name = name
	return stmt
}

func (node *DropExternalCatalog) Format(ctx *FmtCtx) {
	ctx.WriteString("drop external catalog")
	if node.IfExists {
		ctx.WriteString(" if exists")
	}
	ctx.WriteByte(' ')
	ctx.WriteString(string(node.Name))
}

func (node *DropExternalCatalog) GetStatementType() string { return "Drop External Catalog" }
func (node *DropExternalCatalog) GetQueryType() string     { return QueryTypeDDL }
func (node DropExternalCatalog) TypeName() string          { return "tree.DropExternalCatalog" }

func (node *DropExternalCatalog) Free() {
	reuse.Free[DropExternalCatalog](node, nil)
}

func (node *DropExternalCatalog) reset() {
	*node = DropExternalCatalog{}
}

type CreateExternalCatalog struct {
	statementImpl
	IfNotExists    bool
	Name           Identifier
	ConnectionName Identifier
	Type           string
	Options        []*ExternalCatalogOption
}

func NewCreateExternalCatalog(
	ifNotExists bool,
	name Identifier,
	connectionName Identifier,
	typ string,
	options []*ExternalCatalogOption,
) *CreateExternalCatalog {
	stmt := reuse.Alloc[CreateExternalCatalog](nil)
	stmt.IfNotExists = ifNotExists
	stmt.Name = name
	stmt.ConnectionName = connectionName
	stmt.Type = typ
	stmt.Options = options
	return stmt
}

func (node *CreateExternalCatalog) Format(ctx *FmtCtx) {
	ctx.WriteString("create external catalog")
	if node.IfNotExists {
		ctx.WriteString(" if not exists")
	}
	ctx.WriteByte(' ')
	ctx.WriteString(string(node.Name))
	ctx.WriteString(" using connection ")
	ctx.WriteString(string(node.ConnectionName))
	ctx.WriteString(" type = ")
	writeConnectionQuotedString(ctx, node.Type)
	ctx.WriteString(" options (")
	for i, opt := range node.Options {
		if i > 0 {
			ctx.WriteString(", ")
		}
		opt.Format(ctx)
	}
	ctx.WriteByte(')')
}

func (node *CreateExternalCatalog) GetStatementType() string { return "Create External Catalog" }
func (node *CreateExternalCatalog) GetQueryType() string     { return QueryTypeDDL }
func (node CreateExternalCatalog) TypeName() string          { return "tree.CreateExternalCatalog" }

func (node *CreateExternalCatalog) Free() {
	reuse.Free[CreateExternalCatalog](node, nil)
}

func (node *CreateExternalCatalog) reset() {
	*node = CreateExternalCatalog{}
}

type ExternalCatalogOption struct {
	createOptionImpl
	Key   Identifier
	Value string
}

func NewExternalCatalogOption(key Identifier, value string) *ExternalCatalogOption {
	opt := reuse.Alloc[ExternalCatalogOption](nil)
	opt.Key = key
	opt.Value = value
	return opt
}

func (node *ExternalCatalogOption) Format(ctx *FmtCtx) {
	ctx.WriteString(string(node.Key))
	ctx.WriteString(" = ")
	writeConnectionQuotedString(ctx, node.Value)
}

func (node *ExternalCatalogOption) Free() {
	reuse.Free[ExternalCatalogOption](node, nil)
}

func (node ExternalCatalogOption) TypeName() string { return "tree.ExternalCatalogOption" }

func (node *ExternalCatalogOption) reset() {
	*node = ExternalCatalogOption{}
}
