// Copyright 2021-present The Atlas Authors. All rights reserved.
// This source code is licensed under the Apache 2.0 license found
// in the LICENSE file in the root directory of this source tree.

package migrate

import (
	"context"
	"strings"

	"ariga.io/atlas/cmd/atlascmd/migrate/ent"
	"ariga.io/atlas/cmd/atlascmd/migrate/ent/revision"
	"ariga.io/atlas/sql/migrate"
	"ariga.io/atlas/sql/schema"
	"ariga.io/atlas/sql/sqlclient"
	"entgo.io/ent/dialect/sql"
	entschema "entgo.io/ent/dialect/sql/schema"
)

// A EntRevisions provides implementation for the migrate.RevisionReadWriter interface.
type EntRevisions struct {
	ac     *sqlclient.Client // underlying Atlas client.
	ec     *ent.Client       // underlying Ent client
	schema string            // name of the schema the revision table resides in
}

// NewEntRevisions creates a new EntRevisions with the given ec.Client.
func NewEntRevisions(ac *sqlclient.Client, schema string) (*EntRevisions, error) {
	if schema == "" {
		schema = "atlas_schema_revisions"
	}
	ec, err := ent.Open(ac.Name, strings.TrimSuffix(ac.URL.DSN, ac.URL.Schema)+schema)
	if err != nil {
		return nil, err
	}
	return &EntRevisions{ac, ec, schema}, nil
}

// Init makes sure the revision table does exist in the connected database.
func (r *EntRevisions) Init(ctx context.Context) error {
	// Create the schema.
	if err := r.ac.ApplyChanges(ctx, []schema.Change{
		&schema.AddSchema{S: &schema.Schema{Name: r.schema}, Extra: []schema.Clause{&schema.IfNotExists{}}},
	}); err != nil {
		return err
	}
	return r.ec.Schema.Create(ctx, entschema.WithAtlas(true))
}

// ReadRevisions reads the revisions from the revisions table.
func (r *EntRevisions) ReadRevisions(ctx context.Context) (migrate.Revisions, error) {
	revs, err := r.ec.Revision.Query().Order(ent.Asc(revision.FieldID)).All(ctx)
	if err != nil {
		return nil, err
	}
	ret := make(migrate.Revisions, len(revs))
	for i, r := range revs {
		ret[i] = &migrate.Revision{
			Version:         r.ID,
			Description:     r.Description,
			ExecutionState:  string(r.ExecutionState),
			ExecutedAt:      r.ExecutedAt,
			ExecutionTime:   r.ExecutionTime,
			Hash:            r.Hash,
			OperatorVersion: r.OperatorVersion,
			Meta:            r.Meta,
		}
	}
	return ret, nil
}

// WriteRevisions writes the revisions to the revisions table.
func (r *EntRevisions) WriteRevisions(ctx context.Context, rs migrate.Revisions) error {
	bulk := make([]*ent.RevisionCreate, len(rs))
	for i, rev := range rs {
		bulk[i] = r.ec.Revision.Create().
			SetID(rev.Version).
			SetDescription(rev.Description).
			SetExecutionState(revision.ExecutionState(rev.ExecutionState)).
			SetExecutedAt(rev.ExecutedAt).
			SetExecutionTime(rev.ExecutionTime).
			SetHash(rev.Hash).
			SetOperatorVersion(rev.OperatorVersion).
			SetMeta(rev.Meta)
	}
	return r.ec.Revision.CreateBulk(bulk...).
		OnConflict(
			sql.ConflictColumns(revision.FieldID),
		).
		UpdateNewValues().
		Exec(ctx)
}

var _ migrate.RevisionReadWriter = (*EntRevisions)(nil)
