// Copyright 2021-present The Atlas Authors. All rights reserved.
// This source code is licensed under the Apache 2.0 license found
// in the LICENSE file in the root directory of this source tree.

package spectest

import (
	"reflect"
	"testing"

	"ariga.io/atlas/schema/schemaspec"
	"ariga.io/atlas/schema/schemaspec/schemahcl"
	"ariga.io/atlas/sql/internal/specutil"
	"ariga.io/atlas/sql/schema"

	"github.com/stretchr/testify/require"
)

// RegistrySanityTest runs a sanity for a TypeRegistry, generated a dummy *schemaspec.Type
// then converting it to a schema.Type and back to a *schemaspec.Type.
func RegistrySanityTest(t *testing.T, registry *schemahcl.TypeRegistry, skip []string) {
	for _, ts := range registry.Specs() {
		if contains(ts.Name, skip) {
			continue
		}
		t.Run(ts.Name, func(t *testing.T) {
			spec := dummyType(t, ts)
			styp, err := registry.Type(spec, nil)
			require.NoError(t, err)
			require.NoErrorf(t, err, "failed formatting: %styp", err)
			convert, err := registry.Convert(styp)
			require.NoError(t, err)
			after, err := registry.Type(convert, nil)
			require.NoError(t, err)
			require.EqualValues(t, styp, after)
		})
	}
}

// TestInputVars runs a test verifying that the driver's exposed Eval function uses
// input variables properly.
func TestInputVars(t *testing.T, evaluator schemahcl.Evaluator) {
	h := `
variable "tenant" {
	type = string
	default = "test"
}
schema "tenant" {
	name = var.tenant
}
table "users" {
	schema = schema.tenant
	column "id" {
		type = int
	}
	index "user_name" {
    on {
      column = column.id
      unique = true
    }
  }
}
`
	var test schema.Realm
	err := evaluator.Eval([]byte(h), &test, map[string]string{"tenant": "rotemtam"})
	require.NoError(t, err)
	require.EqualValues(t, "rotemtam", test.Schemas[0].Name)
	require.Len(t, test.Schemas[0].Tables, 1)
}

func contains(s string, l []string) bool {
	for i := range l {
		if s == l[i] {
			return true
		}
	}
	return false
}

func dummyType(t *testing.T, ts *schemaspec.TypeSpec) *schemaspec.Type {
	spec := &schemaspec.Type{T: ts.T}
	for _, attr := range ts.Attributes {
		var a *schemaspec.Attr
		switch attr.Kind {
		case reflect.Int, reflect.Int64:
			a = specutil.LitAttr(attr.Name, "2")
		case reflect.String:
			a = specutil.LitAttr(attr.Name, `"a"`)
		case reflect.Slice:
			a = specutil.ListAttr(attr.Name, `"a"`, `"b"`)
		case reflect.Bool:
			a = specutil.LitAttr(attr.Name, "false")
		default:
			t.Fatalf("unsupported kind: %s", attr.Kind)
		}
		spec.Attrs = append(spec.Attrs, a)
	}
	return spec
}
