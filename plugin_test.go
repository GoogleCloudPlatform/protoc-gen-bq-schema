// Copyright 2014 Google Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/golang/protobuf/proto"

	plugin "github.com/golang/protobuf/protoc-gen-go/plugin"
)

// schema is an internal representation of generated BigQuery schema
type schema []map[string]interface{}

func joinNames(targets map[string]*schema) (result string) {
	sep := ""
	for name := range targets {
		result += sep + name
		sep = ", "
	}
	return
}

func testConvert(t *testing.T, input string, expectedOutputs map[string]string) {
	req := plugin.CodeGeneratorRequest{}
	if err := proto.UnmarshalText(input, &req); err != nil {
		t.Fatal("Failed to parse test input: ", err)
	}
	expectedSchema := make(map[string]*schema)
	for filename, data := range expectedOutputs {
		parsed := new(schema)
		if err := json.Unmarshal([]byte(data), parsed); err != nil {
			t.Fatalf("Failed to parse an expectation: %s: %v", data, err)
		}
		expectedSchema[filename] = parsed
	}

	res, err := convert(&req)
	if err != nil {
		t.Fatal("Conversion failed. ", err)
	}
	if res.Error != nil {
		t.Fatal("Conversion failed. ", res.Error)
	}

	actualSchema := make(map[string]*schema)
	for _, file := range res.GetFile() {
		s := &schema{}
		if err := json.Unmarshal([]byte(file.GetContent()), s); err != nil {
			t.Fatalf("Expected to be a valid JSON, but wasn't %s: %v", file.GetContent(), err)
		}
		actualSchema[file.GetName()] = s
	}

	if len(actualSchema) != len(expectedSchema) {
		t.Errorf("Expected %d files generated, but actually %d files:\nExpectation: %s\n Actual: %s",
			len(expectedSchema), len(actualSchema), joinNames(expectedSchema), joinNames(actualSchema))
	}

	for name, actual := range actualSchema {
		expected, ok := expectedSchema[name]
		if !ok {
			t.Error("Unexpected file generated: ", name)
		}
		if !reflect.DeepEqual(expected, actual) {
			t.Errorf("Expected the content of %s to be \"%v\" but got \"%v\"", name, expected, actual)
		}
	}
}

// TestSimple tries a simple code generator request.
func TestSimple(t *testing.T) {
	testConvert(t, `
			file_to_generate: "foo.proto"
			proto_file <
				name: "foo.proto"
				package: "example_package.nested"
				message_type <
					name: "FooProto"
					field < name: "i1" number: 1 type: TYPE_INT32 label: LABEL_OPTIONAL >
					options < [gen_bq_schema.table_name]: "foo_table" >
				>
			>
		`,
		map[string]string{
			"example_package/nested/foo_table.schema": `[
				{ "name": "i1", "type": "INTEGER", "mode": "NULLABLE" }
			]`,
		})
}

// TestIgnoreNonTargetMessage checks if the generator ignores messages without gen_bq_schema.table_name option.
func TestIgnoreNonTargetMessage(t *testing.T) {
	testConvert(t, `
			file_to_generate: "foo.proto"
			proto_file <
				name: "foo.proto"
				package: "example_package.nested"
				message_type <
					name: "FooProto"
					field < name: "i1" number: 1 type: TYPE_INT32 label: LABEL_OPTIONAL >
				>
				message_type <
					name: "BarProto"
					field < name: "i1" number: 1 type: TYPE_INT32 label: LABEL_OPTIONAL >
					options < [gen_bq_schema.table_name]: "bar_table" >
				>
				message_type <
					name: "BazProto"
					field < name: "i1" number: 1 type: TYPE_INT32 label: LABEL_OPTIONAL >
				>
			>
		`,
		map[string]string{
			"example_package/nested/bar_table.schema": `[
				{ "name": "i1", "type": "INTEGER", "mode": "NULLABLE" }
			]`,
		})
}

// TestIgnoreNonTargetFile checks if the generator ignores messages in non target files.
func TestIgnoreNonTargetFile(t *testing.T) {
	testConvert(t, `
			file_to_generate: "foo.proto"
			proto_file <
				name: "foo.proto"
				package: "example_package.nested"
				message_type <
					name: "FooProto"
					field < name: "i1" number: 1 type: TYPE_INT32 label: LABEL_OPTIONAL >
					options < [gen_bq_schema.table_name]: "foo_table" >
				>
			>
			proto_file <
				name: "bar.proto"
				package: "example_package.nested"
				message_type <
					name: "BarProto"
					field < name: "i1" number: 1 type: TYPE_INT32 label: LABEL_OPTIONAL >
					options < [gen_bq_schema.table_name]: "bar_table" >
				>
			>
		`,
		map[string]string{
			"example_package/nested/foo_table.schema": `[
				{ "name": "i1", "type": "INTEGER", "mode": "NULLABLE" }
			]`,
		})
}

// TestTypes tests the generator with various field types
func TestTypes(t *testing.T) {
	testConvert(t, `
			file_to_generate: "foo.proto"
			proto_file <
				name: "foo.proto"
				package: "example_package.nested"
				message_type <
					name: "FooProto"
					field < name: "i32" number: 1 type: TYPE_INT32 label: LABEL_OPTIONAL >
					field < name: "i64" number: 2 type: TYPE_INT64 label: LABEL_OPTIONAL >
					field < name: "ui32" number: 3 type: TYPE_UINT32 label: LABEL_OPTIONAL >
					field < name: "ui64" number: 4 type: TYPE_UINT64 label: LABEL_OPTIONAL >
					field < name: "si32" number: 5 type: TYPE_SINT32 label: LABEL_OPTIONAL >
					field < name: "si64" number: 6 type: TYPE_SINT64 label: LABEL_OPTIONAL >
					field < name: "ufi32" number: 7 type: TYPE_FIXED32 label: LABEL_OPTIONAL >
					field < name: "ufi64" number: 8 type: TYPE_FIXED64 label: LABEL_OPTIONAL >
					field < name: "sfi32" number: 9 type: TYPE_SFIXED32 label: LABEL_OPTIONAL >
					field < name: "sfi64" number: 10 type: TYPE_SFIXED64 label: LABEL_OPTIONAL >
					field < name: "d" number: 11 type: TYPE_DOUBLE label: LABEL_OPTIONAL >
					field < name: "f" number: 12 type: TYPE_FLOAT label: LABEL_OPTIONAL >
					field < name: "bool" number: 16 type: TYPE_BOOL label: LABEL_OPTIONAL >
					field < name: "str" number: 13 type: TYPE_STRING label: LABEL_OPTIONAL >
					field < name: "bytes" number: 14 type: TYPE_BYTES label: LABEL_OPTIONAL >
					field <
						name: "enum1" number: 15 type: TYPE_ENUM label: LABEL_OPTIONAL
						type_name: ".example_package.nested.FooProto.Enum1"
					>
					field <
						name: "enum2" number: 16 type: TYPE_ENUM label: LABEL_OPTIONAL
						type_name: "FooProto.Enum1"
					>
					field <
						name: "grp1" number: 17 type: TYPE_GROUP label: LABEL_OPTIONAL
						type_name: ".example_package.nested.FooProto.Group1"
					>
					field <
						name: "grp2" number: 18 type: TYPE_GROUP label: LABEL_OPTIONAL
						type_name: "FooProto.Group1"
					>
					field <
						name: "msg1" number: 19 type: TYPE_MESSAGE label: LABEL_OPTIONAL
						type_name: ".example_package.nested.FooProto.Nested1"
					>
					field <
						name: "msg2" number: 20 type: TYPE_MESSAGE label: LABEL_OPTIONAL
						type_name: "FooProto.Nested1"
					>
					field <
						name: "msg3" number: 21 type: TYPE_MESSAGE label: LABEL_OPTIONAL
						type_name: ".example_package.nested2.BarProto"
					>
					field <
						name: "msg4" number: 22 type: TYPE_MESSAGE label: LABEL_OPTIONAL
						type_name: "nested2.BarProto"
					>
					field <
						name: "msg2" number: 23 type: TYPE_MESSAGE label: LABEL_OPTIONAL
						type_name: "FooProto.EmptyNested1"
					>
					nested_type <
						name: "Group1"
						field < name: "i1" number: 1 type: TYPE_INT32 label: LABEL_OPTIONAL >
					>
					nested_type <
						name: "Nested1"
						field < name: "i1" number: 1 type: TYPE_INT32 label: LABEL_OPTIONAL >
					>
					nested_type <
						name: "EmptyNested1"
					>
					enum_type < name: "Enum1" value < name: "E1" number: 1 > value < name: "E2" number: 2 > >
					options < [gen_bq_schema.table_name]: "foo_table" >
				>
			>
			proto_file <
				name: "bar.proto"
				package: "example_package.nested2"
				message_type <
					name: "BarProto"
					field < name: "i1" number: 1 type: TYPE_INT32 label: LABEL_OPTIONAL >
					field < name: "i2" number: 2 type: TYPE_INT32 label: LABEL_OPTIONAL >
					field < name: "i3" number: 3 type: TYPE_INT32 label: LABEL_OPTIONAL >
				>
			>
		`,
		map[string]string{
			"example_package/nested/foo_table.schema": `[
				{ "name": "i32", "type": "INTEGER", "mode": "NULLABLE" },
				{ "name": "i64", "type": "INTEGER", "mode": "NULLABLE" },
				{ "name": "ui32", "type": "INTEGER", "mode": "NULLABLE" },
				{ "name": "ui64", "type": "INTEGER", "mode": "NULLABLE" },
				{ "name": "si32", "type": "INTEGER", "mode": "NULLABLE" },
				{ "name": "si64", "type": "INTEGER", "mode": "NULLABLE" },
				{ "name": "ufi32", "type": "INTEGER", "mode": "NULLABLE" },
				{ "name": "ufi64", "type": "INTEGER", "mode": "NULLABLE" },
				{ "name": "sfi32", "type": "INTEGER", "mode": "NULLABLE" },
				{ "name": "sfi64", "type": "INTEGER", "mode": "NULLABLE" },
				{ "name": "d", "type": "FLOAT", "mode": "NULLABLE" },
				{ "name": "f", "type": "FLOAT", "mode": "NULLABLE" },
				{ "name": "bool", "type": "BOOLEAN", "mode": "NULLABLE" },
				{ "name": "str", "type": "STRING", "mode": "NULLABLE" },
				{ "name": "bytes", "type": "BYTES", "mode": "NULLABLE" },
				{ "name": "enum1", "type": "STRING", "mode": "NULLABLE" },
				{ "name": "enum2", "type": "STRING", "mode": "NULLABLE" },
				{
					"name": "grp1", "type": "RECORD", "mode": "NULLABLE",
					"fields": [{ "name": "i1", "type": "INTEGER", "mode": "NULLABLE" }]
				},
				{
					"name": "grp2", "type": "RECORD", "mode": "NULLABLE",
					"fields": [{ "name": "i1", "type": "INTEGER", "mode": "NULLABLE" }]
				},
				{
					"name": "msg1", "type": "RECORD", "mode": "NULLABLE",
					"fields": [{ "name": "i1", "type": "INTEGER", "mode": "NULLABLE" }]
				},
				{
					"name": "msg2", "type": "RECORD", "mode": "NULLABLE",
					"fields": [{ "name": "i1", "type": "INTEGER", "mode": "NULLABLE" }]
				},
				{
					"name": "msg3", "type": "RECORD", "mode": "NULLABLE",
					"fields": [
						{ "name": "i1", "type": "INTEGER", "mode": "NULLABLE" },
						{ "name": "i2", "type": "INTEGER", "mode": "NULLABLE" },
						{ "name": "i3", "type": "INTEGER", "mode": "NULLABLE" }
					]
				},
				{
					"name": "msg4", "type": "RECORD", "mode": "NULLABLE",
					"fields": [
						{ "name": "i1", "type": "INTEGER", "mode": "NULLABLE" },
						{ "name": "i2", "type": "INTEGER", "mode": "NULLABLE" },
						{ "name": "i3", "type": "INTEGER", "mode": "NULLABLE" }
					]
				}
			]`,
		})
}

// TestWellKnownTypes tests the generator with various well-known message types
// which have custom JSON serialization.
func TestWellKnownTypes(t *testing.T) {
	testConvert(t, `
			file_to_generate: "foo.proto"
			proto_file <
				name: "foo.proto"
				package: "example_package"
				message_type <
					name: "FooProto"
					field <
						name: "i32" number: 1 type: TYPE_MESSAGE label: LABEL_OPTIONAL
						type_name: ".google.protobuf.Int32Value"
					>
					field <
						name: "i64" number: 2 type: TYPE_MESSAGE label: LABEL_OPTIONAL
						type_name: ".google.protobuf.Int64Value"
					>
					field <
						name: "ui32" number: 3 type: TYPE_MESSAGE label: LABEL_OPTIONAL
						type_name: ".google.protobuf.UInt32Value"
					>
					field <
						name: "ui64" number: 4 type: TYPE_MESSAGE label: LABEL_OPTIONAL
						type_name: ".google.protobuf.UInt64Value"
					>
					field <
						name: "d" number: 5 type: TYPE_MESSAGE label: LABEL_OPTIONAL
						type_name: ".google.protobuf.DoubleValue"
					>
					field <
						name: "f" number: 6 type: TYPE_MESSAGE label: LABEL_OPTIONAL
						type_name: ".google.protobuf.FloatValue"
					>
					field <
						name: "bool" number: 7 type: TYPE_MESSAGE label: LABEL_OPTIONAL
						type_name: ".google.protobuf.BoolValue"
					>
					field <
						name: "str" number: 8 type: TYPE_MESSAGE label: LABEL_OPTIONAL
						type_name: ".google.protobuf.StringValue"
					>
					field <
						name: "bytes" number: 9 type: TYPE_MESSAGE label: LABEL_OPTIONAL
						type_name: ".google.protobuf.BytesValue"
					>
					field <
						name: "du" number: 10 type: TYPE_MESSAGE label: LABEL_OPTIONAL
						type_name: ".google.protobuf.Duration"
					>
					field <
						name: "t" number: 11 type: TYPE_MESSAGE label: LABEL_OPTIONAL
						type_name: ".google.protobuf.Timestamp"
					>
					options < [gen_bq_schema.table_name]: "foo_table" >
				>
			>
		`,
		map[string]string{
			"example_package/foo_table.schema": `[
				{ "name": "i32", "type": "INTEGER", "mode": "NULLABLE" },
				{ "name": "i64", "type": "INTEGER", "mode": "NULLABLE" },
				{ "name": "ui32", "type": "INTEGER", "mode": "NULLABLE" },
				{ "name": "ui64", "type": "INTEGER", "mode": "NULLABLE" },
				{ "name": "d", "type": "FLOAT", "mode": "NULLABLE" },
				{ "name": "f", "type": "FLOAT", "mode": "NULLABLE" },
				{ "name": "bool", "type": "BOOLEAN", "mode": "NULLABLE" },
				{ "name": "str", "type": "STRING", "mode": "NULLABLE" },
				{ "name": "bytes", "type": "BYTES", "mode": "NULLABLE" },
				{ "name": "du", "type": "STRING", "mode": "NULLABLE" },
				{ "name": "t", "type": "TIMESTAMP", "mode": "NULLABLE" }
			]`,
		})
}

// TestModes tests the generator with different label values.
func TestModes(t *testing.T) {
	testConvert(t, `
			file_to_generate: "foo.proto"
			proto_file <
				name: "foo.proto"
				package: "example_package.nested"
				message_type <
					name: "FooProto"
					field < name: "i1" number: 1 type: TYPE_INT32 label: LABEL_OPTIONAL >
					field < name: "i2" number: 2 type: TYPE_INT32 label: LABEL_REQUIRED >
					field < name: "i3" number: 3 type: TYPE_INT32 label: LABEL_REPEATED >
					options < [gen_bq_schema.table_name]: "foo_table" >
				>
			>
		`,
		map[string]string{
			"example_package/nested/foo_table.schema": `[
				{ "name": "i1", "type": "INTEGER", "mode": "NULLABLE" },
				{ "name": "i2", "type": "INTEGER", "mode": "REQUIRED" },
				{ "name": "i3", "type": "INTEGER", "mode": "REPEATED" }
			]`,
		})
}
