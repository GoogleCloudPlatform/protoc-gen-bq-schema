// Copyright 2018 Google Inc. All rights reserved.
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

package converter

import (
	"testing"
)

func TestIgnore(t *testing.T) {
	testConvert(t, `
		file_to_generate: "foo.proto"
		proto_file <
			name: "foo.proto"
			package: "example_package"
			message_type <
				name: "FooProto"
				field <
					name: "i1"
					number: 1
					type: TYPE_INT32
					label: LABEL_OPTIONAL
				>
				field <
					name: "i2"
					number: 2
					type: TYPE_INT32
					label: LABEL_OPTIONAL
					options <
						[gen_bq_schema.bigquery] <
							ignore: true
						>
					>
				>
				options <
					[gen_bq_schema.bigquery_opts]: <
						table_name: "foo_table"
					>
				>
			>
		>
	`, map[string]string{
		"example_package/foo_table.schema": `[
			{ "name": "i1", "type": "INTEGER", "mode": "NULLABLE"}
		]`,
	})
}

func TestRequire(t *testing.T) {
	testConvert(t, `
		file_to_generate: "foo.proto"
		proto_file <
			name: "foo.proto"
			package: "example_package"
			message_type <
				name: "FooProto"
				field <
					name: "i1"
					number: 1
					type: TYPE_INT32
					label: LABEL_OPTIONAL
					options <
						[gen_bq_schema.bigquery] <
							require: true
						>
					>
				>
				options <
					[gen_bq_schema.bigquery_opts]: <
						table_name: "foo_table"
					>
				>
			>
		>
	`, map[string]string{
		"example_package/foo_table.schema": `[
			{ "name": "i1", "type": "INTEGER", "mode": "REQUIRED"}
		]`,
	})
}

func TestTypeOverride(t *testing.T) {
	testConvert(t, `
		file_to_generate: "foo.proto"
		proto_file <
			name: "foo.proto"
			package: "example_package"
			message_type <
				name: "FooProto"
				field <
					name: "i1"
					number: 1
					type: TYPE_INT32
					label: LABEL_OPTIONAL
					options <
						[gen_bq_schema.bigquery] <
							type_override: "FLOAT"
						>
					>
				>
				options <
					[gen_bq_schema.bigquery_opts]: <
						table_name: "foo_table"
					>
				>
			>
		>
	`, map[string]string{
		"example_package/foo_table.schema": `[
			{ "name": "i1", "type": "FLOAT", "mode": "NULLABLE"}
		]`,
	})
}

func TestDescription(t *testing.T) {
	testConvert(t, `
		file_to_generate: "foo.proto"
		proto_file <
			name: "foo.proto"
			package: "example_package"
			message_type <
				name: "FooProto"
				field <
					name: "i1"
					number: 1
					type: TYPE_INT32
					label: LABEL_OPTIONAL
					options <
						[gen_bq_schema.bigquery] <
							description: "bar"
						>
					>
				>
				options <
					[gen_bq_schema.bigquery_opts]: <
						table_name: "foo_table"
					>
				>
			>
		>
	`, map[string]string{
		"example_package/foo_table.schema": `[
			{ "name": "i1", "type": "INTEGER", "mode": "NULLABLE", "description": "bar"}
		]`,
	})
}

func TestNameOverride(t *testing.T) {
	testConvert(t, `
		file_to_generate: "foo.proto"
		proto_file <
			name: "foo.proto"
			package: "example_package"
			message_type <
				name: "FooProto"
				field <
					name: "i1"
					number: 1
					type: TYPE_INT32
					label: LABEL_OPTIONAL
					options <
						[gen_bq_schema.bigquery] <
							name: "Integer1"
						>
					>
				>
				options <
					[gen_bq_schema.bigquery_opts]: <
						table_name: "foo_table"
					>
				>
			>
		>
	`, map[string]string{
		"example_package/foo_table.schema": `[
			{ "name": "Integer1", "type": "INTEGER", "mode": "NULLABLE"}
		]`,
	})
}

func TestJsonNames(t *testing.T) {
	testConvert(t, `
		file_to_generate: "foo.proto"
		proto_file <
			name: "foo.proto"
			package: "example_package"
			message_type <
				name: "FooProto"
				field <
					name: "i1"
					number: 1
					type: TYPE_INT32
					label: LABEL_OPTIONAL
					json_name: "int1"
				>
				field <
					name: "i2"
					number: 2
					type: TYPE_INT32
					label: LABEL_OPTIONAL
				>
				options <
					[gen_bq_schema.bigquery_opts]: <
						table_name: "foo_table"
						use_json_names: true
					>
				>
			>
		>
	`, map[string]string{
		"example_package/foo_table.schema": `[
			{ "name": "int1", "type": "INTEGER", "mode": "NULLABLE"},
			{ "name": "i2", "type": "INTEGER", "mode": "NULLABLE"}
		]`,
	})
}

func TestPolicyTags(t *testing.T) {
	testConvert(t, `
		file_to_generate: "foo.proto"
		proto_file <
			name: "foo.proto"
			package: "example_package"
			message_type <
				name: "FooProto"
				field <
					name: "i1"
					number: 1
					type: TYPE_INT32
					label: LABEL_OPTIONAL
					json_name: "int11"
				>
				field <
					name: "i2"
					number: 2
					type: TYPE_INT32
					label: LABEL_OPTIONAL
					options <
						[gen_bq_schema.bigquery]: <
							policy_tags: "pii"
						>
					>
				>
				options <
					[gen_bq_schema.bigquery_opts]: <
						table_name: "foo_table"
						use_json_names: true
					>
				>
			>
		>
	`, map[string]string{
		"example_package/foo_table.schema": `[
			{ "name": "int11", "type": "INTEGER", "mode": "NULLABLE"},
			{ "name": "i2", "type": "INTEGER", "mode": "NULLABLE", "policyTags": {"names": ["pii"]}}
		]`,
	})
}
