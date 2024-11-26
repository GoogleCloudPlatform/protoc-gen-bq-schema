# protoc-gen-bq-schema


protoc-gen-bq-schema is a plugin for [ProtocolBuffer compiler](https://github.com/google/protobuf).
It converts messages written in .proto format into schema files in JSON for BigQuery.
So you can reuse existing data definitions in .proto for BigQuery with this plugin.

## Installation

```sh
go install github.com/GoogleCloudPlatform/protoc-gen-bq-schema/v2@latest
```

## Usage
 protoc --bq-schema\_out=path/to/outdir \[--bq-schema_opt=single-message\] foo.proto

`protoc` and `protoc-gen-bq-schema` commands must be found in $PATH.

The generated JSON schema files are suffixed with `.schema` and their base names are named
after their package names and `bq_table_name` options.

If you do not already have the standard google protobuf libraries in your `proto_path`, you'll need to specify them directly on the command line (and potentially need to copy `bq_schema.proto` into a proto_path directory as well), like this:

```sh
protoc --bq-schema_out=path/to/out/dir foo.proto --proto_path=. --proto_path=<path_to_google_proto_folder>/src
```

### Example
Suppose that we have the following foo.proto.

```protobuf
syntax = "proto2";
package foo;
import "bq_table.proto";
import "bq_field.proto";

message Bar {
  option (gen_bq_schema.bigquery_opts).table_name = "bar_table";

  message Nested {
    repeated int32 a = 1;
  }

  // Description of field a -- this is an int32
  required int32 a = 1;

  // Nested b structure
  optional Nested b = 2;

  // Repeated c string
  repeated string c = 3;

  optional bool d = 4 [(gen_bq_schema.bigquery).ignore = true];

  // TIMESTAMP (uint64 in proto) - required in BigQuery
  optional uint64 e = 5 [
    (gen_bq_schema.bigquery) = {
      require: true
      type_override: 'TIMESTAMP'
    }
  ];
}

message Baz {
  required int32 a = 1;
}
```

`protoc --bq-schema_out=. foo.proto` will generate a file named `foo/bar_table.schema`.
The message `foo.Baz` is ignored because it doesn't have option `gen_bq_schema.bigquery_opts`.

`protoc --bq-schema_out=. --bq-schema_opt=single-message single_message.proto` will generate a file named `foo/single_message.schema`.
The message `foo.Baz` is also ignored because it is not the first message in the file.


### Support for PolicyTags
`protoc-gen-bq-schema` now supports [policyTags](https://cloud.google.com/bigquery/docs/column-level-security-intro).
You can define a `Policy Tag` for a field in `.proto` file.

### Example with Policy Tags
Suppose that you have the following `test_table.proto`
```protobuf
syntax = "proto3";
package foo;
import "bq_table.proto";
import "bq_field.proto";

message TestTable{
    option (gen_bq_schema.bigquery_opts).table_name = "test_table";

    int32 a = 1 [
        (gen_bq_schema.bigquery) = {
          require: true
          policy_tags : "private"
        }
      ];

    string b = 2 [(gen_bq_schema.bigquery).policy_tags="public"];

    message Nested {
        int32 a = 1 [(gen_bq_schema.bigquery) = {
            require: true
            policy_tags : "private"
            }
        ];

        string b = 2;
    }

    repeated Nested nested = 3 [(gen_bq_schema.bigquery).require = true];

    message EmptyMessage {}

    repeated EmptyMessage hasMessage = 4;
}
```
`protoc --bq-schema_out=. test_table.proto` will generate a file named `foo/test_table.schema`.
The field `hasMessage` is ignored because the message `EmptyMessage` is empty.

It will generate the following `JSON` schema
```json
[
 {
  "name": "a",
  "type": "INTEGER",
  "mode": "REQUIRED",
  "policyTags": {
   "names": [
    "private"
   ]
  }
 },
 {
  "name": "b",
  "type": "STRING",
  "mode": "NULLABLE",
  "policyTags": {
   "names": [
    "public"
   ]
  }
 },
 {
  "name": "nested",
  "type": "RECORD",
  "mode": "REQUIRED",
  "fields": [
   {
    "name": "a",
    "type": "INTEGER",
    "mode": "REQUIRED",
    "policyTags": {
     "names": [
      "private"
     ]
    }
   },
   {
    "name": "b",
    "type": "STRING",
    "mode": "NULLABLE"
   }
  ]
 }
]
```

The policy tag name provided in `test_table.proto` file is taken as it is. According to [Google Docs](https://cloud.google.com/bigquery/docs/column-level-security-intro),
the policy tag string should be of the following format

`projects/project-id/locations/location/taxonomies/taxonomy-id/policyTags/policytag-id`


## License

protoc-gen-bq-schema is licensed under the Apache License version 2.0.
This is not an official Google product.
