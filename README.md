# protoc-gen-bq-schema


protoc-gen-bq-schema is a plugin for [ProtocolBuffer compiler](https://github.com/google/protobuf).
It converts messages written in .proto format into schema files in JSON for BigQuery.
So you can reuse existing data definitions in .proto for BigQuery with this plugin.

## Installation
 go get github.com/GoogleCloudPlatform/protoc-gen-bq-schema

## Usage
 protoc --bq-schema\_out=path/to/outdir foo.proto

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
package foo;
import "bq_table_name.proto";

message Bar {
  option (gen_bq_schema.table_name) = "bar_table";

  message Nested {
    repeated int32 a = 1;
  }

  required int32 a = 1;
  optional Nested b = 2;
  repeated string c = 3;
}

message Baz {
  required int32 a = 1;
}
```

`protoc --bq-schema_out=. foo.proto` will generate a file named `foo/bar_table.schema`.
The message `foo.Baz` is ignored because it doesn't have option `gen_bq_schema.table_name`.

## License

protoc-gen-bq-schema is licensed under the Apache License version 2.0.
This is not an official Google product.
