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
syntax = "proto2";
package foo;
import "bq_table.proto";
import "bq_field.proto";

message Bar {
  option (gen_bq_schema.bigquery_opts).table_name = "bar_table";

  message Nested {
    repeated int32 a = 1;
  }

  required int32 a = 1;
  optional Nested b = 2;
  repeated string c = 3;

  optional bool d = 4 [(gen_bq_schema.bigquery).ignore = true];
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

### Example - Policy Tags
Support exists for specifying a [BigQuery Policy Tag](https://cloud.google.com/bigquery/docs/column-level-security-intro) on a field.

Suppose that we have the following `foo.proto` that has `policy_tags` set on the `first_name` field:

```protobuf
syntax = "proto3";
package foo;
import "bq_table.proto";
import "bq_field.proto";

message Bar {
  option (gen_bq_schema.bigquery_opts).table_name = "bar_table";

  required string user = 1;
  optional string first_name = 2 [(gen_bq_schema.bigquery).policy_tags = "pii"];
}
```

`protoc --bq-schema_out=. foo.proto` will generate a file named `foo/bar_table.schema`. The table `bar_table` is defined in `bar_table.schema` and the field with name `first_name` includes the `policyTags` json marking it as a field which is subject to some form of column-level security.

```json
{
  "name": "first_name",
  "type": "STRING",
  "mode": "NULLABLE",
  "policyTags": {
   "names": [
    "pii"
   ]
  }
 }
```

Policy tags must take the form `projects/project-id/locations/location/taxonomies/taxonomy-id/policyTags/policytag-id` and so users can either specify this form in the `.proto` file, or use an alias which refers to an entry in a config file, e.g.

`policy_tags.env`
```dotenv
pii=projects/my-project/locations/EU/taxonomies/142965068551231978/policyTags/projects/uw-partner-dev/locations/eu/taxonomies/142935068551331978/policyTags/9114836628731764131
```

`protoc --bq-schema_out=policy_tags_config_path=policy_tags.env:. foo.proto` will generate a file named `foo/bar_table.schema`, but in this instance the BigQuery field is generated like so:

```json
{
  "name": "first_name",
  "type": "STRING",
  "mode": "NULLABLE",
  "policyTags": {
   "names": [
    "projects/my-project/locations/EU/taxonomies/142965068551231978/policyTags/projects/uw-partner-dev/locations/eu/taxonomies/142935068551331978/policyTags/9114836628731764131"
   ]
  }
 }
```

If no config file is specified as input parameter, or a file is specified by no Policy Tag match is found, then the text provided by the user for the Policy Tag is used.

## Special Case - OneOf Repeated fields of type Empty Message

Take the following example:

```proto
message PartnerPositionApplicationRequiresReviewEvent {
  option (gen_bq_schema.bigquery_opts).table_name = "partner_position_application_requires_review";

  string application_id = 1;
  repeated ApplicationReviewReason reasons = 2;
}

message ApplicationReviewReason {
  oneof reason {
    ReasonAddressMatch address_match = 1;
    ReasonHasCriminalRecord has_criminal_record = 2;
  }
}

message ReasonAddressMatch {
  repeated string address_partner_ids = 1;
}

message ReasonHasCriminalRecord {}
```

The reason `has_criminal_record` is message type `ReasonHasCriminalRecord` which has no fields. In this instance the message is used like a boolean. Rather than discard this field in the BigQuery schema, a field of type `BOOLEAN` is created instead.

## License

protoc-gen-bq-schema is licensed under the Apache License version 2.0.
This is not an official Google product.
