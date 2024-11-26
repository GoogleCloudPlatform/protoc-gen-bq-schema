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

// protoc plugin which converts .proto to schema for BigQuery.
// It is spawned by protoc and generates schema for BigQuery, encoded in JSON.
//
// usage:
//  $ bin/protoc --bq-schema_out=path/to/outdir foo.proto
//

// Protobuf code for extensions are generated --
//go:generate protoc --go_out=. --go_opt=module=github.com/GoogleCloudPlatform/protoc-gen-bq-schema bq_table.proto bq_field.proto

package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/GoogleCloudPlatform/protoc-gen-bq-schema/v2/pkg/converter"
	"github.com/golang/glog"
	"google.golang.org/protobuf/proto"
	plugin "google.golang.org/protobuf/types/pluginpb"
)

func main() {
	flag.Parse()
	ok := true
	glog.Info("Processing code generator request")
	res, err := converter.ConvertFrom(os.Stdin)
	if err != nil {
		ok = false
		if res == nil {
			message := fmt.Sprintf("Failed to read input: %v", err)
			res = &plugin.CodeGeneratorResponse{
				Error: &message,
			}
		}
	}

	glog.Info("Serializing code generator response")
	data, err := proto.Marshal(res)
	if err != nil {
		glog.Fatal("Cannot marshal response", err)
	}
	_, err = os.Stdout.Write(data)
	if err != nil {
		glog.Fatal("Failed to write response", err)
	}

	if ok {
		glog.Info("Succeeded to process code generator request")
	} else {
		glog.Info("Failed to process code generator but successfully sent the error to protoc")
	}
}
