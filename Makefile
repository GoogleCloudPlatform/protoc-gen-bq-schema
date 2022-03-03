# Copyright 2014 Google Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

BQ_PLUGIN=/usr/local/bin/protoc-gen-bq-schema
GO_PLUGIN=bin/protoc-gen-go
PROTOC_GEN_GO_PKG=github.com/golang/protobuf/protoc-gen-go
GLOG_PKG=github.com/golang/glog
PROTO_SRC=bq_table.proto bq_field.proto
PROTO_GENFILES=protos/bq_table.pb.go protos/bq_field.pb.go
PROTO_PKG=github.com/golang/protobuf/proto
PKGMAP=Mgoogle/protobuf/descriptor.proto=$(PROTOC_GEN_GO_PKG)/descriptor
EXAMPLES_PROTO=examples/foo.proto

install: $(BQ_PLUGIN)

$(BQ_PLUGIN): $(PROTO_GENFILES) goprotobuf glog
	go build -o $@

$(PROTO_GENFILES): $(PROTO_SRC) $(GO_PLUGIN)
	protoc -I. -Ivendor/protobuf --plugin=$(GO_PLUGIN) --go_out=$(PKGMAP):protos $(PROTO_SRC)

goprotobuf:
	go get $(PROTO_PKG)

glog:
	go get $(GLOG_PKG)

$(GO_PLUGIN):
	go get $(PROTOC_GEN_GO_PKG)
	go build -o $@ $(PROTOC_GEN_GO_PKG)

test: $(PROTO_SRC)
	go test

distclean clean:
	go clean
	rm -f $(GO_PLUGIN) $(BQ_PLUGIN)

realclean: distclean
	rm -f $(PROTO_GENFILES)

examples: $(BQ_PLUGIN)
	protoc -I. -Ivendor/protobuf --plugin=$(BQ_PLUGIN) --bq-schema_out=examples $(EXAMPLES_PROTO)

.PHONY: goprotobuf glog