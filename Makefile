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

BQ_PLUGIN=bin/protoc-gen-bq-schema
GO_PLUGIN=bin/protoc-gen-go
PROTOC_GEN_GO_PKG=github.com/golang/protobuf/protoc-gen-go
GLOG_PKG=github.com/golang/glog
PROTO_PKG=github.com/golang/protobuf/proto
PKGMAP=Mgoogle/protobuf/descriptor.proto=$(PROTOC_GEN_GO_PKG)/descriptor

install: $(BQ_PLUGIN)

$(BQ_PLUGIN): bq_table.pb.go bq_field.pb.go goprotobuf glog
	go build -o $@

bq_table.pb.go bq_field.pb.go: bq_table.proto bq_field.proto $(GO_PLUGIN)
	protoc -I. -Ivendor/protobuf --plugin=bin/protoc-gen-go --go_out=$(PKGMAP):protos bq_table.proto bq_field.proto

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
	rm -f bq_table.pb.go bq_field.pb.go

.PHONY: goprotobuf glog
