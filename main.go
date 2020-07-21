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
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"strings"

	"github.com/utilitywarehouse/protoc-gen-bq-schema/protos"

	"github.com/golang/glog"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/protoc-gen-go/descriptor"
	plugin "github.com/golang/protobuf/protoc-gen-go/plugin"
)

const (
	messagePath    = 4 // FileDescriptorProto.message_type
	fieldPath      = 2 // DescriptorProto.field
	subMessagePath = 3 // DescriptorProto.nested_type
)

var globalPkg = &ProtoPackage{
	name:     "",
	parent:   nil,
	children: make(map[string]*ProtoPackage),
	types:    make(map[string]*descriptor.DescriptorProto),
	path:     make(map[string]string),
}

// Field describes the schema of a field in BigQuery.
type Field struct {
	Name        string      `json:"name"`
	Type        string      `json:"type"`
	Mode        string      `json:"mode"`
	PolicyTags  *PolicyTags `json:"policyTags,omitempty"`
	Description string      `json:"description,omitempty"`
	Fields      []*Field    `json:"fields,omitempty"`
}

type PolicyTags struct {
	Names []string `json:"names,omitempty"`
}

// ProtoPackage describes a package of Protobuf, which is an container of message types.
type ProtoPackage struct {
	name     string
	parent   *ProtoPackage
	children map[string]*ProtoPackage
	types    map[string]*descriptor.DescriptorProto
	path     map[string]string
}

func registerType(pkgName *string, msg *descriptor.DescriptorProto, path string) {
	pkg := globalPkg
	if pkgName != nil {
		for _, node := range strings.Split(*pkgName, ".") {
			if pkg == globalPkg && node == "" {
				// Skips leading "."
				continue
			}
			child, ok := pkg.children[node]
			if !ok {
				child = &ProtoPackage{
					name:     pkg.name + "." + node,
					parent:   pkg,
					children: make(map[string]*ProtoPackage),
					types:    make(map[string]*descriptor.DescriptorProto),
					path:     make(map[string]string),
				}
				pkg.children[node] = child
			}
			pkg = child
		}
	}
	pkg.types[msg.GetName()] = msg
	pkg.path[msg.GetName()] = path
}

func (pkg *ProtoPackage) lookupType(name string) (*descriptor.DescriptorProto, bool, string) {
	if strings.HasPrefix(name, ".") {
		return globalPkg.relativelyLookupType(name[1:len(name)])
	}

	for ; pkg != nil; pkg = pkg.parent {
		if desc, ok,  path := pkg.relativelyLookupType(name); ok {
			return desc, ok, path
		}
	}
	return nil, false, ""
}

func relativelyLookupNestedType(desc *descriptor.DescriptorProto, name string) (*descriptor.DescriptorProto, bool, string) {
	components := strings.Split(name, ".")
	path := ""
componentLoop:
	for _, component := range components {
		for nestedIndex, nested := range desc.GetNestedType() {
			if nested.GetName() == component {
				desc = nested
				path = fmt.Sprintf("%s.%d.%d", path, subMessagePath, nestedIndex)
				continue componentLoop
			}
		}
		glog.Infof("no such nested message %s in %s", component, desc.GetName())
		return nil, false, ""
	}
	return desc, true, strings.Trim(path, ".")
}

func (pkg *ProtoPackage) relativelyLookupType(name string) (*descriptor.DescriptorProto, bool, string) {
	components := strings.SplitN(name, ".", 2)
	switch len(components) {
	case 0:
		glog.V(1).Info("empty message name")
		return nil, false, ""
	case 1:
		found, ok := pkg.types[components[0]]
		return found, ok, pkg.path[components[0]]
	case 2:
		glog.Infof("looking for %s in %s at %s (%v)", components[1], components[0], pkg.name, pkg)
		if child, ok := pkg.children[components[0]]; ok {
			found, ok,  path := child.relativelyLookupType(components[1])
			return found, ok,  path
		}
		if msg, ok := pkg.types[components[0]]; ok {
			found, ok, path := relativelyLookupNestedType(msg, components[1])
			return found, ok, pkg.path[components[0]] + "." + path
		}
		glog.V(1).Infof("no such package nor message %s in %s", components[0], pkg.name)
		return nil, false,  ""
	default:
		glog.Fatal("not reached")
		return nil, false, ""
	}
}

func (pkg *ProtoPackage) relativelyLookupPackage(name string) (*ProtoPackage, bool) {
	components := strings.Split(name, ".")
	for _, c := range components {
		var ok bool
		pkg, ok = pkg.children[c]
		if !ok {
			return nil, false
		}
	}
	return pkg, true
}

var (
	typeFromWKT = map[string]string{
		".google.protobuf.Int32Value":  "INTEGER",
		".google.protobuf.Int64Value":  "INTEGER",
		".google.protobuf.UInt32Value": "INTEGER",
		".google.protobuf.UInt64Value": "INTEGER",
		".google.protobuf.DoubleValue": "FLOAT",
		".google.protobuf.FloatValue":  "FLOAT",
		".google.protobuf.BoolValue":   "BOOLEAN",
		".google.protobuf.StringValue": "STRING",
		".google.protobuf.BytesValue":  "BYTES",
		".google.protobuf.Duration":    "STRING",
		".google.protobuf.Timestamp":   "TIMESTAMP",
	}
	typeFromFieldType = map[descriptor.FieldDescriptorProto_Type]string{
		descriptor.FieldDescriptorProto_TYPE_DOUBLE: "FLOAT",
		descriptor.FieldDescriptorProto_TYPE_FLOAT:  "FLOAT",

		descriptor.FieldDescriptorProto_TYPE_INT64:    "INTEGER",
		descriptor.FieldDescriptorProto_TYPE_UINT64:   "INTEGER",
		descriptor.FieldDescriptorProto_TYPE_INT32:    "INTEGER",
		descriptor.FieldDescriptorProto_TYPE_UINT32:   "INTEGER",
		descriptor.FieldDescriptorProto_TYPE_FIXED64:  "INTEGER",
		descriptor.FieldDescriptorProto_TYPE_FIXED32:  "INTEGER",
		descriptor.FieldDescriptorProto_TYPE_SFIXED32: "INTEGER",
		descriptor.FieldDescriptorProto_TYPE_SFIXED64: "INTEGER",
		descriptor.FieldDescriptorProto_TYPE_SINT32:   "INTEGER",
		descriptor.FieldDescriptorProto_TYPE_SINT64:   "INTEGER",

		descriptor.FieldDescriptorProto_TYPE_STRING: "STRING",
		descriptor.FieldDescriptorProto_TYPE_BYTES:  "BYTES",
		descriptor.FieldDescriptorProto_TYPE_ENUM:   "STRING",

		descriptor.FieldDescriptorProto_TYPE_BOOL: "BOOLEAN",

		descriptor.FieldDescriptorProto_TYPE_GROUP:   "RECORD",
		descriptor.FieldDescriptorProto_TYPE_MESSAGE: "RECORD",
	}

	modeFromFieldLabel = map[descriptor.FieldDescriptorProto_Label]string{
		descriptor.FieldDescriptorProto_LABEL_OPTIONAL: "NULLABLE",
		descriptor.FieldDescriptorProto_LABEL_REQUIRED: "REQUIRED",
		descriptor.FieldDescriptorProto_LABEL_REPEATED: "REPEATED",
	}
)

func convertField(curPkg *ProtoPackage, desc *descriptor.FieldDescriptorProto, msgOpts *protos.BigQueryMessageOptions, path string) (*Field, error) {
	field := &Field{
		Name: desc.GetName(),
	}
	if msgOpts.GetUseJsonNames() && desc.GetJsonName() != "" {
		field.Name = desc.GetJsonName()
	}

	var ok bool
	field.Mode, ok = modeFromFieldLabel[desc.GetLabel()]
	if !ok {
		return nil, fmt.Errorf("unrecognized field label: %s", desc.GetLabel().String())
	}

	field.Type, ok = typeFromFieldType[desc.GetType()]
	if !ok {
		return nil, fmt.Errorf("unrecognized field type: %s", desc.GetType().String())
	}

	opts := desc.GetOptions()
	if opts != nil && proto.HasExtension(opts, protos.E_Bigquery) {
		rawOpt, err := proto.GetExtension(opts, protos.E_Bigquery)
		if err != nil {
			return nil, err
		}
		opt := *rawOpt.(*protos.BigQueryFieldOptions)
		if opt.Ignore {
			// skip the field below
			return nil, nil
		}

		if opt.Require {
			field.Mode = "REQUIRED"
		}

		if len(opt.TypeOverride) > 0 {
			field.Type = opt.TypeOverride
		}

		if len(opt.Name) > 0 {
			field.Name = opt.Name
		}

		if len(opt.Description) > 0 {
			field.Description = opt.Description
		}

		if len(opt.PolicyTags) > 0 {
			field.PolicyTags = &PolicyTags{Names:[]string{opt.PolicyTags}}
		}
	}

	if field.Type != "RECORD" {
		return field, nil
	}
	if t, ok := typeFromWKT[desc.GetTypeName()]; ok {
		field.Type = t
		return field, nil
	}

	recordType, ok, path := curPkg.lookupType(desc.GetTypeName())
	if !ok {
		return nil, fmt.Errorf("no such message type named %s", desc.GetTypeName())
	}
	fieldMsgOpts, err := getBigqueryMessageOptions(recordType)
	if err != nil {
		return nil, err
	}
	field.Fields, err = convertMessageType(curPkg, recordType, fieldMsgOpts, path)
	if err != nil {
		return nil, err
	}

	// Convert zero field RECORDS to a single boolean where the presence of
	// the message is interpreted as true. This only really applies in limited
	// use-cases e.g. where a oneOf value of empty message is used like a bool
	if len(field.Fields) == 0 {
		return &Field{
			Name: desc.GetName(),
			Type: "BOOLEAN",
			Mode: "NULLABLE",
		}, nil
	}

	return field, nil
}

func convertMessageType(curPkg *ProtoPackage, msg *descriptor.DescriptorProto, opts *protos.BigQueryMessageOptions, path string) (schema []*Field, err error) {
	if glog.V(4) {
		glog.Info("Converting message: ", proto.MarshalTextString(msg))
	}

	for fieldIndex, fieldDesc := range msg.GetField() {
		fieldCommentPath := fmt.Sprintf("%s.%d.%d", path, fieldPath, fieldIndex)
		field, err := convertField(curPkg, fieldDesc, opts, fieldCommentPath)
		if err != nil {
			glog.Errorf("Failed to convert field %s in %s: %v", fieldDesc.GetName(), msg.GetName(), err)
			return nil, err
		}

		// if we got no error and the field is nil, skip it
		if field != nil {
			schema = append(schema, field)
		}
	}
	return
}

// NB: This is what the extension for tag 1021 used to look like. For some
// level of backwards compatibility, we will try to parse the extension using
// this definition if we get an error trying to parse it as the current
// definition (a message, to support multiple extension fields therein).
var e_TableName = &proto.ExtensionDesc{
	ExtendedType:  (*descriptor.MessageOptions)(nil),
	ExtensionType: (*string)(nil),
	Field:         1021,
	Name:          "gen_bq_schema.table_name",
	Tag:           "bytes,1021,opt,name=table_name,json=tableName",
	Filename:      "bq_table.proto",
}

func convertFile(file *descriptor.FileDescriptorProto) ([]*plugin.CodeGeneratorResponse_File, error) {
	name := path.Base(file.GetName())
	pkg, ok := globalPkg.relativelyLookupPackage(file.GetPackage())
	if !ok {
		pkg = &ProtoPackage{
			name: name,
		}
	}

	var response []*plugin.CodeGeneratorResponse_File
	for msgIndex, msg := range file.GetMessageType() {
		path := fmt.Sprintf("%d.%d", messagePath, msgIndex)

		opts, err := getBigqueryMessageOptions(msg)
		if err != nil {
			return nil, err
		}
		if opts == nil {
			continue
		}

		tableName := opts.GetTableName()
		if len(tableName) == 0 {
			continue
		}

		glog.V(2).Info("Generating schema for a message type ", msg.GetName())
		schema, err := convertMessageType(pkg, msg, opts, path)
		if err != nil {
			glog.Errorf("Failed to convert %s: %v", name, err)
			return nil, err
		}

		jsonSchema, err := json.MarshalIndent(schema, "", " ")
		if err != nil {
			glog.Error("Failed to encode schema", err)
			return nil, err
		}

		resFile := &plugin.CodeGeneratorResponse_File{
			Name:    proto.String(fmt.Sprintf("%s.schema", tableName)),
			Content: proto.String(string(jsonSchema)),
		}
		response = append(response, resFile)
	}

	return response, nil
}

// getBigqueryMessageOptions returns the bigquery options for the given message.
// If an error is encountered, it is returned instead. If no error occurs, but
// the message has no gen_bq_schema.bigquery_opts option, this function returns
// nil, nil.
func getBigqueryMessageOptions(msg *descriptor.DescriptorProto) (*protos.BigQueryMessageOptions, error) {
	options := msg.GetOptions()
	if options == nil {
		return nil, nil
	}

	if !proto.HasExtension(options, protos.E_BigqueryOpts) {
		return nil, nil
	}

	optionValue, err := proto.GetExtension(options, protos.E_BigqueryOpts)
	if err == nil {
		return optionValue.(*protos.BigQueryMessageOptions), nil
	}

	// try to decode the extension using old definition before failing
	optionValue, newErr := proto.GetExtension(options, e_TableName)
	if newErr != nil {
		return nil, err // return original error
	}
	// translate this old definition to the expected message type
	name := *optionValue.(*string)
	return &protos.BigQueryMessageOptions{
		TableName: name,
	}, nil
}

func convert(req *plugin.CodeGeneratorRequest) (*plugin.CodeGeneratorResponse, error) {

	generateTargets := make(map[string]bool)
	for _, file := range req.GetFileToGenerate() {
		generateTargets[file] = true
	}

	res := &plugin.CodeGeneratorResponse{}
	for msgIndex, file := range req.GetProtoFile() {
		for _, msg := range file.GetMessageType() {
			glog.V(1).Infof("Loading a message type %s from package %s", msg.GetName(), file.GetPackage())
			registerType(file.Package, msg, fmt.Sprintf("%d.%d", messagePath, msgIndex))
		}
	}
	for _, file := range req.GetProtoFile() {
		if _, ok := generateTargets[file.GetName()]; ok {
			glog.V(1).Info("Converting ", file.GetName())
			converted, err := convertFile(file)
			if err != nil {
				res.Error = proto.String(fmt.Sprintf("Failed to convert %s: %v", file.GetName(), err))
				return res, err
			}
			res.File = append(res.File, converted...)
		}
	}
	return res, nil
}

func convertFrom(rd io.Reader) (*plugin.CodeGeneratorResponse, error) {
	glog.V(1).Info("Reading code generation request")
	input, err := ioutil.ReadAll(rd)
	if err != nil {
		glog.Error("Failed to read request:", err)
		return nil, err
	}
	req := &plugin.CodeGeneratorRequest{}
	err = proto.Unmarshal(input, req)
	if err != nil {
		glog.Error("Can't unmarshal input:", err)
		return nil, err
	}

	glog.V(1).Info("Converting input")
	return convert(req)
}

func main() {
	flag.Parse()
	ok := true
	glog.Info("Processing code generator request")

	res, err := convertFrom(os.Stdin)
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
