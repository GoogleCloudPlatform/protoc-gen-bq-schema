package main

import (
	"./protos"

	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"strings"

	"github.com/golang/glog"
	"github.com/golang/protobuf/proto"
	descriptor "github.com/golang/protobuf/protoc-gen-go/descriptor"
	plugin "github.com/golang/protobuf/protoc-gen-go/plugin"
)

var (
	globalPkg = &ProtoPackage{
		name:     "",
		parent:   nil,
		children: make(map[string]*ProtoPackage),
		types:    make(map[string]*descriptor.DescriptorProto),
	}
)

// Field describes the schema of a field in Hive.
type Field struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// ProtoPackage describes a package of Protobuf, which is an container of message types.
type ProtoPackage struct {
	name     string
	parent   *ProtoPackage
	children map[string]*ProtoPackage
	types    map[string]*descriptor.DescriptorProto
}

func registerType(pkgName *string, msg *descriptor.DescriptorProto) {
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
				}
				pkg.children[node] = child
			}
			pkg = child
		}
	}
	pkg.types[msg.GetName()] = msg
}

func (pkg *ProtoPackage) lookupType(name string) (*descriptor.DescriptorProto, bool) {
	if strings.HasPrefix(name, ".") {
		return globalPkg.relativelyLookupType(name[1:len(name)])
	}

	for ; pkg != nil; pkg = pkg.parent {
		if desc, ok := pkg.relativelyLookupType(name); ok {
			return desc, ok
		}
	}
	return nil, false
}

func relativelyLookupNestedType(desc *descriptor.DescriptorProto, name string) (*descriptor.DescriptorProto, bool) {
	components := strings.Split(name, ".")
componentLoop:
	for _, component := range components {
		for _, nested := range desc.GetNestedType() {
			if nested.GetName() == component {
				desc = nested
				continue componentLoop
			}
		}
		glog.Infof("no such nested message %s in %s", component, desc.GetName())
		return nil, false
	}
	return desc, true
}

func (pkg *ProtoPackage) relativelyLookupType(name string) (*descriptor.DescriptorProto, bool) {
	components := strings.SplitN(name, ".", 2)
	switch len(components) {
	case 0:
		glog.V(1).Info("empty message name")
		return nil, false
	case 1:
		found, ok := pkg.types[components[0]]
		return found, ok
	case 2:
		glog.Infof("looking for %s in %s at %s (%v)", components[1], components[0], pkg.name, pkg)
		if child, ok := pkg.children[components[0]]; ok {
			found, ok := child.relativelyLookupType(components[1])
			return found, ok
		}
		if msg, ok := pkg.types[components[0]]; ok {
			found, ok := relativelyLookupNestedType(msg, components[1])
			return found, ok
		}
		glog.V(1).Infof("no such package nor message %s in %s", components[0], pkg.name)
		return nil, false
	default:
		glog.Fatal("not reached")
		return nil, false
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

// https://docs.aws.amazon.com/athena/latest/ug/data-types.html
var (
	typeFromWKT = map[string]string{
		".google.protobuf.Int32Value":  "int",
		".google.protobuf.Int64Value":  "bigint",
		".google.protobuf.UInt32Value": "int",
		".google.protobuf.UInt64Value": "bigint",
		".google.protobuf.DoubleValue": "double",
		".google.protobuf.FloatValue":  "float",
		".google.protobuf.BoolValue":   "boolean",
		".google.protobuf.StringValue": "varchar",
		".google.protobuf.BytesValue":  "bynary",
		".google.protobuf.Duration":    "varchar",
		".google.protobuf.Timestamp":   "timestamp",
	}
	typeFromFieldType = map[descriptor.FieldDescriptorProto_Type]string{
		descriptor.FieldDescriptorProto_TYPE_DOUBLE: "double",
		descriptor.FieldDescriptorProto_TYPE_FLOAT:  "float",

		descriptor.FieldDescriptorProto_TYPE_INT64:    "bigint",
		descriptor.FieldDescriptorProto_TYPE_UINT64:   "bigint",
		descriptor.FieldDescriptorProto_TYPE_INT32:    "int",
		descriptor.FieldDescriptorProto_TYPE_UINT32:   "int",
		descriptor.FieldDescriptorProto_TYPE_FIXED64:  "bigint",
		descriptor.FieldDescriptorProto_TYPE_FIXED32:  "int",
		descriptor.FieldDescriptorProto_TYPE_SFIXED32: "int",
		descriptor.FieldDescriptorProto_TYPE_SFIXED64: "bigint",
		descriptor.FieldDescriptorProto_TYPE_SINT32:   "int",
		descriptor.FieldDescriptorProto_TYPE_SINT64:   "bigint",

		descriptor.FieldDescriptorProto_TYPE_STRING: "varchar",
		descriptor.FieldDescriptorProto_TYPE_BYTES:  "bynary",
		descriptor.FieldDescriptorProto_TYPE_ENUM:   "varchar",

		descriptor.FieldDescriptorProto_TYPE_BOOL: "boolean",

		descriptor.FieldDescriptorProto_TYPE_GROUP:   "record",
		descriptor.FieldDescriptorProto_TYPE_MESSAGE: "record",
	}
	modeFromFieldLabel = map[descriptor.FieldDescriptorProto_Label]string{
		descriptor.FieldDescriptorProto_LABEL_OPTIONAL: "nullable",
		descriptor.FieldDescriptorProto_LABEL_REQUIRED: "required",
		descriptor.FieldDescriptorProto_LABEL_REPEATED: "repeated",
	}
)

func getField(desc *descriptor.FieldDescriptorProto, msgOpts *protos.HiveMessageOptions) (field *Field, fieldMode string, err error) {
	field = &Field{
		Name: desc.GetName(),
	}

	if msgOpts.GetUseJsonNames() && desc.GetJsonName() != "" {
		field.Name = desc.GetJsonName()
	}

	fieldMode, ok := modeFromFieldLabel[desc.GetLabel()]
	if !ok {
		return nil, fieldMode, fmt.Errorf("unrecognized field label: %s", desc.GetLabel().String())
	}

	field.Type, ok = typeFromFieldType[desc.GetType()]
	if !ok {
		return nil, fieldMode, fmt.Errorf("unrecognized field type: %s", desc.GetType().String())
	}

	opts := desc.GetOptions()
	if opts != nil && proto.HasExtension(opts, protos.E_Hive) {
		rawOpt, err := proto.GetExtension(opts, protos.E_Hive)
		if err != nil {
			return nil, fieldMode, err
		}
		opt := *rawOpt.(*protos.HiveFieldOptions)
		if opt.Ignore {
			fieldMode = "ignore"
			return field, fieldMode, nil
		}
		if len(opt.TypeOverride) > 0 {
			field.Type = opt.TypeOverride
		}
		if len(opt.Name) > 0 {
			field.Name = opt.Name
		}
	}

	return field, fieldMode, nil
}

func convertFieldAsStr(curPkg *ProtoPackage, desc *descriptor.FieldDescriptorProto, msgOpts *protos.HiveMessageOptions) (string, error) {
	field, fieldMode, err := getField(desc, msgOpts)
	if err != nil {
		return "", nil
	}
	if fieldMode == "ignore" {
		return "", nil
	}

	field.Name = field.Name + ":" + field.Type

	if field.Type != "record" {
		if fieldMode == "repeated" {
			field.Type = field.Name + ":array<" + field.Type + ">"
		}
		return field.Name, nil
	}

	if t, ok := typeFromWKT[desc.GetTypeName()]; ok {
		field.Type = t
		if fieldMode == "repeated" {
			field.Name = field.Name + ":array<" + field.Type + ">"
		}
		return field.Name, nil
	}

	recordType, ok := curPkg.lookupType(desc.GetTypeName())
	if !ok {
		return "", fmt.Errorf("no such message type named %s", desc.GetTypeName())
	}

	fieldMsgOpts, err := getHiveMessageOptions(recordType)
	if err != nil {
		return "", err
	}

	fields, err := convertMessageTypeAsStr(curPkg, recordType, fieldMsgOpts)
	if err != nil {
		return "", err
	}

	field.Type = "struct<" + fields + ">"
	if fieldMode == "repeated" {
		field.Type = "array<struct<" + fields + ">>"
	}

	return field.Type, nil
}

func convertField(curPkg *ProtoPackage, desc *descriptor.FieldDescriptorProto, msgOpts *protos.HiveMessageOptions) (*Field, error) {
	field, fieldMode, err := getField(desc, msgOpts)
	if err != nil {
		return nil, nil
	}
	if fieldMode == "ignore" {
		return nil, nil
	}

	if field.Type != "record" {
		if fieldMode == "repeated" {
			field.Type = "array<" + field.Type + ">"
		}
		return field, nil
	}

	if t, ok := typeFromWKT[desc.GetTypeName()]; ok {
		field.Type = t
		return field, nil
	}

	recordType, ok := curPkg.lookupType(desc.GetTypeName())
	if !ok {
		return nil, fmt.Errorf("no such message type named %s", desc.GetTypeName())
	}

	fieldMsgOpts, err := getHiveMessageOptions(recordType)
	if err != nil {
		return nil, err
	}

	fields, err := convertMessageTypeAsStr(curPkg, recordType, fieldMsgOpts)
	if err != nil {
		return nil, err
	}

	field.Type = "struct<" + fields + ">"
	if fieldMode == "repeated" {
		field.Type = "array<struct<" + fields + ">>"
	}

	return field, nil
}

func convertMessageTypeAsStr(curPkg *ProtoPackage, msg *descriptor.DescriptorProto, opts *protos.HiveMessageOptions) (fields string, err error) {
	if glog.V(4) {
		glog.Info("Converting message: ", proto.MarshalTextString(msg))
	}
	var fieldList []string

	for _, fieldDesc := range msg.GetField() {
		field, err := convertFieldAsStr(curPkg, fieldDesc, opts)
		if err != nil {
			glog.Errorf("Failed to convert field %s in %s: %v", fieldDesc.GetName(), msg.GetName(), err)
			return "", err
		}

		// if we got no error and the field is nil, skip it
		if field != "" {
			fieldList = append(fieldList, field)
			continue
		}
	}
	fields = strings.Join(fieldList, ",")
	return
}

func convertMessageType(curPkg *ProtoPackage, msg *descriptor.DescriptorProto, opts *protos.HiveMessageOptions) (schema []*Field, err error) {
	if glog.V(4) {
		glog.Info("Converting message: ", proto.MarshalTextString(msg))
	}

	for _, fieldDesc := range msg.GetField() {
		field, err := convertField(curPkg, fieldDesc, opts)
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
	Name:          "gen_hive_schema.table_name",
	Tag:           "bytes,1021,opt,name=table_name,json=tableName",
	Filename:      "hive_table.proto",
}

func convertFile(file *descriptor.FileDescriptorProto) ([]*plugin.CodeGeneratorResponse_File, error) {
	name := path.Base(file.GetName())
	pkg, ok := globalPkg.relativelyLookupPackage(file.GetPackage())
	if !ok {
		return nil, fmt.Errorf("no such package found: %s", file.GetPackage())
	}

	response := []*plugin.CodeGeneratorResponse_File{}
	for _, msg := range file.GetMessageType() {
		opts, err := getHiveMessageOptions(msg)
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
		schema, err := convertMessageType(pkg, msg, opts)
		if err != nil {
			glog.Errorf("Failed to convert %s: %v", name, err)
			return nil, err
		}

		jsonSchema, err := JSONMarshalIndent(schema, "", " ")
		if err != nil {
			glog.Error("Failed to encode schema", err)
			return nil, err
		}

		resFile := &plugin.CodeGeneratorResponse_File{
			Name:    proto.String(fmt.Sprintf("schemas/%s.json", tableName)),
			Content: proto.String(string(jsonSchema)),
		}
		response = append(response, resFile)
	}

	return response, nil
}

// getHiveMessageOptions returns the hive options for the given message.
// If an error is encountered, it is returned instead. If no error occurs, but
// the message has no gen_hive_schema.hive_opts option, this function returns
// nil, nil.
func getHiveMessageOptions(msg *descriptor.DescriptorProto) (*protos.HiveMessageOptions, error) {
	options := msg.GetOptions()
	if options == nil {
		return nil, nil
	}

	if !proto.HasExtension(options, protos.E_HiveOpts) {
		return nil, nil
	}

	optionValue, err := proto.GetExtension(options, protos.E_HiveOpts)
	if err == nil {
		return optionValue.(*protos.HiveMessageOptions), nil
	}

	// try to decode the extension using old definition before failing
	optionValue, newErr := proto.GetExtension(options, e_TableName)
	if newErr != nil {
		return nil, err // return original error
	}
	// translate this old definition to the expected message type
	name := *optionValue.(*string)
	return &protos.HiveMessageOptions{
		TableName: name,
	}, nil
}

func convert(req *plugin.CodeGeneratorRequest) (*plugin.CodeGeneratorResponse, error) {
	generateTargets := make(map[string]bool)
	for _, file := range req.GetFileToGenerate() {
		generateTargets[file] = true
	}

	res := &plugin.CodeGeneratorResponse{}
	for _, file := range req.GetProtoFile() {
		for _, msg := range file.GetMessageType() {
			glog.V(1).Infof("Loading a message type %s from package %s", msg.GetName(), file.GetPackage())
			registerType(file.Package, msg)
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
