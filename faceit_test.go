package protoc_gen_bq_schema

import (
	"io/ioutil"
	"path/filepath"
	"strings"
	"testing"

	"github.com/golang/protobuf/proto"
	plugin "github.com/golang/protobuf/protoc-gen-go/plugin"
	"google.golang.org/protobuf/types/descriptorpb"
)

var expSchemaJson = `[
 {
  "name": "event_id",
  "type": "STRING",
  "mode": "NULLABLE"
 },
 {
  "name": "event_timestamp",
  "type": "TIMESTAMP",
  "mode": "NULLABLE"
 },
 {
  "name": "match_id",
  "type": "STRING",
  "mode": "NULLABLE"
 }
]`

func Test_Convert(t *testing.T) {
	input, err := ioutil.ReadFile("testdata/proto-defs.bin")
	if err != nil {
		t.Fatal("Failed to read file: ", err)
	}

	set := new(descriptorpb.FileDescriptorSet)
	err = proto.Unmarshal(input, set)
	if err != nil {
		t.Fatal("Can't unmarshal input:", err)
	}

	toGenerate := make([]string, len(set.File))
	for i, file := range set.File {
		file.Extension = append(file.Extension)
		toGenerate[i] = file.GetName()
	}

	res, err := Convert(&plugin.CodeGeneratorRequest{
		FileToGenerate:  toGenerate,
		Parameter:       nil,
		ProtoFile:       set.File,
		CompilerVersion: nil,
	})
	if err != nil {
		t.Fatal("failed to convert", err)
	}

	if len(res.File) != 1 {
		t.Fatalf("expected 1 file, got %d", len(res.File))
	}

	file := res.File[0]
	expTableName := "test_name_v1"
	actualTableName := strings.TrimSuffix(filepath.Base(file.GetName()), filepath.Ext(file.GetName()))
	if expTableName != actualTableName {
		t.Fatalf("expected name %s but got %s", expTableName, actualTableName)
	}

	if expSchemaJson != file.GetContent() {
		t.Fatalf("expected schema %s but got %s", expSchemaJson, file.GetContent())
	}
}
