package converter

import (
	"reflect"
	"testing"

	descriptor "google.golang.org/protobuf/types/descriptorpb"
)

func TestParseComments(t *testing.T) {
	leadingComment := "    leading comment"
	trailingComment := "trailing comment"
	subMessageFieldLeadingComment := "submessage field leading comment"

	actual := ParseComments(
		&descriptor.FileDescriptorProto{
			SourceCodeInfo: &descriptor.SourceCodeInfo{
				Location: []*descriptor.SourceCodeInfo_Location{
					{
						Path:             []int32{4, 0},
						LeadingComments:  &leadingComment,
						TrailingComments: &trailingComment,
					},
					{
						Path:             []int32{4, 0, 3, 0, 2, 0},
						LeadingComments:  &subMessageFieldLeadingComment,
						TrailingComments: nil,
					},
				},
			},
		},
	)

	expected := Comments(map[string]string{
		"4.0":         "leading comment\n\ntrailing comment",
		"4.0.3.0.2.0": "submessage field leading comment",
	})

	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("Expectation: %v\n Actual: %v", actual, expected)
	}
}

func TestParseCommentsWithoutComments(t *testing.T) {
	actual := ParseComments(
		&descriptor.FileDescriptorProto{
			SourceCodeInfo: &descriptor.SourceCodeInfo{
				Location: []*descriptor.SourceCodeInfo_Location{
					{
						Path: []int32{4, 0},
					},
				},
			},
		},
	)

	expected := Comments(map[string]string{})

	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("Expectation: %v\n Actual: %v", actual, expected)
	}
}

func TestCommentsGet(t *testing.T) {
	comment := "comment"
	comments := ParseComments(
		&descriptor.FileDescriptorProto{
			SourceCodeInfo: &descriptor.SourceCodeInfo{
				Location: []*descriptor.SourceCodeInfo_Location{
					{
						Path:            []int32{4, 0},
						LeadingComments: &comment,
					},
				},
			},
		},
	)

	actual := comments.Get("4.0")
	expected := "comment"

	if actual != expected {
		t.Errorf("Expectation: %v\n Actual: %v", actual, expected)
	}
}
