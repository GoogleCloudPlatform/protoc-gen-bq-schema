package converter

import (
	"strconv"
	"strings"

	descriptor "google.golang.org/protobuf/types/descriptorpb"
)

const (
	messagePath    = 4 // FileDescriptorProto.message_type
	fieldPath      = 2 // DescriptorProto.field
	subMessagePath = 3 // DescriptorProto.nested_type
)

// Comments is a map between path in FileDescriptorProto and leading/trailing comments for each field.
type Comments map[string]string

// ParseComments reads FileDescriptorProto and parses comments into a map.
func ParseComments(fd *descriptor.FileDescriptorProto) Comments {
	comments := make(Comments)

	for _, loc := range fd.GetSourceCodeInfo().GetLocation() {
		if !hasComment(loc) {
			continue
		}

		path := loc.GetPath()
		key := make([]string, len(path))
		for idx, p := range path {
			key[idx] = strconv.FormatInt(int64(p), 10)
		}

		comments[strings.Join(key, ".")] = buildComment(loc)
	}

	return comments
}

// Get returns comment for path or empty string if path has no comment.
func (c Comments) Get(path string) string {
	if val, ok := c[path]; ok {
		return val
	}

	return ""
}

func hasComment(loc *descriptor.SourceCodeInfo_Location) bool {
	if loc.GetLeadingComments() == "" && loc.GetTrailingComments() == "" {
		return false
	}

	return true
}

func buildComment(loc *descriptor.SourceCodeInfo_Location) string {
	comment := strings.TrimSpace(loc.GetLeadingComments()) + "\n\n" + strings.TrimSpace(loc.GetTrailingComments())
	return strings.Trim(comment, "\n")
}
