package converter

import (
	"fmt"
	"strings"

	"github.com/golang/glog"
	descriptor "google.golang.org/protobuf/types/descriptorpb"
)

var (
	globalPkg = &ProtoPackage{
		name:     "",
		parent:   nil,
		children: make(map[string]*ProtoPackage),
		types:    make(map[string]*descriptor.DescriptorProto),
		comments: make(map[string]Comments),
		path:     make(map[string]string),
	}
)

// ProtoPackage describes a package of Protobuf, which is an container of message types.
type ProtoPackage struct {
	name     string
	parent   *ProtoPackage
	children map[string]*ProtoPackage
	types    map[string]*descriptor.DescriptorProto
	comments map[string]Comments
	path     map[string]string
}

func (pkg *ProtoPackage) lookupType(name string) (*descriptor.DescriptorProto, bool, Comments, string) {
	if strings.HasPrefix(name, ".") {
		return globalPkg.relativelyLookupType(name[1:])
	}

	for ; pkg != nil; pkg = pkg.parent {
		if desc, ok, comments, path := pkg.relativelyLookupType(name); ok {
			return desc, ok, comments, path
		}
	}
	return nil, false, Comments{}, ""
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

func (pkg *ProtoPackage) relativelyLookupType(name string) (*descriptor.DescriptorProto, bool, Comments, string) {
	components := strings.SplitN(name, ".", 2)
	switch len(components) {
	case 0:
		glog.V(1).Info("empty message name")
		return nil, false, Comments{}, ""
	case 1:
		found, ok := pkg.types[components[0]]
		return found, ok, pkg.comments[components[0]], pkg.path[components[0]]
	case 2:
		glog.Infof("looking for %s in %s at %s (%v)", components[1], components[0], pkg.name, pkg)

		if child, ok := pkg.children[components[0]]; ok {
			found, ok, comments, path := child.relativelyLookupType(components[1])
			return found, ok, comments, path
		}
		if msg, ok := pkg.types[components[0]]; ok {
			found, ok, path := relativelyLookupNestedType(msg, components[1])
			return found, ok, pkg.comments[components[0]], pkg.path[components[0]] + "." + path
		}
		glog.V(1).Infof("no such package nor message %s in %s", components[0], pkg.name)
		return nil, false, Comments{}, ""
	default:
		glog.Fatal("not reached")
		return nil, false, Comments{}, ""
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
