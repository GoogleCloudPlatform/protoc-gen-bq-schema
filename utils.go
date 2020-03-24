package main

import (
	"bytes"
	"encoding/json"
)

// JSONMarshal marshal json without HTML escaping
func JSONMarshal(t interface{}) ([]byte, error) {
	buffer := &bytes.Buffer{}
	encoder := json.NewEncoder(buffer)
	encoder.SetEscapeHTML(false)
	err := encoder.Encode(t)
	return buffer.Bytes(), err
}

// JSONMarshalIndent reimplement to use custom json marshalling
func JSONMarshalIndent(v interface{}, prefix, indent string) ([]byte, error) {
	b, err := JSONMarshal(v)
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	err = json.Indent(&buf, b, prefix, indent)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
