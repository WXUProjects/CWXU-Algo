package utils

import (
	"bytes"
	"encoding/gob"
)

// GobEncoder Gob通用编码工具包
func GobEncoder(v interface{}) ([]byte, error) {
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	err := encoder.Encode(v)
	return buf.Bytes(), err
}

// GobDecoder Gob通用解码工具包
func GobDecoder(b []byte, v interface{}) error {
	decoder := gob.NewDecoder(bytes.NewReader(b))
	return decoder.Decode(v)
}
