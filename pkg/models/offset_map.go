package models

import (
	"fmt"

	"google.golang.org/protobuf/proto"
)

func NewOffsetMap(spaces ...string) *OffsetMap {

	offsets := make(map[string]*Offset)
	for _, space := range spaces {
		offsets[space] = &Offset{}
	}
	return &OffsetMap{
		Offsets: offsets,
	}
}

func (o *OffsetMap) Get(space string) *Offset {
	return o.Offsets[space]
}

func (o *OffsetMap) Set(space string, offset *Offset) {
	o.Offsets[space] = offset
}

func (o *OffsetMap) Marshal() []byte {
	bytes, err := proto.Marshal(o)
	if err != nil {
		fmt.Println("Error marshalling OffsetMap: ", err)
	}
	return bytes
}

func (o *OffsetMap) Unmarshal(data []byte) *OffsetMap {
	proto.Unmarshal(data, o)
	return o
}
