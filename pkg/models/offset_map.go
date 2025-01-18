package models

import (
	"fmt"

	"google.golang.org/protobuf/proto"
)

func NewOffsetMap(data []byte) *OffsetMap {
	offsets := &OffsetMap{}
	return offsets.Unmarshal(data)
}

func (o *OffsetMap) Get(space, partition string) *Offset {
	key := GetOffsetMapKey(space, partition)
	return o.Offsets[key]
}

func (o *OffsetMap) Set(space, partition string, offset *Offset) {
	key := GetOffsetMapKey(space, partition)
	o.Offsets[key] = offset
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

func GetOffsetMapKey(space, partition string) string {
	return space + ":" + partition
}
