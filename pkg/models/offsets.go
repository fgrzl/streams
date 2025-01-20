package models

import (
	"fmt"

	"google.golang.org/protobuf/proto"
)

func NewPartitionOffsets(data []byte) *PartitionOffsets {
	offsets := &PartitionOffsets{}
	return offsets.Unmarshal(data)
}

func (o *PartitionOffsets) Get(partition string) *Offset {
	return o.Offsets[partition]
}

func (o *PartitionOffsets) GetOffsetMap() map[string]*Offset {
	return o.Offsets
}

func (o *PartitionOffsets) Set(partition string, offset *Offset) {
	o.Offsets[partition] = offset
}

func (o *PartitionOffsets) Marshal() []byte {
	bytes, err := proto.Marshal(o)
	if err != nil {
		fmt.Println("Error marshalling PartitionOffsets: ", err)
	}
	return bytes
}

func (o *PartitionOffsets) Unmarshal(data []byte) *PartitionOffsets {
	proto.Unmarshal(data, o)
	return o
}

func NewSpaceOffsets(data []byte) *SpaceOffsets {
	offsets := &SpaceOffsets{}
	return offsets.Unmarshal(data)
}

func (o *SpaceOffsets) GetOffset(space, partition string) *Offset {
	partitionOffsets, ok := o.Offsets[space]
	if ok {
		return partitionOffsets.Get(partition)
	}
	return &Offset{}
}

func (o *SpaceOffsets) GetOffsetMap(space string) map[string]*Offset {
	partitionOffsets, ok := o.Offsets[space]
	if ok {
		return partitionOffsets.GetOffsetMap()
	}
	return make(map[string]*Offset)
}

func (o *SpaceOffsets) Set(space, partition string, offset *Offset) {
	partitionOffsets, ok := o.Offsets[space]
	if !ok {
		partitionOffsets = &PartitionOffsets{
			Offsets: make(map[string]*Offset),
		}
	}
	partitionOffsets.Set(partition, offset)
}

func (o *SpaceOffsets) SetOffsetFromEnvelope(envelope *EntryEnvelope) {
	o.Set(envelope.PartitionDescriptor.Space, envelope.PartitionDescriptor.Partition, envelope.GetOffset())
}

func (o *SpaceOffsets) Marshal() []byte {
	bytes, err := proto.Marshal(o)
	if err != nil {
		fmt.Println("Error marshalling PartitionOffsets: ", err)
	}
	return bytes
}

func (o *SpaceOffsets) Unmarshal(data []byte) *SpaceOffsets {
	proto.Unmarshal(data, o)
	return o
}
