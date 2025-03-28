package pebble

import "github.com/fgrzl/streams/server"

// Messages
type ClusterStatus = server.ClusterStatus
type Commit = server.Commit
type ConfirmSegmentOffset = server.ConfirmSegmentOffset
type ConfirmSpaceOffset = server.ConfirmSpaceOffset
type Consume = server.Consume
type ConsumeSegment = server.ConsumeSegment
type ConsumeSpace = server.ConsumeSpace
type Entry = server.Entry
type EnumerateSegment = server.EnumerateSegment
type EnumerateSpace = server.EnumerateSpace
type GetSegments = server.GetSegments
type GetSpaces = server.GetSpaces
type GetStatus = server.GetStatus
type NodeHeartbeat = server.NodeHeartbeat
type NodeShutdown = server.NodeShutdown
type Peek = server.Peek
type Produce = server.Produce
type Reconcile = server.Reconcile
type Record = server.Record
type Rollback = server.Rollback
type SegmentStatus = server.SegmentStatus
type Synchronize = server.Synchronize
type Transaction = server.Transaction
type TRX = server.TRX
type ACK = server.ACK
type NACK = server.NACK
