package server

import (
	"sync"
	"time"

	"github.com/fgrzl/timestamp"
	"github.com/google/uuid"
)

var (
	ttl = time.Duration(15 * time.Second)
)

// Quorum interface defines quorum behavior
type Quorum interface {
	GetNode() uuid.UUID
	GetNodes() map[uuid.UUID]int64
	GetTTL() time.Duration
	SetOnline(node uuid.UUID, timestamp int64) bool
	SetOffline(node uuid.UUID)
	GetReadCount() int
	GetWriteCount() int
	GetNodeCount() int
}

// quorumImpl implements the Quorum interface
type quorumImpl struct {
	node  uuid.UUID
	nodes sync.Map
}

// NewQuorum initializes a new quorum with a single node
func NewQuorum(node uuid.UUID) Quorum {
	q := &quorumImpl{
		node: node,
	}
	return q
}

func (q *quorumImpl) GetNode() uuid.UUID {
	return q.node
}

func (q *quorumImpl) GetNodes() map[uuid.UUID]int64 {
	nodes := make(map[uuid.UUID]int64)
	q.nodes.Range(func(key, value any) bool {
		if lastHeartbeat, ok := value.(int64); ok {
			nodes[key.(uuid.UUID)] = lastHeartbeat
		}
		return true
	})
	return nodes
}

func (q *quorumImpl) GetTTL() time.Duration {
	return ttl
}

// SetOnline marks a node as online by updating its timestamp
func (q *quorumImpl) SetOnline(node uuid.UUID, timestamp int64) bool {
	_, alreadyExisted := q.nodes.LoadOrStore(node, timestamp)
	if alreadyExisted {
		q.nodes.Store(node, timestamp)
	}
	return !alreadyExisted
}

// SetOffline removes a node and updates quorum settings
func (q *quorumImpl) SetOffline(node uuid.UUID) {
	q.nodes.Delete(node)
}

// GetNodeCount returns the total number of nodes
func (q *quorumImpl) GetNodeCount() int {

	count, ts := 1, timestamp.GetTimestamp()
	q.nodes.Range(func(key, value any) bool {
		if lastHeartbeat, ok := value.(int64); ok && ts-lastHeartbeat < ttl.Milliseconds() {
			count++
		}
		return true
	})

	return count
}

// GetWriteCount returns the write quorum value
func (q *quorumImpl) GetWriteCount() int {
	// requires majority
	return q.GetNodeCount()/2 + 1
}

// GetReadCount returns the read quorum value
func (q *quorumImpl) GetReadCount() int {
	// requires majority
	return q.GetNodeCount()/2 + 1
}
