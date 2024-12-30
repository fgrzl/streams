package util

import (
	"fmt"
	"sync"
	"time"
)

// Item represents the value stored in the map with a timestamp
type Item[T any] struct {
	Value     T
	Timestamp time.Time
}

// TTLMap encapsulates a thread-safe map with TTL management
type TTLMap[TKey any, TValue Disposable] struct {
	data            sync.Map
	ttl             time.Duration
	cleanupInterval time.Duration
	stopChan        chan struct{}
	wg              sync.WaitGroup
}

// NewTTLMap creates a new TTLMap instance
func NewTTLMap[TKey any, TValue Disposable](ttl, cleanupInterval time.Duration) *TTLMap[TKey, TValue] {
	ttlMap := &TTLMap[TKey, TValue]{
		ttl:             ttl,
		cleanupInterval: cleanupInterval,
		stopChan:        make(chan struct{}),
	}

	ttlMap.startCleanup()
	return ttlMap
}

func (m *TTLMap[TKey, TValue]) Dispose() {
	// todo. stop cleanup
	// todo. dispose all items

}

// Store adds an item to the map
func (m *TTLMap[TKey, TValue]) Store(key TKey, value TValue) {
	m.data.Store(key, &Item[TValue]{Value: value, Timestamp: time.Now()})
}

// Load retrieves an item from the map
func (m *TTLMap[TKey, TValue]) Load(key TKey) (TValue, bool) {
	if item, ok := m.data.Load(key); ok {
		typedItem := item.(*Item[TValue])
		typedItem.Timestamp = time.Now()
		return typedItem.Value, true
	}
	var empty TValue
	return empty, false
}

// Load retrieves an item from the map
func (m *TTLMap[TKey, TValue]) LoadOrStore(key TKey, factory func() TValue) (TValue, bool) {

	if value, ok := m.Load(key); ok {
		return value, true
	}

	value := factory()
	newItem := &Item[TValue]{Value: value, Timestamp: time.Now()}

	if item, ok := m.data.LoadOrStore(key, newItem); ok {

		typedItem := item.(*Item[TValue])
		typedItem.Timestamp = time.Now()
		value.Dispose()
		return typedItem.Value, true
	}
	return value, false
}

// Delete removes an item from the map
func (m *TTLMap[TKey, TValue]) Delete(key TKey) {
	if item, ok := m.data.LoadAndDelete(key); ok {
		typedItem := item.(*Item[TValue])
		typedItem.Value.Dispose()
	}
}

// Range iterates over all items in the map
func (m *TTLMap[TKey, TValue]) Range(f func(key TKey, value TValue) bool) {
	m.data.Range(func(k, v interface{}) bool {
		key := k.(TKey)
		value := v.(*Item[TValue])
		value.Timestamp = time.Now()
		return f(key, value.Value)
	})
}

// startCleanup starts the background cleanup process
func (m *TTLMap[TKey, TValue]) startCleanup() {
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		ticker := time.NewTicker(m.cleanupInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				m.cleanup()
			case <-m.stopChan:
				return
			}
		}
	}()
}

// cleanup removes expired items from the map
func (m *TTLMap[TKey, TValue]) cleanup() {
	now := time.Now()
	m.data.Range(func(key, value interface{}) bool {
		item := value.(Item[TValue])
		if now.Sub(item.Timestamp) > m.ttl {
			fmt.Printf("Removing expired key: %s\n", key.(string))
			m.data.Delete(key)
		}
		return true
	})
}

// StopCleanup stops the background cleanup process
func (m *TTLMap[TKey, TValue]) StopCleanup() {
	close(m.stopChan)
	m.wg.Wait()
}
