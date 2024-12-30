package util

import (
	"strings"
)

type CaseInsensitiveMap struct {
	data map[string]string
}

// New creates a new CaseInsensitiveMap
func NewCaseInsensitiveMap() *CaseInsensitiveMap {
	return &CaseInsensitiveMap{
		data: make(map[string]string),
	}
}

// Set stores a key-value pair in the map, converting the key to lowercase.
func (m *CaseInsensitiveMap) Set(key, value string) {
	m.data[strings.ToLower(key)] = value
}

// Get retrieves a value by its key, case-insensitively.
func (m *CaseInsensitiveMap) Get(key string) (string, bool) {
	value, ok := m.data[strings.ToLower(key)]
	return value, ok
}

// Delete removes a key-value pair from the map, case-insensitively.
func (m *CaseInsensitiveMap) Delete(key string) {
	delete(m.data, strings.ToLower(key))
}

// Exists checks if a key exists in the map, case-insensitively.
func (m *CaseInsensitiveMap) Exists(key string) bool {
	_, ok := m.data[strings.ToLower(key)]
	return ok
}
