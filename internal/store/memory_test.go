package store

import "testing"

func TestMemoryLifecycle(t *testing.T) {
	s := NewMemory()
	defer func() { _ = s.Close() }()
	testLifecycle(t, s)
	testList(t, s)
	testTrim(t, s)
}
