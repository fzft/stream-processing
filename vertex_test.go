package stream_processing

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestVertex_when_constructed_then_hasDefaultParallelism(t *testing.T) {
	v := NewVertex("v", NewNoopP())

	assert.Equal(t, -1, v.localParallelism)
}

