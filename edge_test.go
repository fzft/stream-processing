package stream_processing

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

type EdgeTest struct {
	a *Vertex
	b *Vertex
}

func EdgeTestSetup(tb testing.TB) (func(tb testing.TB), EdgeTest) {
	et := EdgeTest{}
	et.a = NewVertex("A", nil)
	et.b = NewVertex("B", nil)

	return func(tb testing.TB) {
		tb.Log("EdgeTestSetup teardown")
	}, et
}

func TestEdge_whenBetween_thenFromAndToAtOrdinalZero(t *testing.T) {
	teardownTest, et := EdgeTestSetup(t)
	defer teardownTest(t)
	e := Between(et.a, et.b)
	assert.Equal(t, "A", e.sourceName)
	assert.Equal(t, "B", e.destName)
	assert.Equal(t, 0, e.sourceOrdinal)
	assert.Equal(t, 0, e.destOrdinal)
}

func TestEdge_whenFrom_thenSourceSet(t *testing.T) {
	teardownTest, et := EdgeTestSetup(t)
	defer teardownTest(t)
	e := From(et.a, 0)
	assert.Equal(t, "A", e.sourceName)
	assert.Equal(t, 0, e.sourceOrdinal)
}

func TestEdge_whenTo_thenDestSet(t *testing.T) {
	teardownTest, et := EdgeTestSetup(t)
	defer teardownTest(t)
	e := From(et.a, 0).To(et.b, 0)
	assert.Equal(t, "B", e.destName)
	assert.Equal(t, 0, e.destOrdinal)
}

func TestEdge_whenPartitionedByKey_thenPartitionerExtractsKey(t *testing.T) {
	teardownTest, et := EdgeTestSetup(t)
	defer teardownTest(t)
	e := From(et.a, 0)
	partitioningKey := 42

	e.partitioned(func(t interface{}) interface{} {
		return partitioningKey
	}, NewDefaultPartitioner())
	partitioner := e.partitioner
	assert.NotNil(t, partitioner)
	partitioner.init(DefaultPartitionStrategyImpl{})

	assert.Equal(t, PARTITIONED, e.routingPolicy)
	assert.Equal(t, partitioningKey, partitioner.getPartition(13, 0))
}

func TestEdge_whenPartitionedByCustom_thenCustomPartitioned(t *testing.T) {
	teardownTest, et := EdgeTestSetup(t)
	defer teardownTest(t)
	e := From(et.a, 0)
	partitioningKey := 42

	e.partitioned(func(t interface{}) interface{} {
		return t
	}, NewTestPartitioner(partitioningKey))
	partitioner := e.partitioner
	assert.NotNil(t, partitioner)

	assert.Equal(t, PARTITIONED, e.routingPolicy)
	assert.Equal(t, partitioningKey, partitioner.getPartition(13, 0))
}

func TestEdge_whenAllToOne_thenAlwaysSamePartition(t *testing.T) {
	teardownTest, et := EdgeTestSetup(t)
	defer teardownTest(t)
	e := From(et.a, 0)
	mockPartitionCount := 100

	e.allToOne("key")
	partitioner := e.partitioner
	assert.NotNil(t, partitioner)

	assert.Equal(t, PARTITIONED, e.routingPolicy)
	assert.Equal(t, partitioner.getPartition(17, mockPartitionCount), partitioner.getPartition(13, mockPartitionCount))
}
