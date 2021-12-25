package stream_processing

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

type DAGTest struct {
	PROCESSOR_SUPPLIER SupplierEx
}

func DAGTestSetup(tb testing.TB) (func(tb testing.TB), DAGTest) {
	et := DAGTest{}
	et.PROCESSOR_SUPPLIER = NewNoopP()

	return func(tb testing.TB) {
		tb.Log("DAGTestSetup teardown")
	}, et
}

func TestDAG_when_newVertex_then_hasIt(t *testing.T) {
	teardownTest, dt := DAGTestSetup(t)
	defer teardownTest(t)
	dag := NewDAG()
	a := dag.newVertex("a", dt.PROCESSOR_SUPPLIER)
	assert.Equal(t, a, dag.getVertex("a"))
}

func TestDAG_when_newUniqueVertex_then_hasIt(t *testing.T) {
	teardownTest, dt := DAGTestSetup(t)
	defer teardownTest(t)
	dag := NewDAG()
	a0 := dag.newUniqueVertex("a", dt.PROCESSOR_SUPPLIER)
	assert.Equal(t, a0, dag.getVertex("a"))

	a1 := dag.newUniqueVertex("a", dt.PROCESSOR_SUPPLIER)
	assert.Equal(t, a1, dag.getVertex("a-2"))
}

func TestDAG_when_connectKnownVertices_then_success(t *testing.T) {
	teardownTest, dt := DAGTestSetup(t)
	defer teardownTest(t)
	dag := NewDAG()
	a := dag.newVertex("a", dt.PROCESSOR_SUPPLIER)
	b := dag.newVertex("b", dt.PROCESSOR_SUPPLIER)
	dag.edge(Between(a, b))
}

func TestDAG_when_selfEdge_then_illegalArgument(t *testing.T) {
	teardownTest, dt := DAGTestSetup(t)
	defer teardownTest(t)
	dag := NewDAG()
	a := dag.newVertex("a", dt.PROCESSOR_SUPPLIER)
	dag.edge(Between(a, a))
}

func TestDAG_inboundEdges(t *testing.T) {
	teardownTest, dt := DAGTestSetup(t)
	defer teardownTest(t)
	dag := NewDAG()
	a := dag.newVertex("a", dt.PROCESSOR_SUPPLIER)
	b := dag.newVertex("b", dt.PROCESSOR_SUPPLIER)
	c := dag.newVertex("c", dt.PROCESSOR_SUPPLIER)
	d := dag.newVertex("d", dt.PROCESSOR_SUPPLIER)
	e1 := From(a, 0).To(d, 0)
	e2 := From(b, 0).To(d, 1)
	e3 := From(c, 0).To(d, 2)
	dag.edge(e1).
		edge(e2).
		edge(e3).
		edge(From(a, 1).To(b, 0))

	edges := dag.getInboundEdges("d")
	assert.Equal(t, []interface{}{e1, e2, e3}, edges)
}

func TestDAG_outboundEdges(t *testing.T) {
	teardownTest, dt := DAGTestSetup(t)
	defer teardownTest(t)
	dag := NewDAG()
	a := dag.newVertex("a", dt.PROCESSOR_SUPPLIER)
	b := dag.newVertex("b", dt.PROCESSOR_SUPPLIER)
	c := dag.newVertex("c", dt.PROCESSOR_SUPPLIER)
	d := dag.newVertex("d", dt.PROCESSOR_SUPPLIER)
	e1 := From(a, 0).To(b, 0)
	e2 := From(a, 1).To(c, 0)
	e3 := From(a, 2).To(d, 0)
	dag.edge(e1).
		edge(e2).
		edge(e3).
		edge(From(b, 0).To(c, 1))

	edges := dag.getOutboundEdges("a")
	assert.Equal(t, []interface{}{e1, e2, e3}, edges)
}

// build this DAG:
// a --> b \
//            --> e --> f
// c --> d /
func TestDAG_when_iterator_then_topologicalOrder(t *testing.T) {
	teardownTest, dt := DAGTestSetup(t)
	defer teardownTest(t)
	dag := NewDAG()
	a := dag.newVertex("a", dt.PROCESSOR_SUPPLIER)
	b := dag.newVertex("b", dt.PROCESSOR_SUPPLIER)
	c := dag.newVertex("c", dt.PROCESSOR_SUPPLIER)
	d := dag.newVertex("d", dt.PROCESSOR_SUPPLIER)
	e := dag.newVertex("e", dt.PROCESSOR_SUPPLIER)
	f := dag.newVertex("f", dt.PROCESSOR_SUPPLIER)

	dag.edge(Between(a, b)).
		edge(Between(b, e)).
		edge(Between(c, d)).
		edge(From(d, 0).To(e, 1)).
		edge(Between(e, f))

	sorted := dag.iterator()

	// assert that for every edge x -> y , x is before y in the ordering
	assert.True(t, findVertexPos(sorted, a) < findVertexPos(sorted, b))
	assert.True(t, findVertexPos(sorted, b) < findVertexPos(sorted, e))
	assert.True(t, findVertexPos(sorted, c) < findVertexPos(sorted, d))
	assert.True(t, findVertexPos(sorted, d) < findVertexPos(sorted, e))
	assert.True(t, findVertexPos(sorted, e) < findVertexPos(sorted, f))

}

func TestDAG_when_multigraph_then_valid(t *testing.T) {
	teardownTest, dt := DAGTestSetup(t)
	defer teardownTest(t)
	dag := NewDAG()
	a := dag.newVertex("a", dt.PROCESSOR_SUPPLIER)
	b := dag.newVertex("b", dt.PROCESSOR_SUPPLIER)

	dag.edge(From(a, 0).To(b, 0))
	dag.edge(From(a, 1).To(b, 1))
}

func findVertexPos(vertices []*Vertex, vertex *Vertex) int {
	for i, v := range vertices {
		if v == vertex {
			return i
		}
	}
	return -1
}
