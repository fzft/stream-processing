package stream_processing

// RoutingPolicy decides where exactly to route each particular item emit from an upstream processor
type RoutingPolicy int

const (
	// UNICAST this policy chooses for each item a single destination processor from the candidate set
	UNICAST RoutingPolicy = iota

	// ISOLATED this policy sets up isolated parallel data paths between two vertices, parallelism
	// if LP_upstream <= LP_downstream, every downstream processor receives data from only one upstream processor
	// if LP_upstream >= LP_downstream, every upstream processor sends data to only one downstream processor
	ISOLATED

	// PARTITIONED this policy sends every item to the one processor responsible for the item's partition ID
	PARTITIONED

	// BROADCAST this policy sends each item to all candidate processors
	BROADCAST

	// FANOUT TODO
	FANOUT
)

type Edge struct {
	source        *Vertex
	sourceName    string
	sourceOrdinal int

	destination *Vertex
	destName    string
	destOrdinal int
	priority    int

	partitioner   Partitioner
	routingPolicy RoutingPolicy
}

func NewEdge(source, destination *Vertex, sourceOrdinal, destOrdinal int) *Edge {
	e := &Edge{
		source:        source,
		sourceName:    source.name,
		sourceOrdinal: sourceOrdinal,
	}

	e.destOrdinal = destOrdinal

	if destination != nil {
		e.destination = destination
		e.destName = destination.name
	}
	return e
}

// To sets the destination vertex and ordinal of this edge
func (e *Edge) To(destination *Vertex, ordinal int) *Edge {
	if e.destination != nil {
		panic("destination already set")
	}
	e.destination = destination
	e.destName = destination.name
	e.destOrdinal = ordinal
	return e
}

// setPriority a lower number means higher priority and default is 0
// example: there two incoming edges on a vertex, with priorities 1 and 2.
// the data from the edge with priority 1 will be processed in full before accepting any data from the edge with priority 2
func (e *Edge) setPriority(priority int) *Edge {
	e.priority = priority
	return e
}

// partitioned ...
func (e *Edge) partitioned(extractKeyFn FunctionEx, partitioner Partitioner) *Edge {
	e.routingPolicy = PARTITIONED
	e.partitioner = NewKeyPartitioner(extractKeyFn, partitioner)
	return e
}

// allToOne ...
func (e *Edge) allToOne(key interface{}) *Edge {
	return e.partitioned(NewWholeItem(), NewSinglePartitioner(key))
}

// broadcast ...
func (e *Edge) broadcast() *Edge {
	e.routingPolicy = BROADCAST
	return e
}

// unicast ...
func (e *Edge) unicast() *Edge {
	e.routingPolicy = UNICAST
	return e
}


// Between return an edge between two vertices. the ordinal of the edge is 0 at both ends
func Between(source, destination *Vertex) *Edge {
	return NewEdge(source, destination, 0, 0)
}

// From return an edge with the given source vertex at the given ordinal and no destination vertex
func From(source *Vertex, ordinal int) *Edge {
	return NewEdge(source, nil, ordinal, 0)
}
