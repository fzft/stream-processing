package stream_processing

import (
	"fmt"
	"github.com/emirpasic/gods/sets/hashset"
	"github.com/emirpasic/gods/sets/linkedhashset"
	"strings"
)

// DAG describes a computation to be performed by the computation engine.
type DAG struct {
	edges              *linkedhashset.Set
	nameToVertex       map[string]*Vertex
	verticesByIdentity *hashset.Set
}

func NewDAG() *DAG {
	return &DAG{
		edges:              linkedhashset.New(),
		nameToVertex:       make(map[string]*Vertex),
		verticesByIdentity: hashset.New(),
	}
}

// newVertex creates a vertex from processor supplier and adds it to this DAG
func (d *DAG) newVertex(name string, simpleSupplier SupplierEx) *Vertex {
	return d.addVertex(NewVertex(name, simpleSupplier))
}

// newUniqueVertex creates a vertex from processor supplier and adds it to this DAG. the vertex will be given a unique name created from namePrefix
func (d *DAG) newUniqueVertex(namePrefix string, simpleSupplier SupplierEx) *Vertex {
	return d.addVertex(NewVertex(d.uniqueName(namePrefix), simpleSupplier))
}

// vertex add a vertex to this DAG. the vertex name must be unique
func (d *DAG) vertex(vertex *Vertex) *Vertex {
	d.addVertex(vertex)
	return vertex
}

func (d *DAG) addVertex(vertex *Vertex) *Vertex {
	if v, ok := d.nameToVertex[vertex.name]; ok {
		panic(fmt.Sprintf("Vertex %s is already defined", v.name))
	}
	d.verticesByIdentity.Add(vertex)
	d.nameToVertex[vertex.name] = vertex
	return vertex
}

// edge add an edge to this DAG, the vertices it connects must already be present in the DAG.
// it is an error to connect an edge to a vertex at the same ordinal as another exsiting edge.
// However , inbound and outbound ordinals are independent, so there can be two edges at the same ordinal,
// one inbound and one outbound
func (d *DAG) edge(edge *Edge) *DAG {
	if edge.destination == nil {
		panic("Edge has no destination")
	}
	if !d.containsVertex(edge.source) {
		if d.containsVertexName(edge.source) {
			panic(fmt.Sprintf("This DAG has a vertex called %s but the supplied edge's source is a different vertex with the same name", edge.sourceName))
		} else {
			panic(fmt.Sprintf("Source vertex %s is not in this DAG", edge.sourceName))
		}
	}
	if !d.containsVertex(edge.destination) {
		if d.containsVertexName(edge.destination) {
			panic(fmt.Sprintf("This DAG has a vertex called %s but the supplied edge's destination is a different vertex with the same name", edge.destName))
		} else {
			panic(fmt.Sprintf("Destination vertex %s is not in this DAG", edge.destName))
		}
	}
	if AnyMatch(d.getInboundEdges(edge.destName), func(v interface{}) bool {
		if e, ok := v.(*Edge); ok {
			return e.destOrdinal == edge.destOrdinal
		}
		return false
	}) {
		panic(fmt.Sprintf("Vertex %s already has an inbound edge at ordinal", edge.destName))
	}
	if AnyMatch(d.getOutboundEdges(edge.sourceName), func(v interface{}) bool {
		if e, ok := v.(*Edge); ok {
			return e.sourceOrdinal == edge.sourceOrdinal
		}
		return false
	}) {
		panic(fmt.Sprintf("Vertex %s already has an outbound edge at ordinal", edge.destName))
	}

	if edge.source == edge.destination {
		panic(fmt.Sprintf("Attempt to add an edge from %s to itself", edge.sourceName))
	}

	d.edges.Add(edge)
	return d
}

// uniqueName create a vertex name that is unique in the DAG and starts with the given prefix
func (d *DAG) uniqueName(prefix string) string {
	name := prefix
	if _, ok := d.nameToVertex[name]; !ok {
		return name
	}
	for i := 2; ; i++ {
		name = fmt.Sprintf("%s-%d", prefix, i)
		if _, ok := d.nameToVertex[name]; !ok {
			break
		}
	}
	return name
}

// iterator return iterable over vertices in topological order
func (d *DAG) iterator() []*Vertex {
	adjacencyMap := make(map[*Vertex][]*Vertex)
	for _, v := range d.edges.Values() {
		edge, _ := v.(*Edge)
		adjacencyMap[edge.source] = append(adjacencyMap[edge.source], edge.destination)
	}

	for _, v := range d.nameToVertex {
		if _, ok := adjacencyMap[v]; !ok {
			adjacencyMap[v] = []*Vertex{}
		}
	}

	return TopologicalSort(adjacencyMap)

}

// getVertex return the vertex with the given name
func (d *DAG) getVertex(vertexName string) *Vertex {
	return d.nameToVertex[vertexName]
}

func (d *DAG) containsVertexName(source *Vertex) bool {
	if _, ok := d.nameToVertex[source.name]; ok {
		return true
	}
	return false
}

func (d *DAG) containsVertex(source *Vertex) bool {
	return d.verticesByIdentity.Contains(source)
}

// getInboundEdges returns the inbound edges connected to the vertex with the given name
func (d *DAG) getInboundEdges(vertexName string) []interface{} {
	if _, ok := d.nameToVertex[vertexName]; !ok {
		panic(fmt.Sprintf("No Vertex with name %s found in this DAG", vertexName))
	}
	var inboundEdges []interface{}
	for _, v := range d.edges.Values() {
		if edge, ok := v.(*Edge); ok {
			if edge.destName == vertexName {
				inboundEdges = append(inboundEdges, edge)
			}
		}
	}
	return inboundEdges
}

// toString return a string representation of DAG
func (d *DAG) toString(defaultLocalParallelism int) string {
	var builder strings.Builder
	for _, v := range d.iterator() {
		builder.WriteString("     .vertex(\"")
		builder.WriteString(v.name)
		builder.WriteString("\")")
		builder.WriteString("\n")
	}
	for _, v := range d.edges.Values() {
		builder.WriteString("     .edge(")
		builder.WriteString(v.(*Edge).toString())
		builder.WriteString(")\n")
	}
	return builder.String()
}

// getOutboundEdges Returns the outbound edges connected to the vertex with the given name.
func (d *DAG) getOutboundEdges(vertexName string) []interface{} {
	if _, ok := d.nameToVertex[vertexName]; !ok {
		panic(fmt.Sprintf("No Vertex with name %s found in this DAG", vertexName))
	}
	var outboundEdges []interface{}
	for _, v := range d.edges.Values() {
		if edge, ok := v.(*Edge); ok {
			if edge.sourceName == vertexName {
				outboundEdges = append(outboundEdges, edge)
			}
		}
	}
	return outboundEdges
}
