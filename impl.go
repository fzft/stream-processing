package stream_processing

import (
	"fmt"
)

type TarjanVertex struct {
	v interface{}

	index   int
	lowlink int
	onStack bool
}

func NewTarjanVertex(v interface{}) *TarjanVertex {
	return &TarjanVertex{
		v:       v,
		index:   -1,
		lowlink: -1,
	}
}

func (v *TarjanVertex) visitedAtIndex(index int) {
	v.index = index
	v.lowlink = index
}

// TopologicalSorter computes a topological ordering of the vertex in a graph
type TopologicalSorter struct {
	adjacencyMap map[*TarjanVertex][]*TarjanVertex

	topologicallySorted []*Vertex
	tarjanStack         []*TarjanVertex
	nextIndex           int
}

func NewTopologicalSorter(adjacencyMap map[*TarjanVertex][]*TarjanVertex) *TopologicalSorter {
	return &TopologicalSorter{adjacencyMap: adjacencyMap}
}

func (t *TopologicalSorter) Iterator() []*Vertex {
	for tv, _ := range t.adjacencyMap {
		if tv.index != -1 {
			continue
		}
		t.strongConnect(tv)
	}
	for i, j := 0, len(t.topologicallySorted)-1; i < j; i, j = i+1, j-1 {
		t.topologicallySorted[i], t.topologicallySorted[j] = t.topologicallySorted[j], t.topologicallySorted[i]
	}
	return t.topologicallySorted
}

// strongConnect ...
func (t *TopologicalSorter) strongConnect(currTv *TarjanVertex) {
	currTv.visitedAtIndex(t.nextIndex)
	t.nextIndex++
	t.push(currTv)
	for _, outTv := range t.adjacencyMap[currTv] {
		if outTv == currTv {
			panic(fmt.Sprintf("Vertex %+v is connected to itself", currTv.v))
		}
		if outTv.index == -1 {
			// outTv not discovered yet, visit it
			t.strongConnect(outTv)

			// propagate lowlink computed for it to currTv
			currTv.lowlink = Min(currTv.lowlink, outTv.lowlink)
		} else if outTv.onStack {
			// outTv is already on the stack => there is a cycle in the graph
			// Process with the algorithm until the full extent fo the cycle is known
			currTv.lowlink = Min(currTv.lowlink, outTv.index)
		}
	}
	if currTv.lowlink < currTv.index {
		// currTv has a path to some vertex that is already on the stack
		// leave currTv on the stack and return
		return
	}
	popped := t.pop()
	if popped == currTv {
		// currTv was on the top of the stack => it is the sole member of its Sc component => it is not involved in any cycle. Add it to the output list and return
		t.topologicallySorted = append(t.topologicallySorted, currTv.v.(*Vertex))
		return
	}

	panic("DAG contains a cycle")
}

func (t *TopologicalSorter) push(tv *TarjanVertex) {
	tv.onStack = true
	t.tarjanStack = append(t.tarjanStack, tv)
}

func (t *TopologicalSorter) pop() *TarjanVertex {
	tv := t.tarjanStack[len(t.tarjanStack) - 1]
	t.tarjanStack = t.tarjanStack[:len(t.tarjanStack) - 1]
	tv.onStack = false
	return tv
}

func TopologicalSort(adjacencyMap map[*Vertex][]*Vertex) []*Vertex {
	// dedecorate all the vertices with Tarjan vertices, which hold the metadata needed by the algorithm
	tarjanAdjacencyMap := make(map[*TarjanVertex][]*TarjanVertex)

	// insure the TarjanVertex Pointer in keyset and valueset is the same if there have same vertex name
	vMap := make(map[string]*TarjanVertex)
	var newK *TarjanVertex
	var newVV *TarjanVertex
	for k, v := range adjacencyMap {
		if vertex, ok := vMap[k.name]; !ok {
			newK = NewTarjanVertex(k)
			vMap[k.name] = newK
		} else {
			newK = vertex
		}
		var newV []*TarjanVertex
		for _, vv := range v {
			if vertex, ok := vMap[vv.name]; !ok {
				newVV = NewTarjanVertex(vv)
				vMap[vv.name] = newVV
			} else {
				newVV = vertex
			}
			newV = append(newV, newVV)
		}
		tarjanAdjacencyMap[newK] = newV
	}

	return NewTopologicalSorter(tarjanAdjacencyMap).Iterator()
}
