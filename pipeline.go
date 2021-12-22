package stream_processing

// BatchSource a finite source of data for pipeline
type BatchSource interface {
	name() string
}

// StreamSource an infinite source of data for pipeline.
type StreamSource interface {

	// name return a descriptive name of this source
	name() string

	// supportNativeTimestamps return true if this source supports StreamSourceStage
	supportNativeTimestamps() bool

	// setPartitionIdleTimeout set a timeout after which idle partitions will be excluded from watermark coalescing. the source will advance the watermark based on events from other partitions and will ignore the idle paritions
	// the default timeout is 60 secs
	setPartitionIdleTimeout(timeout int64) StreamSource

	// partitionIdleTimeout return the value set by setPartitionIdleTimeout
	partitionIdleTimeout() int64
}

// Sink accepts the data the pipeline processed and exports it to an external system
type Sink interface {
	name() string
}

// Pipeline is a container of all the stages defined on a pipeline
type Pipeline interface {
	// create a new, empty pipeline
	create() Pipeline

	// isPreserveOrder return the preserve order property of this pipeline
	isPreserveOrder() bool

	// setPreserveOrder whether or not it is allowed to reorder the events for better performance
	setPreserveOrder(value bool) Pipeline

	// readFromBatchSource return a pipeline stage that represents a bounded data source
	readFromBatchSource(source BatchSource) BatchStage

	// readFromStreamSource return a pipeline stage that represents a unbounded data source
	readFromStreamSource(source StreamSource) StreamSourceStage

	// writeTo attached the supplied sink to two or more pipeline stages, return the representing the sink
	writeTo(sink Sink, stages ...GeneralStage) SinkStage

	// isEmpty return true if there are no stages in the pipeline
	isEmpty() bool
}

type ServiceFactory struct {
}

// PipelineImpl implementation of Pipeline
type PipelineImpl struct {
	adjacencyMap  map[Transform][]Transform
	attachedFiles map[string]interface{}
	preserveOrder bool
}

func (p PipelineImpl) create() Pipeline {
	panic("implement me")
}

func (p PipelineImpl) isPreserveOrder() bool {
	panic("implement me")
}

func (p PipelineImpl) setPreserveOrder(value bool) Pipeline {
	panic("implement me")
}

func (p PipelineImpl) readFromBatchSource(source BatchSource) BatchStage {
	panic("implement me")
}

func (p PipelineImpl) readFromStreamSource(source StreamSource) StreamSourceStage {
	panic("implement me")
}

func (p PipelineImpl) writeTo(sink Sink, stages ...GeneralStage) SinkStage {
	panic("implement me")
}

func (p PipelineImpl) isEmpty() bool {
	panic("implement me")
}

type Planner struct {
	xform2vertex map[Transform]PlannerVertex
	pipeline     Pipeline
}

func NewPlanner(pipeline Pipeline) *Planner {
	return &Planner{
		xform2vertex: make(map[Transform]PlannerVertex),
		pipeline:     pipeline,
	}
}

func (p *Planner) addEdges(transform Transform, vertex Vertex, edgeFn ConsumerFn) {
	//var destOrdinal int
	//for _, fromTransform := range transform.getUpstream() {
	//	fromPv := p.xform2vertex[fromTransform]
	//	edge :=
	//}
}

type PlannerVertex struct {
	v                Vertex
	availableOrdinal int
}

func NewPlannerVertex(v Vertex) *PlannerVertex {
	return &PlannerVertex{
		v: v,
	}
}

func (v *PlannerVertex) nextAvailableOrdinal() int {
	v.availableOrdinal++
	return v.availableOrdinal
}
