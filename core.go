package stream_processing



// Vertex represents a unit of data processing in a computation job.
// Vertex receives data items over its inbound Edge and pushes data items to its outbound Edge
// a single Vertex  is represented by a set of instances of Processor
type Vertex struct {
	name             string
	localParallelism int
	metaSupplier     ProcessorMetaSupplier
}

func NewVertex(name string, processorSupplier ProcessorSupplier) *Vertex {
	return &Vertex{
		localParallelism: -1,
		name:             name,
		metaSupplier:     NewMetaSupplierFromProcessorSupplier(LOCAL_PARALLELISM_USE_DEFAULT, processorSupplier),
	}
}

// Sources contains factory methods for various types of pipeline sources
type Sources struct {
}

func NewSources() Sources {
	return Sources{}
}

func (s Sources) batchFromProcessor(sourceName string, metaSupplier ProcessorMetaSupplier) BatchSource {
	//return NewBatchSource(s)
	return nil
}
