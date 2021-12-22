package stream_processing

// Inbox a subset of queue restricted to the consumer side
type Inbox interface {
	isEmpty()

	// peek Retrieves, but does not remove
	peek() interface{}

	// poll Retrieves and removes the head of this inbox
	poll() interface{}

	remove()

	Iter() []interface{}

	clear()
}

// Outbox data sink for a Processor. the outbox consists of individual output buckets
type Outbox interface {

	// bucketCount returns the number of buckets in this outbox
	bucketCount() int

	// offer the supplied item to the bucket with the supplied ordinal
	offer(ordinal int, item interface{})
}

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
		name:         name,
		metaSupplier: NewMetaSupplierFromProcessorSupplier(LOCAL_PARALLELISM_USE_DEFAULT, processorSupplier),
	}
}

// Watermark ...
type Watermark struct {
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
