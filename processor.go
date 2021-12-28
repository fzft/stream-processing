package stream_processing

import "context"

// Processor when execute a Dag, it creates one or more instance of Processor on each cluster member to do the work of a given vertex.
// the vertex localParallelism property controls the number of processors per member
// the Processor is a single-threaded processing unit that performs the computation need to transform zero or more input data streams into zero or more output streams
// the Processor accepts input from instances of Inbox and pushes its output to an instance of Outbox
type Processor interface {
	// isCooperative whether this processor is able to participate in cooperative multithreading
	isCooperative() bool

	// init Initializes this processor with the outbox
	init(context context.Context, outbox Outbox)

	// process called with a batch of items retrieved from an inbound edge's stream
	process(ordinal int, inbox Inbox)

	// tryProcessWatermark ...
	tryProcessWatermark(watermark Watermark) bool
}

// ProcessorSupplier factory Processor instance
type ProcessorSupplier interface {
}

// ProcessorMetaSupplier factory of ProcessorSupplier instances
type ProcessorMetaSupplier interface {

	// getTags returns the metadata on this supplier, a string-to-string map, There is no predefined metadata;
	getTags()

	// preferredLocalParallelism return the local parallelism
	getPreferredLocalParallelism() int

	// init called on the cluster member that receives the client request
	init(ctx context.Context)

	// get called to create a mapping from member Address to the ProcessorSupplier that will be sent to that member
	get(address []interface{}) FunctionEx
}

// NoopP A no-operation processor
type NoopP struct {
	outbox Outbox
}

func NewNoopP() *NoopP {
	return &NoopP{}
}

func (n NoopP) get() interface{} {
	panic("implement me")
}

func (n NoopP) isCooperative() bool {
	return true
}

func (n NoopP) init(context context.Context, outbox Outbox) {
	n.outbox = outbox
}

func (n NoopP) process(ordinal int, inbox Inbox) {

}

func (n NoopP) tryProcessWatermark(watermark Watermark) bool {
	return true
}

type MetaSupplierFromProcessorSupplier struct {
	preferredLocalParallelism int
	processorSupplier         ProcessorSupplier
}

func NewMetaSupplierFromProcessorSupplier(preferredLocalParallelism int, processorSupplier ProcessorSupplier) *MetaSupplierFromProcessorSupplier {
	return &MetaSupplierFromProcessorSupplier{preferredLocalParallelism: preferredLocalParallelism, processorSupplier: processorSupplier}
}

func (m MetaSupplierFromProcessorSupplier) getTags() {
	panic("implement me")
}

func (m MetaSupplierFromProcessorSupplier) getPreferredLocalParallelism() int {
	return m.preferredLocalParallelism
}

func (m MetaSupplierFromProcessorSupplier) init(ctx context.Context) {
	panic("implement me")
}

func (m MetaSupplierFromProcessorSupplier) get(address []interface{}) FunctionEx {
	panic("implement me")
}
