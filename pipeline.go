package stream_processing



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
