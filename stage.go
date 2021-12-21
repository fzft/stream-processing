package stream_processing

// Stage ...
type Stage interface {
	// getPipeline returns the pipeline this stage belong to
	getPipeline() Pipeline

	// setLocalParallelism set the preferred local parallelism this stage will configure its DAG vertices
	setLocalParallelism(localParallelism int)

	// setName ...
	setName(name string)

	// name ...
	name() string
}

// GeneralStage the common aspect of BatchStage and StreamStage pipeline stage, defining those operations that apply to both
type GeneralStage interface {
	Stage

	// mapX attaches a mapping stage which applies the given function to each input item in dependently and emits the function's result as the output item
	mapX(mapFn FunctionEx) GeneralStage

	// filter attaches a filtering stage which applies the provided predicate function to each input item to decide whether to pass the item to output or to discard it
	filter(filterFn PredicateEx) GeneralStage

	// flatMap attaches a flat-mapping stage which applies the supplied function to each input item dependently and emits all the items from the Traverser it returns
	// sample:
	// stage.flatMap(sentence -> traverseArray(sentence.split("\\w+")))
	flatMap(flatMapFn FunctionEx) GeneralStage

	// mapStateful attaches a stage that performs a stateful mapping operation, return the object that holds the state
	mapStateful(createFn SupplierEx, mapFn BiFunctionEx) GeneralStage

	// filterStateful attaches a stage that performs a stateful filtering operation, return the object that holds the state
	filterStateful(createFn SupplierEx, filterFn BiFunctionEx) GeneralStage

	// flatMapStateful attaches a stage that performs a stateful flat-mapping operation, return the object that holds the state
	flatMapStateful(createFn SupplierEx, flatMapFn BiFunctionEx) GeneralStage

	// mapUsingService attaches a mapping stage which applies the supplies function to each input item independently and emits the function's result as the output item
	mapUsingService(serviceFactory ServiceFactory, mapFn BiFunctionEx) GeneralStage

	// mapUsingServiceAsync asynchronous version of mapUsingService
	mapUsingServiceAsync(serviceFactory ServiceFactory, maxConcurrentOps int, preserveOrder bool, mapAsyncFn BiFunctionEx) GeneralStage

	// filterUsingService attaches a filtering stage which applies the provided predicate function to each input item to decide wh
	filterUsingService(serviceFactory ServiceFactory, filterFn BiPredicateEx) GeneralStage

	// flatMapUsingService attaches a flat-mapping stage which applies the supplied function to each input item independently and emits all items from Traverser, it returns as the output items
	flatMapUsingService(serviceFactory ServiceFactory,flatMapFn BiFunctionEx) GeneralStage

	// rebalance returns a new stage that applies data rebalancing to the output of this stage
	rebalance() GeneralStage

	// addTimestamps add a timestamp to each item in the stream using the supplied function and specifies the allowed amount of disorder between them
	addTimestamps(timestampFn ToLongFunctionEx, allowedLag int64) StreamStage

	// writeTo attaches a sink stage, one that accepts data but doesn't emit any. the supplied argument specifies what to do with the received data
	writeTo(sink Sink) SinkStage

	// peek attaches a peeking stage which logs this stage's output and passes it through without transformation
	peek(shouldLogFn PredicateEx, toStringFn FunctionEx) GeneralStage

	// customTransform attaches a stage with custom transform based on the provided supplier of Core api
	customTransform(stageName string, procSupplier ProcessorMetaSupplier)

}

type BatchStage interface {
	GeneralStage

	// groupingKey ...
	groupingKey(keyFn FunctionEx) BatchStageWithKey

	// sort attaches a stage that sorts the input items according to their natural order
	sort() BatchStage
}

type StreamStage interface {
	GeneralStage

	// window adds the given window definition to this stage, as the first step in the construction of a pipeline stage that performs windowed aggregation
	window(wDef WindowDefinition) StageWithWindow

	// merge attaches a stage that emits all the items from this stage as well as all the items from the supplied stage
	merge(other StreamStage) StreamStage

	groupingKey(keyFn FunctionEx) StreamStageWithKey

}

// StreamSourceStage a source stage in a distributed computation that will observe an unbound amount of data
type StreamSourceStage interface {

	// withoutTimestamps declare that the source will not assign any timestamps to the events it emits
	withoutTimestamps() StreamStage

	// withIngestionTimestamps declare that the source will assign the time of ingestion as the event timestamps
	withIngestionTimestamps() StreamStage

	// withNativeTimestamps declare that the stream will use the source's native timestamps
	withNativeTimestamps(allowedLag int64) StreamStage

	// withTimestamps declare that the source will extract timestamps from the stream items
	withTimestamps(timestampFn ToLongFunctionEx, allowedLag int64) StreamStage

}

// SinkStage a pipeline stage doesn't allow any downstream stages to be attached to it
type SinkStage interface {
	Stage
}

