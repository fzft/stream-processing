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
	flatMap(flatMapFn FunctionEx) GeneralStage
}
