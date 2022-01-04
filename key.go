package stream_processing

// GeneralStageWithKey an intermediate step while constructing a group-and aggregation batch pipeline stage
type GeneralStageWithKey interface {

	// keyFn returns the function that extracts the key from stream items. The purpose of the key varies with the operation you apply
	keyFn() ApplyFn

	// mapStateful attaches a stage that performs a stateful mapping function. returns the object that hold the state
	mapStateful(createFn GetFn, mapFn TriApplyFn) GeneralStage

	// filterStateful attaches a stage that performs a stateful filtering operation. returns the object that holds the state
	filterStateful(createFn GetFn, filterFn BiTest) GeneralStage

	// flatMapStateful attaches a stage that performs a stateful flat-mapping operation. returns the object that holds the state
	flatMapStateful(createFn GetFn, flatMapFn TriApplyFn) GeneralStage

	// mapUsingService attaches a mapping stage which applies the given function to each input item independently and emits the function's result as the output item
	mapUsingService(serviceFactory ServiceFactory, mapFn TriApplyFn) GeneralStage

	// mapUsingServiceAsync asynchronous version of mapUsingService
	mapUsingServiceAsync(serviceFactory ServiceFactory, maxConcurrentOps int, preserveOrder bool, mapAsyncFn TriApplyFn)

	// filterUsingService attaches a filtering stage which applies the provided predicate function
	// to each input item to decide whether to pass the item to the output or
	// to discard it.
	filterUsingService(serviceFactory ServiceFactory, filterFn TriTestFn) GeneralStage

	// customTransform attaches a stage with a custom transform based on the provided supplier of core api
	customTransform(stageName string, procSupplier GetFn) GeneralStage
}


// BatchStageWithKey an intermediate step while constructing a group-and aggregation batch pipeline stage
type BatchStageWithKey interface {
	GeneralStageWithKey

	// distinct attaches a stage that emits just the items that are distinct according to the grouping key
	distinct()

}

// StreamStageWithKey ...
type StreamStageWithKey interface {
	GeneralStageWithKey

	// window adds the definition of the window to use in the group-and-aggregate pipeline stage being constructed.
	window(wDef WindowDefinition) StageWithKeyAndWindow

}

// StageWithKeyAndWindow captures the grouping key and the window definition, and offers the method of finalize the construction by specifying the aggregation operation and any additional pipeline
type StageWithKeyAndWindow interface {

	// keyFn returns the function that extracts the grouping key from stream items
	keyFn() ApplyFn

	windowDefinition() WindowDefinition



}
