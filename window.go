package stream_processing

// StageWithWindow you can perform a global aggregation or add a grouping key to perform a group-and-aggregate operation
type StageWithWindow interface {

	// streamStage return the pipeline stage associated with this object
	streamStage() StreamStage

	windowDefinition() WindowDefinition

	groupingKey(keyFn FunctionEx) StageWithKeyAndWindow

}


// WindowDefinition The definition of the window for a windowed aggregation operation
type WindowDefinition struct {
	earlyResultPeriodMs int64
}



