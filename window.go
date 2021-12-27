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

// SlidingWindowPolicy contains parameters that define a sliding/tumbling window over which will apply an aggregate function
// A frame is labelled with its timestamp
type SlidingWindowPolicy struct {
	frameSize   int64
	frameOffset int64
	windowSize  int64
}

func NewSlidingWindowPolicy(frameSize int64, frameOffset int64, framesPerWindow int64) *SlidingWindowPolicy {
	return &SlidingWindowPolicy{frameSize: frameSize, frameOffset: frameOffset, windowSize: frameSize * framesPerWindow}
}

// isTumbling tells whether this definition describes a tumbling window
func (p *SlidingWindowPolicy) isTumbling() bool {
	return p.frameSize == p.windowSize
}

// floorFrameTs returns the highest frame timestamp less than or equal to the given timestamp. if there is no such value, return Min_Value
func (p *SlidingWindowPolicy) floorFrameTs(timestamp int64) int64 {
	var x int64
	if timestamp >= Min_Value+p.frameOffset {
		x = timestamp
	} else {
		x = timestamp + p.frameSize
	}
	return SubtractClamped(timestamp, (x-p.frameOffset)%p.frameSize)
}
