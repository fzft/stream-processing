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

func NewSlidingWindowPolicy(frameSize, frameOffset, framesPerWindow int64) *SlidingWindowPolicy {
	return &SlidingWindowPolicy{frameSize: frameSize, frameOffset: frameOffset, windowSize: frameSize * framesPerWindow}
}

// NewSlidingWithPolicy returns the definition of a sliding window of length windowSize that slides by slideBy
// Given windowSize = 4 and slideBy = 2 , the generated windows would cover timestamps [-2, 2), [0, 4), [2,6) ...
func NewSlidingWithPolicy(windowSize, slideBy int64) *SlidingWindowPolicy {
	return NewSlidingWindowPolicy(slideBy, 0, windowSize/slideBy)
}

// NewTumblingWithPolicy returns the definition of a tumbling window of length windowSize
// Given windowSize =4 , the generated windows would cover timestamps [-4, 0), [0, 4) , [4, 8) ...
func NewTumblingWithPolicy(windowSize int64) *SlidingWindowPolicy {
	return NewSlidingWithPolicy(windowSize, windowSize)

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
	return SubtractClamped(timestamp, floorMod(x-p.frameOffset, p.frameSize))
}

// higherFrameTs returns the lowest frame timestamp greater than or equal to the given timestamp. if there is no such value, return Max_Value
func (p *SlidingWindowPolicy) higherFrameTs(timestamp int64) int64 {
	tsPlusFrame := timestamp + p.frameSize
	if sumHadOverflow(timestamp, p.frameSize, tsPlusFrame) {
		return addClamped(p.floorFrameTs(timestamp), p.frameSize)
	} else {
		return p.floorFrameTs(tsPlusFrame)
	}
}

// withOffset returns a new window definition where all the frames are shifted by the given offset
func (p *SlidingWindowPolicy) withOffset(offset int64) *SlidingWindowPolicy {
	return NewSlidingWindowPolicy(p.frameSize, offset, p.windowSize/p.frameSize)
}

// toTumblingByFrame converts this definition to one defining a tumbling window of the same length as this definition's frame
func (p *SlidingWindowPolicy) toTumblingByFrame() *SlidingWindowPolicy {
	return NewSlidingWindowPolicy(p.frameSize, p.frameOffset, 1)
}
