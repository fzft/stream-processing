package stream_processing

// EventTimePolicy a holder of functions and parameters needs to handle event time and the associated watermark.
// this class should be used EventTimeMapper when implementing a source processor
type EventTimePolicy struct {
	DEFAULT_IDLE_TIMEOUT int64
	// timestampFn extracts the timestamp from an event in the stream
	timestampFn ToLongFunctionEx

	// newWmPolicyFn a factory of watermark policy objects
	newWmPolicyFn SupplierEx

	// wrapFn a function that transforms a given event and its timestamp into the item to emit from the processor
	wrapFn ObjLongBiFunction

	// idleTimeoutMillis ...
	idleTimeoutMillis int64

	// watermarkThrottlingFrameSize ...
	watermarkThrottlingFrameSize int64

	// watermarkThrottlingFrameOffset
	watermarkThrottlingFrameOffset int64
}

func NewEventTimePolicy(timestampFn ToLongFunctionEx, newWmPolicyFn SupplierEx, wrapFn ToLongFunctionEx, idleTimeoutMillis int64, watermarkThrottlingFrameSize int64, watermarkThrottlingFrameOffset int64) *EventTimePolicy {
	return &EventTimePolicy{timestampFn: timestampFn, newWmPolicyFn: newWmPolicyFn, wrapFn: wrapFn, idleTimeoutMillis: idleTimeoutMillis, watermarkThrottlingFrameSize: watermarkThrottlingFrameSize, watermarkThrottlingFrameOffset: watermarkThrottlingFrameOffset}
}


// EventTimeMapper a utility that helps a source emit events according to a given EventTimePolicy. Generally this struct should be used if a source needs emit Watermark
type EventTimeMapper struct {
	NO_NATIVE_TIME int64
	EMPTY_LONGS []int64

	idleTimeoutNanos int64
	timestampFn ToLongFunctionEx
	newWmPolicyFn SupplierEx
	wrapFn ObjLongBiFunction

	watermarkThrottlingFrame
}
