package stream_processing

// WatermarkPolicy tracks and determines the current Watermark given the event timestamps as they occur for a single input stream
type WatermarkPolicy interface {
	// reportEvent called to report the observation of an event with the given timestamp
	reportEvent(timestamp int64)

	// getCurrentWatermark called to get the current watermark
	getCurrentWatermark() int64
}

type limitingLagSupplier struct {
	lag int64
}

func newLimitingLagSupplier(lag int64) *limitingLagSupplier {
	return &limitingLagSupplier{lag: lag}
}

func (l *limitingLagSupplier) get() interface{} {
	return newLimitingLag(l.lag)
}

// limitingLag maintains a watermark that lags behind the top observed timestamp by the given amount
type limitingLag struct {
	wm  int64
	lag int64
}

func newLimitingLag(lag int64) *limitingLag {
	return &limitingLag{lag: lag, wm: Min_Value}
}

func (l *limitingLag) reportEvent(timestamp int64) {
	if timestamp >= Min_Value+l.lag {
		l.wm = Max64(l.wm, timestamp-l.lag)
	}
}

func (l *limitingLag) getCurrentWatermark() int64 {
	return l.wm
}

// Watermark ...
type Watermark struct {
	timestamp int64
}

func NewWatermark(timestamp int64) *Watermark {
	return &Watermark{timestamp: timestamp}
}
