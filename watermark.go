package stream_processing

// WatermarkPolicy tracks and determines the current Watermark given the event timestamps as they occur for a single input stream
type WatermarkPolicy interface {
	// reportEvent called to report the observation of an event with the given timestamp
	reportEvent(timestamp int64)

	// getCurrentWatermark called to get the current watermark
	getCurrentWatermark() int64

}

// Watermark ...
type Watermark struct {
	timestamp int64
}

func NewWatermark(timestamp int64) *Watermark {
	return &Watermark{timestamp: timestamp}
}


