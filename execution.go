package stream_processing

type OutboundCollector interface {

	// offer an item to this collector, if the collector cannot complete the operation, the call must be retried later
	offer(item interface{}) ProgressState

	// getPartitions return the list of partitions handled by this collector
	getPartitions() []int
}