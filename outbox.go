package stream_processing

import "container/list"

// Outbox data sink for a Processor. the outbox consists of individual output buckets
type Outbox interface {

	// bucketCount returns the number of buckets in this outbox
	bucketCount() int

	// offer the supplied item to the bucket with the supplied ordinal
	// if ordinal is -1 , offers the item to all edges
	// return true if the outbox accepted the item
	offer(ordinal int, item interface{}) bool

	offerWithMany(ordinal []int, item interface{}) bool

}

// OutboxInternal ...
type OutboxInternal interface {
	// reset the counter that prevents adding more than batchSize, items until this method is called again
	reset()

	// block the outbox so it allows the caller only to offer the current unfinished item . if there is no unfinished item, the outbox will reject all items until you call unblock
	block()

	// unblock removes block call
	unblock()

	// lastForwardedWm returns the timestamp of the last forwarded watermark
	lastForwardedWm() int64
}



//type OutboxImpl struct {
//	outstreams []
//}


type TestOutbox struct {
	buckets []*list.List

}