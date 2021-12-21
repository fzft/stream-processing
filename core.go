package stream_processing

import "context"

type ProcessorMetaSupplier interface {

	// getTags returns the metadata on this supplier, a string-to-string map, There is no predefined metadata;
	getTags()

	// preferredLocalParallelism return the local parallelism
	preferredLocalParallelism() int

	// init called on the cluster member that receives the client request
	init(ctx context.Context)

	// get called to create a mapping from member Address to the ProcessorSupplier that will be sent to that member
	get(address []interface{}) FunctionEx


}
