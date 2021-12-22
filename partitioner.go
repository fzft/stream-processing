package stream_processing

// Partitioner decides on the partition ID of an item traveling over it.
// the partition ID determines which cluster member and which instance of Processor on that member an item will be forwarded to
type Partitioner interface {

	// getPartition return the partition ID of the given item
	getPartition(item interface{}, partitionCount int) int

	// init callback that injects the default partitioning strategy into partitioner, so it can be consulted
	init(strat DefaultPartitionStrategy)
}

// DefaultPartitionStrategy ...
type DefaultPartitionStrategy interface {

	// getPartition return the partition ID of the given item
	getPartition(o interface{}) int
}

type DefaultPartitioner struct {
	defaultPartitioning DefaultPartitionStrategy
}

func NewDefaultPartitioner() *DefaultPartitioner {
	return &DefaultPartitioner{}
}

func (d *DefaultPartitioner) init(defaultPartitioning DefaultPartitionStrategy) {
	d.defaultPartitioning = defaultPartitioning
}
func (d *DefaultPartitioner) getPartition(item interface{}, partitionCount int) int {
	return d.defaultPartitioning.getPartition(item)
}

type KeyPartitioner struct {
	keyExtractor FunctionEx
	partitioner  Partitioner
}

func NewKeyPartitioner(keyExtractor FunctionEx, partitioner Partitioner) *KeyPartitioner {
	return &KeyPartitioner{keyExtractor: keyExtractor, partitioner: partitioner}
}

func (k *KeyPartitioner) init(strategy DefaultPartitionStrategy) {
	k.partitioner.init(strategy)

}
func (k *KeyPartitioner) getPartition(item interface{}, partitionCount int) int {
	key := k.keyExtractor.apply(item)
	if key == nil {
		panic("Null key from key extractor")
	}
	return k.partitioner.getPartition(item, partitionCount)
}

type SinglePartitioner struct {
	key       interface{}
	partition int
}

func NewSinglePartitioner(key interface{}) *SinglePartitioner {
	return &SinglePartitioner{key: key}
}

func (s *SinglePartitioner) getPartition(item interface{}, partitionCount int) int {
	return s.partition
}

func (s *SinglePartitioner) init(strat DefaultPartitionStrategy) {
	s.partition = strat.getPartition(s.key)
}
