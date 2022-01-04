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


type DefaultPartitionStrategyImpl struct {
}

func (d DefaultPartitionStrategyImpl) getPartition(o interface{}) int {
	if v, ok := o.(int); ok {
		return v
	}
	return 0
}

type DefaultPartitioner struct {
	defaultPartitioning DefaultPartitionStrategy
}

func NewDefaultPartitioner() *DefaultPartitioner {
	p := new(DefaultPartitioner)
	return p
}

func (d *DefaultPartitioner) init(defaultPartitioning DefaultPartitionStrategy) {
	d.defaultPartitioning = defaultPartitioning
}
func (d *DefaultPartitioner) getPartition(item interface{}, partitionCount int) int {
	return d.defaultPartitioning.getPartition(item)
}

type KeyPartitioner struct {
	keyExtractor ApplyFn
	partitioner  Partitioner
}

func NewKeyPartitioner(keyExtractor ApplyFn, partitioner Partitioner) *KeyPartitioner {
	return &KeyPartitioner{keyExtractor: keyExtractor, partitioner: partitioner}
}

func (k *KeyPartitioner) init(strategy DefaultPartitionStrategy) {
	k.partitioner.init(strategy)

}
func (k *KeyPartitioner) getPartition(item interface{}, partitionCount int) int {
	key := k.keyExtractor(item)
	if key == nil {
		panic("Null key from key extractor")
	}
	return k.partitioner.getPartition(key, partitionCount)
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

type TestPartitioner struct {
	val int
}

func NewTestPartitioner(val int) *TestPartitioner {
	return &TestPartitioner{val: val}
}

func (t *TestPartitioner) getPartition(item interface{}, partitionCount int) int {
	return t.val
}

func (t *TestPartitioner) init(strat DefaultPartitionStrategy) {
	panic("implement me")
}







