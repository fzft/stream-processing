package stream_processing

import "context"

const (
	LOCAL_PARALLELISM_USE_DEFAULT = -1
)

// Transform is a pure data object and holds no implementation code for the transformation it presents
type Transform interface {

	// name return the name of this transformation
	getName() string

	setName(name string)

	getLocalParallelism() int

	setLocalParallelism(localParallelism int)

	getDeterminedLocalParallelism() int

	setDeterminedLocalParallelism(determinedLocalParallelism int)

	setRebalanceInput(ordinal int, value bool)

	setPartitionKeyFnForInput(ordinal int, keyFn FunctionEx)

	shouldRebalanceInput(ordinal int) bool

	partitionKeyFnForInput(ordinal int) FunctionEx

	getUpstream() []Transform

	addToDag(context context.Context, p Planner)

	// preferredWatermarkStride returns the optimal watermark stride for this window transform
	preferredWatermarkStride() int64
}

type AbstractTransform struct {
	name                       string
	upstream                   []Transform
	localParallelism           int
	determinedLocalParallelism int
	upstreamRebalancingFlags   []bool
	upstreamPartitionKeyFns    []FunctionEx
}

func NewAbstractTransform(name string, upstream []Transform) *AbstractTransform {
	return &AbstractTransform{
		name:                       name,
		upstream:                   upstream,
		localParallelism:           LOCAL_PARALLELISM_USE_DEFAULT,
		determinedLocalParallelism: LOCAL_PARALLELISM_USE_DEFAULT,
	}
}

func (a *AbstractTransform) getName() string {
	return a.name
}

func (a *AbstractTransform) setName(name string) {
	a.name = name
}

func (a *AbstractTransform) getLocalParallelism() int {
	return a.localParallelism
}

func (a *AbstractTransform) setLocalParallelism(localParallelism int) {
	a.localParallelism = checkLocalParallelism(localParallelism)
}

func (a *AbstractTransform) getDeterminedLocalParallelism() int {
	return a.determinedLocalParallelism
}

func (a *AbstractTransform) setDeterminedLocalParallelism(determinedLocalParallelism int) {
	a.determinedLocalParallelism = checkLocalParallelism(determinedLocalParallelism)
}

func (a *AbstractTransform) setRebalanceInput(ordinal int, value bool) {
	a.upstreamRebalancingFlags[ordinal] = value
}

func (a *AbstractTransform) setPartitionKeyFnForInput(ordinal int, keyFn FunctionEx) {
	a.upstreamPartitionKeyFns[ordinal] = keyFn
}

func (a *AbstractTransform) shouldRebalanceInput(ordinal int) bool {
	return a.upstreamRebalancingFlags[ordinal]
}

func (a *AbstractTransform) partitionKeyFnForInput(ordinal int) FunctionEx {
	return a.upstreamPartitionKeyFns[ordinal]
}

func (a *AbstractTransform) getUpstream() []Transform {
	return a.upstream
}

func (a *AbstractTransform) preferredWatermarkStride() int64 {
	return 0
}

func (a *AbstractTransform) addToDag(context context.Context, p Planner) {
	panic("implement me")
}

type StreamSourceTransform struct {
}

type BatchSourceTransform struct {
	t                 *AbstractTransform
	metaSupplier      ProcessorMetaSupplier
	isAssignedToStage bool
}

func NewBatchSourceTransform(name string, metaSupplier ProcessorMetaSupplier) *BatchSourceTransform {
	t := new(BatchSourceTransform)
	t.t = NewAbstractTransform(name, []Transform{})
	t.metaSupplier = metaSupplier
	return t
}

func (b *BatchSourceTransform) getName() string {
	return b.t.name
}

func (b *BatchSourceTransform) setName(name string) {
	b.t.setName(name)
}

func (b *BatchSourceTransform) getLocalParallelism() int {
	return b.t.getLocalParallelism()
}

func (b *BatchSourceTransform) setLocalParallelism(localParallelism int) {
	b.t.setLocalParallelism(localParallelism)
}

func (b *BatchSourceTransform) getDeterminedLocalParallelism() int {
	return b.t.getDeterminedLocalParallelism()
}

func (b *BatchSourceTransform) setDeterminedLocalParallelism(determinedLocalParallelism int) {
	b.t.setDeterminedLocalParallelism(determinedLocalParallelism)
}

func (b *BatchSourceTransform) setRebalanceInput(ordinal int, value bool) {
	b.t.setRebalanceInput(ordinal, value)
}

func (b *BatchSourceTransform) setPartitionKeyFnForInput(ordinal int, keyFn FunctionEx) {
	b.t.setPartitionKeyFnForInput(ordinal, keyFn)
}

func (b *BatchSourceTransform) shouldRebalanceInput(ordinal int) bool {
	return b.t.shouldRebalanceInput(ordinal)
}

func (b *BatchSourceTransform) partitionKeyFnForInput(ordinal int) FunctionEx {
	return b.t.partitionKeyFnForInput(ordinal)
}

func (b *BatchSourceTransform) getUpstream() []Transform {
	return b.t.getUpstream()
}

func (b *BatchSourceTransform) addToDag(context context.Context, p Planner) {
	panic("implement me")
}

func (b *BatchSourceTransform) preferredWatermarkStride() int64 {
	return b.t.preferredWatermarkStride()
}

// checkLocalParallelism whether the given integer is valid
func checkLocalParallelism(parallelism int) int {
	if parallelism != LOCAL_PARALLELISM_USE_DEFAULT && parallelism < 0 {
		panic("Parallelism must be either -1 or a positive number")
	}
	return parallelism
}
