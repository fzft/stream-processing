package stream_processing

import (
	"context"
)

// Processor when execute a Dag, it creates one or more instance of Processor on each cluster member to do the work of a given vertex.
// the vertex localParallelism property controls the number of processors per member
// the Processor is a single-threaded processing unit that performs the computation need to transform zero or more input data streams into zero or more output streams
// the Processor accepts input from instances of Inbox and pushes its output to an instance of Outbox
type Processor interface {
	// isCooperative whether this processor is able to participate in cooperative multithreading
	isCooperative() bool

	// init Initializes this processor with the outbox
	init(context context.Context, outbox Outbox)

	// process called with a batch of items retrieved from an inbound edge's stream
	process(ordinal int, inbox Inbox)

	// tryProcessWatermark ...
	tryProcessWatermark(watermark Watermark) bool

	// tryProcess this method will be called periodically and only when the current batch of items in the inbox has been
	// exhausted.
	tryProcess(ordinal int, item interface{}) bool

	// complete called after all the inbound edges' streams are exhausted. if it returns false, it will be invoked again until it return true
	complete() bool
}

// ProcessorSupplier factory Processor instance
type ProcessorSupplier interface {
}

// ProcessorMetaSupplier factory of ProcessorSupplier instances
type ProcessorMetaSupplier interface {

	// getTags returns the metadata on this supplier, a string-to-string map, There is no predefined metadata;
	getTags()

	// preferredLocalParallelism return the local parallelism
	getPreferredLocalParallelism() int

	// init called on the cluster member that receives the client request
	init(ctx context.Context)

	// get called to create a mapping from member Address to the ProcessorSupplier that will be sent to that member
	get(address []interface{}) ApplyFn
}

// NoopP A no-operation processor
type NoopP struct {
	outbox Outbox
}

func (n NoopP) tryProcess(ordinal int, item interface{}) bool {
	panic("implement me")
}

func (n NoopP) complete() bool {
	panic("implement me")
}

func NewNoopP() *NoopP {
	return &NoopP{}
}

func (n NoopP) get() interface{} {
	panic("implement me")
}

func (n NoopP) isCooperative() bool {
	return true
}

func (n NoopP) init(context context.Context, outbox Outbox) {
	n.outbox = outbox
}

func (n NoopP) process(ordinal int, inbox Inbox) {

}

func (n NoopP) tryProcessWatermark(watermark Watermark) bool {
	return true
}

type MetaSupplierFromProcessorSupplier struct {
	preferredLocalParallelism int
	processorSupplier         ProcessorSupplier
}

func NewMetaSupplierFromProcessorSupplier(preferredLocalParallelism int, processorSupplier ProcessorSupplier) *MetaSupplierFromProcessorSupplier {
	return &MetaSupplierFromProcessorSupplier{preferredLocalParallelism: preferredLocalParallelism, processorSupplier: processorSupplier}
}

func (m MetaSupplierFromProcessorSupplier) getTags() {
	panic("implement me")
}

func (m MetaSupplierFromProcessorSupplier) getPreferredLocalParallelism() int {
	return m.preferredLocalParallelism
}

func (m MetaSupplierFromProcessorSupplier) init(ctx context.Context) {
	panic("implement me")
}

func (m MetaSupplierFromProcessorSupplier) get(address []interface{}) ApplyFn {
	panic("implement me")
}

// AbstractProcessor base class to implement custom processors
type AbstractProcessor struct {
	outbox      Outbox
	pendingItem interface{}
}

func (p *AbstractProcessor) complete() bool {
	panic("implement me")
}

func (p *AbstractProcessor) isCooperative() bool {
	return true
}

// process the boilerplate of dispatching against the ordinal, taking items from the inbox one by one, and invoking the processing logic on each
func (p *AbstractProcessor) process(ordinal int, inbox Inbox) {
	switch ordinal {
	case 0:
		p.process0(inbox)
		break
	case 1:
		p.process1(inbox)
		break
	case 2:
		p.process2(inbox)
		break
	case 3:
		p.process3(inbox)
		break
	default:
		p.processAny(ordinal, inbox)
	}
}

// tryProcessWatermark this basic implementation only forwards the passed watermark
func (p *AbstractProcessor) tryProcessWatermark(watermark Watermark) bool {
	return p.tryEmit(-1, watermark)
}

func (p *AbstractProcessor) init(ctx context.Context, outbox Outbox) {
	p.outbox = outbox
	p.initContext(ctx)
}

// restoreFromSnapshot implements the boilerplate of polling the inbox, casting the item to key, value pair
func (p *AbstractProcessor) restoreFromSnapshot(inbox Inbox) {
	var item interface{}
	for ; item != nil; item = inbox.poll() {
		if entry, ok := item.(MapEntry); ok {
			p.restoreFromSnapshotWithMapEntry(entry)
		}
	}
}

func (p *AbstractProcessor) initContext(ctx context.Context) {
}

// restoreFromSnapshotWithMapEntry called to restore one key-value pair from snapshot to processor's internal state
func (p *AbstractProcessor) restoreFromSnapshotWithMapEntry(entry MapEntry) {
	panic("implement me")
}

// emitFromTraverser obtains items from traverser and offers them to the outbox's buckets identified in the supplier array
// if the outbox refuse an item, it backs off and returns false
func (p *AbstractProcessor) emitFromTraverser(ordinal int, traverser Traverser) bool {
	var item interface{}
	if p.pendingItem != nil {
		item = p.pendingItem
		p.pendingItem = nil
	} else {
		item = traverser.next()
	}
	for ; item != nil; item = traverser.next() {
		if !p.tryEmit(ordinal, item) {
			p.pendingItem = item
			return false
		}
	}
	return true
}

// emitFromTraverserWithMany obtains items from traverser and offers them to the outbox's buckets identified in the supplier array
// if the outbox refuse an item, it backs off and returns false
func (p *AbstractProcessor) emitFromTraverserWithMany(ordinal []int, traverser Traverser) bool {
	var item interface{}
	if p.pendingItem != nil {
		item = p.pendingItem
		p.pendingItem = nil
	} else {
		item = traverser.next()
	}
	for ; item != nil; item = traverser.next() {
		if !p.tryEmitWithMany(ordinal, item) {
			p.pendingItem = item
			return false
		}
	}
	return true
}

// process0 the processN methods contain repeated looping code in order to give an easier job to compiler to optimize each case independently, and to ensure that ordinal is dispatched on just once per process all
func (p *AbstractProcessor) process0(inbox Inbox) {
	var item interface{}
	for ; item != nil && p.tryProcess0(item); item = inbox.peek() {
		inbox.remove()
	}
}

// tryProcess1 tries to process the supplied input item, which was received from the edge with ordinal 0
func (p *AbstractProcessor) tryProcess0(item interface{}) bool {
	return p.tryProcess(0, item)
}

// process1 the processN methods contain repeated looping code in order to give an easier job to compiler to optimize each case independently, and to ensure that ordinal is dispatched on just once per process all
func (p *AbstractProcessor) process1(inbox Inbox) {
	var item interface{}
	for ; item != nil && p.tryProcess1(item); item = inbox.peek() {
		inbox.remove()
	}
}

// tryProcess1 tries to process the supplied input item, which was received from the edge with ordinal 1
func (p *AbstractProcessor) tryProcess1(item interface{}) bool {
	return p.tryProcess(1, item)
}

// process2 the processN methods contain repeated looping code in order to give an easier job to compiler to optimize each case independently, and to ensure that ordinal is dispatched on just once per process all
func (p *AbstractProcessor) process2(inbox Inbox) {
	var item interface{}
	for ; item != nil && p.tryProcess2(item); item = inbox.peek() {
		inbox.remove()
	}
}

// tryProcess2 tries to process the supplied input item, which was received from the edge with ordinal 2
func (p *AbstractProcessor) tryProcess2(item interface{}) bool {
	return p.tryProcess(2, item)

}

// process3 the processN methods contain repeated looping code in order to give an easier job to compiler to optimize each case independently, and to ensure that ordinal is dispatched on just once per process all
func (p *AbstractProcessor) process3(inbox Inbox) {
	var item interface{}
	for ; item != nil && p.tryProcess3(item); item = inbox.peek() {
		inbox.remove()
	}
}

// tryProcess3 tries to process the supplied input item, which was received from the edge with ordinal 3
func (p *AbstractProcessor) tryProcess3(item interface{}) bool {
	return p.tryProcess(3, item)
}

// process4 the processN methods contain repeated looping code in order to give an easier job to compiler to optimize each case independently, and to ensure that ordinal is dispatched on just once per process all
func (p *AbstractProcessor) process4(inbox Inbox) {
	var item interface{}
	for ; item != nil && p.tryProcess3(item); item = inbox.peek() {
		inbox.remove()
	}
}

// tryProcess4 tries to process the supplied input item, which was received from the edge with ordinal 4
func (p *AbstractProcessor) tryProcess4(item interface{}) bool {
	return p.tryProcess(4, item)
}

func (p *AbstractProcessor) tryProcess(ordinal int, item interface{}) bool {
	panic("implement me")
}

func (p *AbstractProcessor) processAny(ordinal int, inbox Inbox) {
	var item interface{}
	for ; item != nil && p.tryProcess(ordinal, item); item = inbox.peek() {
		inbox.remove()
	}
}

// tryEmit offers the item at the supplied ordinal
// return true if the item was accepted, if false returned, the call must be retired later with the same item
func (p *AbstractProcessor) tryEmit(ordinal int, item interface{}) bool {
	return p.outbox.offer(ordinal, item)
}

func (p *AbstractProcessor) tryEmitWithMany(ordinal []int, item interface{}) bool {
	return p.outbox.offerWithMany(ordinal, item)
}

// GroupP batch processor that groups items by key and computes the supplied aggregate operation on each group
type GroupP struct {
	abstractProcessor *AbstractProcessor
	keyToAcc          map[interface{}]interface{}
	groupKeyFns       []ApplyFn
	aggrOp            AggregateOperation
	resultTraverser   Traverser
	mapToOutputFn     BiApplyFn
}

func NewGroupP(groupKeyFns []ApplyFn, aggrOp AggregateOperation, mapToOutputFn BiApplyFn) *GroupP {
	return &GroupP{groupKeyFns: groupKeyFns, aggrOp: aggrOp, mapToOutputFn: mapToOutputFn, abstractProcessor: &AbstractProcessor{}, keyToAcc: make(map[interface{}]interface{})}
}

func (p *GroupP) isCooperative() bool {
	return true
}

func (p *GroupP) init(context context.Context, outbox Outbox) {
	p.abstractProcessor.init(context, outbox)
}

func (p *GroupP) process(ordinal int, inbox Inbox) {
	p.abstractProcessor.process(ordinal, inbox)
}

func (p *GroupP) tryProcessWatermark(watermark Watermark) bool {
	return p.abstractProcessor.tryProcessWatermark(watermark)
}

func (p *GroupP) tryProcess(ordinal int, item interface{}) bool {
	var (
		acc interface{}
		ok  bool
	)
	keyFn := p.groupKeyFns[ordinal]
	key := keyFn(item)
	if acc, ok = p.keyToAcc[key]; !ok {
		acc = p.aggrOp.getCreateFn()
	}
	p.aggrOp.accumulateFn(ordinal)(acc, item)
	return true
}

func (p *GroupP) complete() bool {
	if p.resultTraverser == nil {
		p.resultTraverser = NewResultTraverser(p.keyToAcc).mapX(func(t interface{}) interface{} {
			if entry, ok := t.(MapEntry); ok {
				return p.mapToOutputFn(entry.key, p.aggrOp.getFinishFn()(entry.value))
			}
			return nil
		})
	}
	return p.abstractProcessor.emitFromTraverser(-1, p.resultTraverser)
}

type GroupMapFn struct {
	mapToOutputFn BiApplyFn
	aggrOp        AggregateOperation
}

func NewGroupMapFn(mapToOutputFn BiApplyFn, aggrOp AggregateOperation) *GroupMapFn {
	return &GroupMapFn{mapToOutputFn: mapToOutputFn, aggrOp: aggrOp}
}

func (g GroupMapFn) apply(t interface{}) interface{} {
	if entry, ok := t.(MapEntry); ok {
		return g.mapToOutputFn(entry.key, g.aggrOp.getFinishFn()(entry.value))
	}
	return nil
}

type TransformP struct {
	*AbstractProcessor
	flatMapper *FlatMapper
}

func NewTransformP(mapper ApplyFn) *TransformP {
	p := new(TransformP)
	p.AbstractProcessor = &AbstractProcessor{}
	p.flatMapper = NewFlatMapper(nil, mapper, p.AbstractProcessor)
	return p
}

func (p *TransformP) tryProcess(ordinal int, item interface{}) bool {
	return p.flatMapper.tryProcess(item)
}

// MapP processor for a vertex which, for each received item, emits the result of applying the given mapping function to it
type MapP struct {
	*TransformP
}

func NewMapP(mapFn ApplyFn) *MapP {
	p := new(MapP)
	trav := &ResettableSingletonTraverser{}
	return &MapP{TransformP: NewTransformP(func(t interface{}) interface{} {
		trav.accept(mapFn(t))
		return trav
	})}
	return p
}

type AggregateP struct {
	*GroupP
}

const CONSTANT_KEY = "ALL"

func NewAggregateP(aggrOp AggregateOperation) *AggregateP {
	p := new(AggregateP)
	groupKeyFns := make([]ApplyFn, aggrOp.arity())
	for i, _ := range groupKeyFns {
		groupKeyFns[i] = func(t interface{}) interface{} {
			return CONSTANT_KEY
		}
	}
	p.GroupP = NewGroupP(groupKeyFns, aggrOp, func(k, r interface{}) interface{} {
		return r
	})
	p.keyToAcc[CONSTANT_KEY] = aggrOp.getCreateFn()
	return p
}

// FlatMapper a helper that simplifies the implementation of tryProcess for emit collection
// User supplier a mapper which takes an item and returns a traverser over all output items that should be emitted
type FlatMapper struct {
	outputOrdinals  []int
	mapper          ApplyFn
	outputTraverser Traverser
	p               *AbstractProcessor
}

func NewFlatMapper(outputOrdinals []int, mapper ApplyFn, p *AbstractProcessor) *FlatMapper {
	return &FlatMapper{outputOrdinals: outputOrdinals, mapper: mapper, p: p}
}

func (m *FlatMapper) tryProcess(item interface{}) bool {
	if m.outputTraverser == nil {
		v := m.mapper(item)
		if t, ok := v.(Traverser); ok {
			m.outputTraverser = t
		}
	}
	if m.emit() {
		m.outputTraverser = nil
		return true
	}
	return false
}

func (m *FlatMapper) emit() bool {
	if m.outputOrdinals != nil {
		return m.p.emitFromTraverserWithMany(m.outputOrdinals, m.outputTraverser)
	} else {
		return m.p.tryEmit(-1, m.outputTraverser)
	}
}
