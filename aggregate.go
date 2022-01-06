package stream_processing

// AggregateOperation contains primitives needed to compute an aggregated result of data processing
type AggregateOperation interface {

	// arity return the number of contributing streams this operation is set up to handle
	arity() int

	// getCreateFn a primitive that return a new accumulator
	getCreateFn() GetFn

	// accumulateFn a primitive that updates the accumulator state to account for a new item.
	// the argument identifies index of the contributing streams the return function will handle
	// the function must be stateless and processor isCooperative
	accumulateFn(index int) BiAcceptFn

	// getFinishFn ...
	getFinishFn() ApplyFn

	getDeductFn() BiAcceptFn

	getExportFn() ApplyFn

	getCombineFn() BiAcceptFn

	// withCombiningAccumulateFn return the copy of this aggregate operation, but with the accumulate primitive replaced with one that expects to find accumulator objects in the input items and combines them all into a single accumulator of same type
	withCombiningAccumulateFn(getAccFn ApplyFn) AggregateOperation
}

// AggregateOperation1 extensive AggregateOperation to the arity-1 case
type AggregateOperation1 interface {
	AggregateOperation

	accumulateFn0() BiAcceptFn

	andThen(thenFn ApplyFn) AggregateOperation1
}

// AggregateOperation2 extensive AggregateOperation to the arity-2 case
type AggregateOperation2 interface {
	AggregateOperation

	accumulateFn0() BiAcceptFn

	accumulateFn1() BiAcceptFn

	andThen(thenFn ApplyFn) AggregateOperation2
}

// AggregateOperation3 extensive AggregateOperation to the arity-3 case
type AggregateOperation3 interface {
	AggregateOperation

	accumulateFn0() BiAcceptFn

	accumulateFn1() BiAcceptFn

	accumulateFn2() BiAcceptFn

	andThen(thenFn ApplyFn) AggregateOperation3
}

type AggregateOperationImpl struct {
	accumulateFns []BiAcceptFn
	createFn      GetFn
	combineFn     BiAcceptFn
	deductFn      BiAcceptFn
	exportFn      ApplyFn
	finishFn      ApplyFn
}

func (a *AggregateOperationImpl) withCombiningAccumulateFn(getAccFn ApplyFn) AggregateOperation {
	return NewAggregateOperation1Impl(a.createFn,
		func(acc, item interface{}) {
			a.combineFn(acc, getAccFn(item))
		},
		a.combineFn,
		a.deductFn,
		a.exportFn,
		a.finishFn,
	)
}

func (a *AggregateOperationImpl) getCombineFn() BiAcceptFn {
	return a.combineFn
}

func (a *AggregateOperationImpl) getDeductFn() BiAcceptFn {
	return a.deductFn
}

func (a *AggregateOperationImpl) getExportFn() ApplyFn {
	return a.exportFn
}

func NewAggregateOperationImpl(createFn GetFn, accumulateFns []BiAcceptFn, combineFn BiAcceptFn, deductFn BiAcceptFn, exportFn ApplyFn, finishFn ApplyFn) *AggregateOperationImpl {
	return &AggregateOperationImpl{accumulateFns: accumulateFns, createFn: createFn, combineFn: combineFn, deductFn: deductFn, exportFn: exportFn, finishFn: finishFn}
}

func (a *AggregateOperationImpl) arity() int {
	return len(a.accumulateFns)
}

func (a *AggregateOperationImpl) getCreateFn() GetFn {
	return a.createFn
}

func (a *AggregateOperationImpl) accumulateFn(index int) BiAcceptFn {
	return a.accumulateFns[index]
}

func (a *AggregateOperationImpl) getFinishFn() ApplyFn {
	return a.finishFn
}

type AggregateOperation1Impl struct {
	*AggregateOperationImpl
}

func NewAggregateOperation1Impl(createFn GetFn, accumulateFn BiAcceptFn, combineFn BiAcceptFn, deductFn BiAcceptFn, exportFn ApplyFn, finishFn ApplyFn) *AggregateOperation1Impl {
	a := new(AggregateOperation1Impl)
	a.AggregateOperationImpl = new(AggregateOperationImpl)
	a.accumulateFns = append(a.accumulateFns, accumulateFn)
	a.createFn = createFn
	a.combineFn = combineFn
	a.deductFn = deductFn
	a.exportFn = exportFn
	a.finishFn = finishFn
	return a
}

func (a *AggregateOperation1Impl) andThen(thenFn ApplyFn) AggregateOperation1 {
	return NewAggregateOperation1Impl(a.createFn, a.accumulateFn(0), a.combineFn, a.deductFn, func(t interface{}) interface{} {
		return thenFn(a.exportFn(t))
	}, func(t interface{}) interface{} {
		return thenFn(a.finishFn(t))
	})
}

func (a *AggregateOperation1Impl) accumulateFn0() BiAcceptFn {
	return a.accumulateFn(0)
}

type AggregateOperation2Impl struct {
	*AggregateOperationImpl
}

// aggregateOperation2 return an aggregate operation that is a composite of two independent aggregation operations, each
// one accepting its two own input
func aggregateOperation2(op0, op1 AggregateOperation1, exportFinishFn BiApplyFn) AggregateOperation2 {
	combine0 := op0.getCombineFn()
	combine1 := op1.getCombineFn()
	deduct0 := op0.getDeductFn()
	deduct1 := op1.getDeductFn()
	return NewAggregateOperationBuilder(func() interface{} {
		return NewTuple2(op0.getCreateFn()(), op1.getCreateFn()())
	}).andAccumulate0(func(acc, item interface{}) {
		op0.accumulateFn0()(acc.(Tuple2).f0, item)
	}).andAccumulate1(func(acc, item interface{}) {
		op1.accumulateFn0()(acc.(Tuple2).f1, item)
	}).andCombine(func(acc1, acc2 interface{}) {
		if combine0 == nil || combine1 == nil {
		} else {
			combine0(acc1.(Tuple2).f0, acc2.(Tuple2).f0)
			combine1(acc1.(Tuple2).f1, acc2.(Tuple2).f1)
		}
	}).andDeduct(func(acc1, acc2 interface{}) {
		if deduct0 == nil || deduct1 == nil {
		} else {
			deduct0(acc1.(Tuple2).f0, acc2.(Tuple2).f0)
			deduct1(acc1.(Tuple2).f1, acc2.(Tuple2).f1)
		}
	}).andExport(func(acc interface{}) interface{} {
		return exportFinishFn(op0.getExportFn()(acc.(Tuple2).f0), op1.getExportFn()(acc.(Tuple2).f1))
	}).andFinish(func(acc interface{}) interface{} {
		return exportFinishFn(op0.getExportFn()(acc.(Tuple2).f0), op1.getExportFn()(acc.(Tuple2).f1))
	})

}

func NewAggregateOperation2Impl(createFn GetFn, combineFn BiAcceptFn, deductFn BiAcceptFn, exportFn ApplyFn, finishFn ApplyFn, accumulateFns ...BiAcceptFn) *AggregateOperation2Impl {
	a := new(AggregateOperation2Impl)
	a.AggregateOperationImpl = new(AggregateOperationImpl)
	a.accumulateFns = append(a.accumulateFns, accumulateFns...)
	a.createFn = createFn
	a.combineFn = combineFn
	a.deductFn = deductFn
	a.exportFn = exportFn
	a.finishFn = finishFn
	return a
}

func (a *AggregateOperation2Impl) andThen(thenFn ApplyFn) AggregateOperation2 {
	return NewAggregateOperation2Impl(a.createFn,
		a.combineFn, a.deductFn, func(t interface{}) interface{} {
			return thenFn(a.exportFn(t))
		}, func(t interface{}) interface{} {
			return thenFn(a.finishFn(t))
		}, a.accumulateFns...)
}

func (a *AggregateOperation2Impl) accumulateFn0() BiAcceptFn {
	return a.accumulateFn(0)
}

func (a *AggregateOperation2Impl) accumulateFn1() BiAcceptFn {
	return a.accumulateFn(1)
}

type AggregateOperation3Impl struct {
	*AggregateOperationImpl
}

// aggregateOperation3 return an aggregate operation that is a composite of two independent aggregation operations, each
// one accepting its two own input
func aggregateOperation3(op0, op1, op2 AggregateOperation1, exportFinishFn TriApplyFn) AggregateOperation3 {
	combine0 := op0.getCombineFn()
	combine1 := op1.getCombineFn()
	combine2 := op2.getCombineFn()
	deduct0 := op0.getDeductFn()
	deduct1 := op1.getDeductFn()
	deduct2 := op1.getDeductFn()
	return NewAggregateOperationBuilder(func() interface{} {
		return NewTuple3(op0.getCreateFn()(), op1.getCreateFn()(), op2.getCreateFn()())
	}).andAccumulate0(func(acc, item interface{}) {
		op0.accumulateFn0()(acc.(Tuple3).f0, item)
	}).andAccumulate1(func(acc, item interface{}) {
		op1.accumulateFn0()(acc.(Tuple3).f1, item)
	}).andAccumulate2(func(acc, item interface{}) {
		op1.accumulateFn0()(acc.(Tuple3).f2, item)
	}).
		andCombine(func(acc1, acc2 interface{}) {
			if combine0 == nil || combine1 == nil || combine2 == nil {
			} else {
				combine0(acc1.(Tuple3).f0, acc2.(Tuple3).f0)
				combine1(acc1.(Tuple3).f1, acc2.(Tuple3).f1)
				combine2(acc1.(Tuple3).f2, acc2.(Tuple3).f1)
			}
		}).andDeduct(func(acc1, acc2 interface{}) {
		if deduct0 == nil || deduct1 == nil || deduct2 == nil {
		} else {
			deduct0(acc1.(Tuple3).f0, acc2.(Tuple3).f0)
			deduct1(acc1.(Tuple3).f1, acc2.(Tuple3).f1)
			deduct2(acc1.(Tuple3).f2, acc2.(Tuple3).f2)
		}
	}).andExport(func(acc interface{}) interface{} {
		return exportFinishFn(op0.getExportFn()(acc.(Tuple3).f0), op1.getExportFn()(acc.(Tuple3).f1), op1.getExportFn()(acc.(Tuple3).f2))
	}).andFinish(func(acc interface{}) interface{} {
		return exportFinishFn(op0.getExportFn()(acc.(Tuple3).f0), op1.getExportFn()(acc.(Tuple3).f1), op1.getExportFn()(acc.(Tuple3).f2))
	})

}

func NewAggregateOperation3Impl(createFn GetFn, combineFn BiAcceptFn, deductFn BiAcceptFn, exportFn ApplyFn, finishFn ApplyFn, accumulateFns ...BiAcceptFn) *AggregateOperation3Impl {
	a := new(AggregateOperation3Impl)
	a.AggregateOperationImpl = new(AggregateOperationImpl)
	a.accumulateFns = append(a.accumulateFns, accumulateFns...)
	a.createFn = createFn
	a.combineFn = combineFn
	a.deductFn = deductFn
	a.exportFn = exportFn
	a.finishFn = finishFn
	return a
}

func (a *AggregateOperation3Impl) andThen(thenFn ApplyFn) AggregateOperation3 {
	return NewAggregateOperation3Impl(a.createFn,
		a.combineFn, a.deductFn, func(t interface{}) interface{} {
			return thenFn(a.exportFn(t))
		}, func(t interface{}) interface{} {
			return thenFn(a.finishFn(t))
		}, a.accumulateFns...)
}

func (a *AggregateOperation3Impl) accumulateFn0() BiAcceptFn {
	return a.accumulateFn(0)
}

func (a *AggregateOperation3Impl) accumulateFn1() BiAcceptFn {
	return a.accumulateFn(1)
}

func (a *AggregateOperation3Impl) accumulateFn2() BiAcceptFn {
	return a.accumulateFn(2)
}

// AggregateOperationBuilder a builder object that can be used to construct the definition of an aggregate operation in a step-by-step manner
type AggregateOperationBuilder struct {
	createFn GetFn
}

func NewAggregateOperationBuilder(createFn GetFn) *AggregateOperationBuilder {
	return &AggregateOperationBuilder{createFn: createFn}
}

// andAccumulate register the AggregateOperation1 accumulate primitive
func (b *AggregateOperationBuilder) andAccumulate(accumulateFn BiAcceptFn) *Arity1 {
	return NewArity1(b.createFn, accumulateFn)
}

func (b *AggregateOperationBuilder) andAccumulate0(accumulateFn0 BiAcceptFn) *Arity1 {
	return NewArity1(b.createFn, accumulateFn0)
}

// varArity selects the variable-arity variant for this aggregate operation builder
func (b *AggregateOperationBuilder) varArity() *VarArity {
	return NewVarArity(b.createFn)
}

type Arity1 struct {
	createFn      GetFn
	accumulateFn0 BiAcceptFn
	combineFn     BiAcceptFn
	deductFn      BiAcceptFn
	exportFn      ApplyFn
}

func NewArity1(createFn GetFn, accumulateFn0 BiAcceptFn) *Arity1 {
	return &Arity1{createFn: createFn, accumulateFn0: accumulateFn0}
}

func (a *Arity1) andAccumulate1(accumulateFn1 BiAcceptFn) *Arity2 {
	return NewArity2(a, accumulateFn1)
}

func (a *Arity1) andCombine(combineFn BiAcceptFn) *Arity1 {
	a.combineFn = combineFn
	return a
}

func (a *Arity1) andDeduct(deductFn BiAcceptFn) *Arity1 {
	a.deductFn = deductFn
	return a
}

func (a *Arity1) andExport(exportFn ApplyFn) *Arity1 {
	a.exportFn = exportFn
	return a
}

func (a *Arity1) andFinish(finishFn ApplyFn) AggregateOperation1 {
	return NewAggregateOperation1Impl(a.createFn, a.accumulateFn0, a.combineFn, a.deductFn, a.exportFn, finishFn)
}

func (a *Arity1) andExportFinish(exportFinishFn ApplyFn) AggregateOperation1 {
	return NewAggregateOperation1Impl(a.createFn, a.accumulateFn0, a.combineFn, a.deductFn, exportFinishFn, exportFinishFn)
}

type Arity2 struct {
	createFn      GetFn
	accumulateFn0 BiAcceptFn
	accumulateFn1 BiAcceptFn
	combineFn     BiAcceptFn
	deductFn      BiAcceptFn
	exportFn      ApplyFn
}

func NewArity2(step1 *Arity1, accumulateFn1 BiAcceptFn) *Arity2 {
	return &Arity2{accumulateFn1: accumulateFn1, createFn: step1.createFn, accumulateFn0: step1.accumulateFn0}
}

func (a *Arity2) andAccumulate2(accumulateFn2 BiAcceptFn) *Arity3 {
	return NewArity3(a, accumulateFn2)
}

func (a *Arity2) andCombine(combineFn BiAcceptFn) *Arity2 {
	a.combineFn = combineFn
	return a
}

func (a *Arity2) andDeduct(deductFn BiAcceptFn) *Arity2 {
	a.deductFn = deductFn
	return a
}

func (a *Arity2) andExport(exportFn ApplyFn) *Arity2 {
	a.exportFn = exportFn
	return a
}

func (a *Arity2) andFinish(finishFn ApplyFn) AggregateOperation2 {
	return NewAggregateOperation2Impl(a.createFn, a.combineFn, a.deductFn, a.exportFn, finishFn, a.accumulateFn0, a.accumulateFn1)
}

func (a *Arity2) andExportFinish(exportFinishFn ApplyFn) AggregateOperation2 {
	return NewAggregateOperation2Impl(a.createFn, a.combineFn, a.deductFn, exportFinishFn, exportFinishFn, a.accumulateFn0, a.accumulateFn1)
}

type Arity3 struct {
	createFn      GetFn
	accumulateFn0 BiAcceptFn
	accumulateFn1 BiAcceptFn
	accumulateFn2 BiAcceptFn
	combineFn     BiAcceptFn
	deductFn      BiAcceptFn
	exportFn      ApplyFn
}

func NewArity3(step2 *Arity2, accumulateFn2 BiAcceptFn) *Arity3 {
	return &Arity3{accumulateFn2: accumulateFn2, createFn: step2.createFn, accumulateFn0: step2.accumulateFn0, accumulateFn1: step2.accumulateFn1}
}

func (a *Arity3) andCombine(combineFn BiAcceptFn) *Arity3 {
	a.combineFn = combineFn
	return a
}

func (a *Arity3) andDeduct(deductFn BiAcceptFn) *Arity3 {
	a.deductFn = deductFn
	return a
}

func (a *Arity3) andExport(exportFn ApplyFn) *Arity3 {
	a.exportFn = exportFn
	return a
}

func (a *Arity3) andFinish(finishFn ApplyFn) AggregateOperation3 {
	return NewAggregateOperation3Impl(a.createFn, a.combineFn, a.deductFn, a.exportFn, finishFn, a.accumulateFn0, a.accumulateFn1, a.accumulateFn2)
}

func (a *Arity3) andExportFinish(exportFinishFn ApplyFn) AggregateOperation3 {
	return NewAggregateOperation3Impl(a.createFn, a.combineFn, a.deductFn, exportFinishFn, exportFinishFn, a.accumulateFn0, a.accumulateFn1, a.accumulateFn2)
}

// VarArity the variable-arity variant of aggregate operation builder .
// accept any number of accumulate primitives and associates them with Tags
type VarArity struct {
	createFn           GetFn
	accumulateFnsByTag map[int]BiAcceptFn
	combineFn          BiAcceptFn
	deductFn           BiAcceptFn
	exportFn           ApplyFn
}

func NewVarArity(createFn GetFn) *VarArity {
	return &VarArity{createFn: createFn, accumulateFnsByTag: make(map[int]BiAcceptFn)}
}

func NewVarArityWithAccumulateFn(createFn GetFn, tag Tag, accumulateFn BiAcceptFn) *VarArity {
	a := NewVarArity(createFn)
	a.accumulateFnsByTag[tag.index] = accumulateFn
	return a
}

func (a *VarArity) andCombine(combineFn BiAcceptFn) *VarArity {
	a.combineFn = combineFn
	return a
}

func (a *VarArity) andDeduct(deductFn BiAcceptFn) *VarArity {
	a.deductFn = deductFn
	return a
}

func (a *VarArity) andExport(exportFn ApplyFn) *VarArity {
	a.exportFn = exportFn
	return a
}

func (a *VarArity) andFinish(finishFn ApplyFn) AggregateOperation {
	return NewAggregateOperationImpl(a.createFn, a.packAccumulateFns(), a.combineFn, a.deductFn, a.exportFn, finishFn)
}

func (a *VarArity) andExportFinish(exportFinishFn ApplyFn) AggregateOperation {
	return NewAggregateOperationImpl(a.createFn, a.packAccumulateFns(), a.combineFn, a.deductFn, exportFinishFn, exportFinishFn)
}

func (a *VarArity) packAccumulateFns() []BiAcceptFn {
	var fns []BiAcceptFn
	for _, fn := range a.accumulateFnsByTag {
		if fn != nil {
			fns = append(fns, fn)
		}
	}
	return fns
}

// counting returns an aggregate operation that counts the items it observes .
// the result is of type long
func counting() AggregateOperation1 {
	return NewAggregateOperationBuilder(func() interface{} {
		return NewLongAccumulator()
	}).
		andAccumulate(func(t, u interface{}) {
			t.(*LongAccumulator).addAllowingOverflow(1)
		}).
		andCombine(func(t, u interface{}) {
			t.(*LongAccumulator).addAllowingOverflowWithAnother(u.(*LongAccumulator))
		}).
		andDeduct(func(t, u interface{}) {
			t.(*LongAccumulator).subtractAllowingOverflowWithAnother(u.(*LongAccumulator))
		}).
		andExportFinish(func(t interface{}) interface{} {
			return t.(*LongAccumulator).get()
		})
}

// summingLong return an aggregate operation that computes the sum of the long values
// it obtains by applying getLongValueFn to each item
func summingLong(getLongValueFn ApplyAsLongFn) AggregateOperation1 {
	return NewAggregateOperationBuilder(func() interface{} {
		return NewLongAccumulator()
	}).
		andAccumulate(func(t, u interface{}) {
			t.(*LongAccumulator).addAllowingOverflow(getLongValueFn(u))
		}).
		andCombine(func(t, u interface{}) {
			t.(*LongAccumulator).addAllowingOverflowWithAnother(u.(*LongAccumulator))
		}).
		andDeduct(func(t, u interface{}) {
			t.(*LongAccumulator).subtractAllowingOverflowWithAnother(u.(*LongAccumulator))
		}).
		andExportFinish(func(t interface{}) interface{} {
			return t.(*LongAccumulator).get()
		})
}

// summingDouble return an aggregate operation that computes the sum of the float values
// it obtains by applying getLongValueFn to each item
func summingDouble(getDoubleValueFn ApplyAsLongFn) AggregateOperation1 {
	return NewAggregateOperationBuilder(func() interface{} {
		return NewLongAccumulator()
	}).
		andAccumulate(func(t, u interface{}) {
			t.(*LongAccumulator).addAllowingOverflow(getDoubleValueFn(u))
		}).
		andCombine(func(t, u interface{}) {
			t.(*LongAccumulator).addAllowingOverflowWithAnother(u.(*LongAccumulator))
		}).
		andDeduct(func(t, u interface{}) {
			t.(*LongAccumulator).subtractAllowingOverflowWithAnother(u.(*LongAccumulator))
		}).
		andExportFinish(func(t interface{}) interface{} {
			return t.(*LongAccumulator).get()
		})
}
