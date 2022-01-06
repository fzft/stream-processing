package stream_processing

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestAggregateOperation1_when_build_then_allPartsThere(t *testing.T) {

	createFn := func() interface{} {
		return NewLongAccumulator()
	}
	accFn0 := func(acc, item interface{}) {
		if a, ok := acc.(*LongAccumulator); ok {
			a.addAllowingOverflow(1)
		}
	}
	combineFn := func(t, u interface{}) {
		t.(*LongAccumulator).addAllowingOverflowWithAnother(u.(*LongAccumulator))
	}
	deductFn := func(t, u interface{}) {
		t.(*LongAccumulator).subtractAllowingOverflowWithAnother(u.(*LongAccumulator))
	}
	exportFn := func(t interface{}) interface{} {
		return int64(1)
	}
	finishFn := func(t interface{}) interface{} {
		return int64(2)
	}

	aggOp := NewAggregateOperationBuilder(createFn).
		andAccumulate(accFn0).
		andCombine(combineFn).
		andDeduct(deductFn).
		andExport(exportFn).
		andFinish(finishFn)

	assert.Same(t, createFn, aggOp.getCreateFn())
	assert.Same(t, accFn0, aggOp.accumulateFn(0))
	assert.Same(t, combineFn, aggOp.getCombineFn())
	assert.Same(t, deductFn, aggOp.getDeductFn())
	assert.Same(t, exportFn, aggOp.getExportFn())
	assert.Same(t, finishFn, aggOp.getFinishFn())

}

func TestAggregateOperation1_accumulate0_synonymFor_accumulate(t *testing.T) {

	createFn := func() interface{} {
		return NewLongAccumulator()
	}
	accFn := func(acc, item interface{}) {
		if a, ok := acc.(*LongAccumulator); ok {
			a.addAllowingOverflow(1)
		}
	}

	aggOp1 := NewAggregateOperationBuilder(createFn).
		andAccumulate(accFn).
		andExportFinish(func(t interface{}) interface{} {
			return t.(*LongAccumulator).get()
		})

	aggOp2 := NewAggregateOperationBuilder(createFn).
		andAccumulate(accFn).
		andExportFinish(func(t interface{}) interface{} {
			return t.(*LongAccumulator).get()
		})

	assert.Same(t, accFn, aggOp1.accumulateFn(0))
	assert.Same(t, accFn, aggOp2.accumulateFn(0))

}

func TestAggregateOperation1_when_withCombiningAccumulateFn_then_accumulateFnCombines(t *testing.T) {

	aggOp := NewAggregateOperationBuilder(func() interface{} {
		return NewLongAccumulator()
	}).
		andAccumulate(func(t, u interface{}) {
			t.(*LongAccumulator).addAllowingOverflow(1)
		}).andCombine(func(t, u interface{}) {
		t.(*LongAccumulator).addAllowingOverflowWithAnother(u.(*LongAccumulator))
	}).andExportFinish(func(t interface{}) interface{} {
		return t.(*LongAccumulator).get()
	})

	combiningAggrOp := aggOp.withCombiningAccumulateFn(func(t interface{}) interface{} {
		return t
	})
	accFn := combiningAggrOp.accumulateFn(0)
	partialAcc1 := combiningAggrOp.getCreateFn()().(*LongAccumulator)
	partialAcc2 := combiningAggrOp.getCreateFn()().(*LongAccumulator)
	combinedAcc := combiningAggrOp.getCreateFn()().(*LongAccumulator)

	partialAcc1.set(2)
	partialAcc2.set(3)
	accFn(combinedAcc, partialAcc1)
	accFn(combinedAcc, partialAcc2)

	assert.Equal(t, int64(5), combinedAcc.get())
}

func TestAggregateOperation1_when_andThen_then_exportAndFinishChanged(t *testing.T) {

	aggOp := summingLong(func(t interface{}) int64 {
		return t.(int64)
	})
	incAggrOp := aggOp.andThen(func(t interface{}) interface{} {
		return t.(int64) + 1
	})

	acc := incAggrOp.getCreateFn()().(*LongAccumulator)
	incAggrOp.accumulateFn0()(acc, int64(13))
	exported := incAggrOp.getExportFn()(acc).(int64)
	finished := incAggrOp.getFinishFn()(acc).(int64)
	assert.Equal(t, int64(14), exported)
	assert.Equal(t, int64(14), finished)
}

func TestAggregateOperation2_when_withCombiningAccumulateFn_then_accumulateFnCombines(t *testing.T) {

	aggOp := NewAggregateOperationBuilder(func() interface{} {
		return NewLongAccumulator()
	}).
		andAccumulate0(func(acc, item interface{}) {
			acc.(*LongAccumulator).addAllowingOverflow(1)
		}).andAccumulate1(func(acc, item interface{}) {
		acc.(*LongAccumulator).addAllowingOverflow(10)
	}).
		andCombine(func(t, u interface{}) {
			t.(*LongAccumulator).addAllowingOverflowWithAnother(u.(*LongAccumulator))
		}).andExportFinish(func(t interface{}) interface{} {
		return t.(*LongAccumulator).get()
	})

	combineAggrOp := aggOp.withCombiningAccumulateFn(func(t interface{}) interface{} {
		return t
	})

	accFn := combineAggrOp.accumulateFn(0)
	partialAcc1 := combineAggrOp.getCreateFn()().(*LongAccumulator)
	partialAcc2 := combineAggrOp.getCreateFn()().(*LongAccumulator)
	combinedAcc := combineAggrOp.getCreateFn()().(*LongAccumulator)

	partialAcc1.set(2)
	partialAcc2.set(3)
	accFn(combinedAcc, partialAcc1)
	accFn(combinedAcc, partialAcc2)

	assert.Equal(t, int64(5), combinedAcc.get())

}

func TestAggregateOperation2_when_andThen_then_exportAndFinishChanged(t *testing.T) {

	aggrOp := aggregateOperation2(
		summingLong(func(t interface{}) int64 {
			return t.(int64)
		}), summingLong(func(t interface{}) int64 {
			return t.(int64)
		}),
		func(r0, r1 interface{}) interface{} {
			return r1
		})

	incAggrOp := aggrOp.andThen(func(t interface{}) interface{} {
		return t.(int64) + 1
	})

	acc := incAggrOp.getCreateFn()().(Tuple2)
	incAggrOp.accumulateFn1()(acc, int64(13))
	exported := incAggrOp.getExportFn()(acc)
	finished := incAggrOp.getFinishFn()(acc)
	assert.Equal(t, int64(14), exported)
	assert.Equal(t, int64(14), finished)

}

func TestAggregateOperation3_when_withCombiningAccumulateFn_then_accumulateFnCombines(t *testing.T) {
	aggrOp := NewAggregateOperationBuilder(func() interface{} {
		return NewLongAccumulator()
	}).
		andAccumulate0(func(acc, item interface{}) {
			acc.(*LongAccumulator).addAllowingOverflow(1)
		}).andAccumulate1(func(acc, item interface{}) {
		acc.(*LongAccumulator).addAllowingOverflow(10)
	}).andAccumulate2(func(acc, item interface{}) {
		acc.(*LongAccumulator).addAllowingOverflow(100)
	}).
		andCombine(func(t, u interface{}) {
			t.(*LongAccumulator).addAllowingOverflowWithAnother(u.(*LongAccumulator))
		}).andExportFinish(func(t interface{}) interface{} {
		return t.(*LongAccumulator).get()
	})
	combiningAggrOp := aggrOp.withCombiningAccumulateFn(func(t interface{}) interface{} {
		return t
	})
	accFn := combiningAggrOp.accumulateFn(0)

	partialAcc1 := combiningAggrOp.getCreateFn()().(*LongAccumulator)
	partialAcc2 := combiningAggrOp.getCreateFn()().(*LongAccumulator)
	combinedAcc := combiningAggrOp.getCreateFn()().(*LongAccumulator)

	partialAcc1.set(2)
	partialAcc2.set(3)
	accFn(combinedAcc, partialAcc1)
	accFn(combinedAcc, partialAcc2)

	assert.Equal(t, int64(5), combinedAcc.get())

}

func TestAggregateOperation3_when_andThen_then_exportAndFinishChanged(t *testing.T) {

	aggrOp := aggregateOperation3(
		summingLong(func(t interface{}) int64 {
			return t.(int64)
		}),
		summingLong(func(t interface{}) int64 {
			return t.(int64)
		}),
		summingLong(func(t interface{}) int64 {
			return t.(int64)
		}),
		func(r0, r1, r2 interface{}) interface{} {
			return r2
		})

	incAggrOp := aggrOp.andThen(func(t interface{}) interface{} {
		return t.(int64) + 1
	})

	acc := incAggrOp.getCreateFn()().(Tuple3)
	incAggrOp.accumulateFn2()(acc, int64(13))
	exported := incAggrOp.getExportFn()(acc)
	finished := incAggrOp.getFinishFn()(acc)
	assert.Equal(t, int64(14), exported)
	assert.Equal(t, int64(14), finished)
}
