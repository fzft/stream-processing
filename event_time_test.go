package stream_processing

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

type EventTimeTest struct {
	Lag int64
}

func (e EventTimeTest) assertTraverser(t *testing.T, actual Traverser, expected ...interface{}) {
	for _, elem := range expected {
		assert.Equal(t, elem, actual.next())
	}
}

func EventTimeTestSetup(tb testing.TB) (func(tb testing.TB), EventTimeTest) {
	et := EventTimeTest{}
	et.Lag = 3
	return func(tb testing.TB) {
		tb.Log("EventTimeTestSetup teardown")
	}, et
}

func TestEventTime_smoke(t *testing.T) {
	teardownTest, et := EventTimeTestSetup(t)
	defer teardownTest(t)

	eventTimeMapper := NewEventTimeMapper(NewEventTimePolicyNoWrapping(longValueFunc{}, newLimitingLagSupplier(et.Lag), 1, 0, 5))
	eventTimeMapper.addPartitions(0, 2)

	// all partitions are active initially
	et.assertTraverser(t, eventTimeMapper.flatMapEvent(ns(1), nil, 0, Min_Value))

	// now idle timeout passed for all partitions, IDLE_MESSAGE should be emitted
	et.assertTraverser(t, eventTimeMapper.flatMapEvent(ns(5), nil, 0, Min_Value), NewWatermark(Max_Value))



}

func ns(ms int64) int64 {
	return 1000000 * ms
}
