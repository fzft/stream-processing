package stream_processing

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
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

	eventTimeMapper := NewEventTimeMapper(NewEventTimePolicyNoWrapping(longValueFunc{}, newLimitingLagSupplier(et.Lag), 5, 1, 0))
	eventTimeMapper.addPartitions(0, 2)

	// all partitions are active initially
	et.assertTraverser(t, eventTimeMapper.flatMapEvent(ns(1), nil, 0, Min_Value))
	// now idle timeout passed for all partitions, IDLE_MESSAGE should be emitted
	et.assertTraverser(t, eventTimeMapper.flatMapEvent(ns(5), nil, 0, Min_Value), NewWatermark(Max_Value))
	// still all partitions are idle, but IDlE_MESSAGE should not be emitted for the second time
	et.assertTraverser(t, eventTimeMapper.flatMapEvent(ns(5), nil, 0, Min_Value))
	// now we observe event on partition0, watermark should be immediately forwarded because the other queue is idle
	et.assertTraverser(t, eventTimeMapper.flatMapEvent(ns(5), int64(100), 0, Min_Value), NewWatermark(100-et.Lag), int64(100))
	// observe another event on the same partition. No WM is emitted because the event is older
	et.assertTraverser(t, eventTimeMapper.flatMapEvent(ns(5), int64(90), 0, Min_Value), int64(90))
	et.assertTraverser(t, eventTimeMapper.flatMapEvent(ns(5), int64(101), 0, Min_Value), NewWatermark(101-et.Lag), int64(101))

}

func TestEventTime_disabledIdleTimeout(t *testing.T) {
	teardownTest, et := EventTimeTestSetup(t)
	defer teardownTest(t)

	eventTimeMapper := NewEventTimeMapper(NewEventTimePolicyNoWrapping(longValueFunc{}, newLimitingLagSupplier(et.Lag), 0, 1, 0))
	eventTimeMapper.addPartitions(time.Now().UnixNano(), 2)

	// all partitions are active initially
	et.assertTraverser(t, eventTimeMapper.flatMapIdle())
	// now idle timeout passed for all partitions, IDLE_MESSAGE should be emitted
	et.assertTraverser(t, eventTimeMapper.flatMapEvent(time.Now().UnixNano(), int64(10), 0, Min_Value), int64(10))
	et.assertTraverser(t, eventTimeMapper.flatMapEvent(time.Now().UnixNano(), int64(11), 0, Min_Value), int64(11))
	// now have some events in the other partition, wms will be output
	et.assertTraverser(t, eventTimeMapper.flatMapEvent(time.Now().UnixNano(), int64(10), 1, Min_Value), NewWatermark(10-et.Lag), int64(10))
	et.assertTraverser(t, eventTimeMapper.flatMapEvent(time.Now().UnixNano(), int64(11), 1, Min_Value), NewWatermark(11-et.Lag), int64(11))
	// now partition1 will get ahead of partition0 -> no WM
	et.assertTraverser(t, eventTimeMapper.flatMapEvent(time.Now().UnixNano(), int64(12), 1, Min_Value), int64(12))
	// another event in partition0, we'll get the wm
	et.assertTraverser(t, eventTimeMapper.flatMapEvent(time.Now().UnixNano(), int64(13), 0, Min_Value), NewWatermark(12-et.Lag), int64(13))

}

func TestEventTime_zeroPartitions(t *testing.T) {
	teardownTest, et := EventTimeTestSetup(t)
	defer teardownTest(t)

	eventTimeMapper := NewEventTimeMapper(NewEventTimePolicyNoWrapping(longValueFunc{}, newLimitingLagSupplier(et.Lag), 0, 1, 0))

	// all partitions are active initially
	et.assertTraverser(t, eventTimeMapper.flatMapIdle(), NewWatermark(Max_Value))
	et.assertTraverser(t, eventTimeMapper.flatMapIdle())

	// after adding a partition and observing an event , WM should be emit
	eventTimeMapper.addPartitions(time.Now().UnixNano(), 1)
	et.assertTraverser(t, eventTimeMapper.flatMapIdle())
	et.assertTraverser(t, eventTimeMapper.flatMapEvent(time.Now().UnixNano(), int64(10), 0, Min_Value), NewWatermark(10-et.Lag), int64(10))

}

func TestEventTime_when_idle_event_idle_then_twoIdleMessagesSent(t *testing.T) {
	teardownTest, et := EventTimeTestSetup(t)
	defer teardownTest(t)

	eventTimeMapper := NewEventTimeMapper(NewEventTimePolicyNoWrapping(longValueFunc{}, newLimitingLagSupplier(et.Lag), 10, 1, 0))
	eventTimeMapper.addPartitions(time.Now().UnixNano(), 1)
	et.assertTraverser(t, eventTimeMapper.flatMapEvent(ns(0), int64(10), 0, Min_Value), NewWatermark(10-et.Lag), int64(10))

	// when become idle
	et.assertTraverser(t, eventTimeMapper.flatMapEvent(ns(10), nil, 0, Min_Value), NewWatermark(Max_Value))
	// when another event, but no wm
	et.assertTraverser(t, eventTimeMapper.flatMapEvent(ns(10), int64(10), 0, Min_Value), int64(10))
	// become idle again
	et.assertTraverser(t, eventTimeMapper.flatMapEvent(ns(10), nil, 0, Min_Value))
	et.assertTraverser(t, eventTimeMapper.flatMapEvent(ns(20), nil, 0, Min_Value), NewWatermark(Max_Value))
}

func TestEventTime_when_eventInOneOfTwoPartitions_then_wmAndIdleMessageForwardedAfterTimeout(t *testing.T) {
	teardownTest, et := EventTimeTestSetup(t)
	defer teardownTest(t)

	eventTimeMapper := NewEventTimeMapper(NewEventTimePolicyNoWrapping(longValueFunc{}, newLimitingLagSupplier(et.Lag), 10, 1, 0))
	eventTimeMapper.addPartitions(ns(0), 2)

	et.assertTraverser(t, eventTimeMapper.flatMapEvent(ns(0), int64(10), 0, Min_Value), int64(10))

	et.assertTraverser(t, eventTimeMapper.flatMapEvent(ns(10), nil, 0, Min_Value))
}

func TestEventTime_when_noTimestampFn_then_useNativeTime(t *testing.T) {
	teardownTest, et := EventTimeTestSetup(t)
	defer teardownTest(t)

	eventTimeMapper := NewEventTimeMapper(NewEventTimePolicyNoWrapping(nil, newLimitingLagSupplier(et.Lag), 5, 1, 0))
	eventTimeMapper.addPartitions(0, 1)

	et.assertTraverser(t, eventTimeMapper.flatMapEvent(ns(1), int64(10), 0, 11), NewWatermark(11-et.Lag), int64(10))
	et.assertTraverser(t, eventTimeMapper.flatMapEvent(ns(1), int64(11), 0, 12), NewWatermark(12-et.Lag), int64(11))
}

func TestEventTime_when_throttlingToMaxFrame_then_noWatermarksOutput(t *testing.T) {
	teardownTest, et := EventTimeTestSetup(t)
	defer teardownTest(t)

	eventTimeMapper := NewEventTimeMapper(NewEventTimePolicyNoWrapping(longValueFunc{}, newLimitingLagSupplier(et.Lag), 5, 0, 0))
	eventTimeMapper.addPartitions(0, 1)

	et.assertTraverser(t, eventTimeMapper.flatMapEvent(ns(1), int64(-10), 0, 11), int64(-10))
	et.assertTraverser(t, eventTimeMapper.flatMapEvent(ns(1), int64(10), 0, 12), int64(10))
}

func TestEventTime_when_restoredState_then_wmDoesNotGoBack(t *testing.T) {
	teardownTest, et := EventTimeTestSetup(t)
	defer teardownTest(t)

	eventTimeMapper := NewEventTimeMapper(NewEventTimePolicyNoWrapping(longValueFunc{}, newLimitingLagSupplier(0), 5, 1, 0))
	eventTimeMapper.addPartitions(0, 1)
	eventTimeMapper.restoreWatermark(0, 10)

	et.assertTraverser(t, eventTimeMapper.flatMapEvent(ns(0), int64(9), 0, Min_Value), int64(9))
	et.assertTraverser(t, eventTimeMapper.flatMapEvent(ns(0), int64(10), 0, Min_Value), int64(10))
	et.assertTraverser(t, eventTimeMapper.flatMapEvent(ns(0), int64(11), 0, Min_Value), NewWatermark(11), int64(11))
}

func TestEventTime_when_twoActiveQueues_theLaggingOneRemoved_then_wmForwarded(t *testing.T) {
	teardownTest, et := EventTimeTestSetup(t)
	defer teardownTest(t)

	eventTimeMapper := NewEventTimeMapper(NewEventTimePolicyNoWrapping(longValueFunc{}, newLimitingLagSupplier(0), 5, 1, 0))
	eventTimeMapper.addPartitions(0, 2)

	et.assertTraverser(t, eventTimeMapper.flatMapEvent(ns(0), int64(10), 0, Min_Value), int64(10))
	et.assertTraverser(t, eventTimeMapper.removePartition(ns(0), 1), NewWatermark(10))
}


func ns(ms int64) int64 {
	return time.UnixMilli(ms).UnixNano()
}
