package stream_processing

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

type WatermarkTest struct {
	LAG int64
	p   WatermarkPolicy
}

func WatermarkTestSetup(tb testing.TB) (func(tb testing.TB), WatermarkTest) {
	wt := WatermarkTest{}
	wt.LAG = 10
	wt.p = newLimitingLag(wt.LAG)

	return func(tb testing.TB) {
		tb.Log("WatermarkTestSetup teardown")
	}, wt
}

func TestWatermark_when_outOfOrderEvents_then_monotonicWm(t *testing.T) {
	teardownTest, wt := WatermarkTestSetup(t)
	defer teardownTest(t)

	for i := int64(0); i < 10; i++ {
		wt.p.reportEvent(i)
		assert.Equal(t, i-wt.LAG, wt.p.getCurrentWatermark())

		wt.p.reportEvent(i - 1)
		assert.Equal(t, i-wt.LAG, wt.p.getCurrentWatermark())
		wt.p.reportEvent(i - 2)
		assert.Equal(t, i-wt.LAG, wt.p.getCurrentWatermark())
	}
}

func TestWatermark_when_eventsStop_then_wmStops(t *testing.T) {
	teardownTest, wt := WatermarkTestSetup(t)
	defer teardownTest(t)

	// when an event and nothing more
	wt.p.reportEvent(wt.LAG)

	// then watermark stops
	for i := int64(0); i < 10; i++ {
		assert.Equal(t, int64(0), wt.p.getCurrentWatermark())
	}
}

func TestWatermark_when_noEventEver_then_minValue(t *testing.T) {
	teardownTest, wt := WatermarkTestSetup(t)
	defer teardownTest(t)

	// then watermark stops
	for i := int64(0); i < 10; i++ {
		assert.Equal(t, Min_Value, wt.p.getCurrentWatermark())
	}
}
