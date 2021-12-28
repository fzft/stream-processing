package stream_processing

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

type WindowTest struct {
	definition *SlidingWindowPolicy
}

func (w *WindowTest) assertFrameTs(t *testing.T, timestamp, expectedFloor, expectedHigher int64) {
	assert.Equal(t, expectedFloor, w.definition.floorFrameTs(timestamp))
	assert.Equal(t, expectedHigher, w.definition.higherFrameTs(timestamp))
}

func WindowTestSetup(tb testing.TB) (func(tb testing.TB), WindowTest) {
	wt := WindowTest{}
	return func(tb testing.TB) {
		tb.Log("WindowTestSetup teardown")
	}, wt
}

func TestSlidingWindowPolicy_when_noOffset(t *testing.T) {
	teardownTest, wt := WindowTestSetup(t)
	defer teardownTest(t)
	wt.definition = NewSlidingWindowPolicy(4, 0, 10)
	wt.assertFrameTs(t, -5, -8, -4)
	wt.assertFrameTs(t, -4, -4, 0)
	wt.assertFrameTs(t, -3, -4, 0)
	wt.assertFrameTs(t, -2, -4, 0)
	wt.assertFrameTs(t, -1, -4, 0)
	wt.assertFrameTs(t, 0, 0, 4)
	wt.assertFrameTs(t, 1, 0, 4)
	wt.assertFrameTs(t, 2, 0, 4)
	wt.assertFrameTs(t, 3, 0, 4)
	wt.assertFrameTs(t, 4, 4, 8)
	wt.assertFrameTs(t, 5, 4, 8)
	wt.assertFrameTs(t, 6, 4, 8)
	wt.assertFrameTs(t, 7, 4, 8)
	wt.assertFrameTs(t, 8, 8, 12)
}

func TestSlidingWindowPolicy_when_offset1(t *testing.T) {
	teardownTest, wt := WindowTestSetup(t)
	defer teardownTest(t)
	wt.definition = NewSlidingWindowPolicy(4, 1, 10)
	wt.assertFrameTs(t, -4, -7, -3)
	wt.assertFrameTs(t, -3, -3, 1)
	wt.assertFrameTs(t, -2, -3, 1)
	wt.assertFrameTs(t, -1, -3, 1)
	wt.assertFrameTs(t, 0, -3, 1)
	wt.assertFrameTs(t, 1, 1, 5)
	wt.assertFrameTs(t, 2, 1, 5)
	wt.assertFrameTs(t, 3, 1, 5)
	wt.assertFrameTs(t, 4, 1, 5)
	wt.assertFrameTs(t, 5, 5, 9)
	wt.assertFrameTs(t, 6, 5, 9)
	wt.assertFrameTs(t, 7, 5, 9)
	wt.assertFrameTs(t, 8, 5, 9)
	wt.assertFrameTs(t, 9, 9, 13)
}

func TestSlidingWindowPolicy_when_offset2(t *testing.T) {
	teardownTest, wt := WindowTestSetup(t)
	defer teardownTest(t)
	wt.definition = NewSlidingWindowPolicy(4, 2, 10)

	wt.assertFrameTs(t, -4, -6, -2)
	wt.assertFrameTs(t, -3, -6, -2)
	wt.assertFrameTs(t, -2, -2, 2)
	wt.assertFrameTs(t, -1, -2, 2)
	wt.assertFrameTs(t, 0, -2, 2)
	wt.assertFrameTs(t, 1, -2, 2)
	wt.assertFrameTs(t, 2, 2, 6)
	wt.assertFrameTs(t, 3, 2, 6)
	wt.assertFrameTs(t, 4, 2, 6)
	wt.assertFrameTs(t, 5, 2, 6)
	wt.assertFrameTs(t, 6, 6, 10)
	wt.assertFrameTs(t, 7, 6, 10)
	wt.assertFrameTs(t, 8, 6, 10)
	wt.assertFrameTs(t, 9, 6, 10)
}

func TestSlidingWindowPolicy_when_frameLength3(t *testing.T) {
	teardownTest, wt := WindowTestSetup(t)
	defer teardownTest(t)
	wt.definition = NewSlidingWindowPolicy(3, 0, 10)
	assert.Equal(t, Min_Value, wt.definition.floorFrameTs(Min_Value))
}

func TestSlidingWindowPolicy_when_floorOutOfRange_then_minValue(t *testing.T) {
	teardownTest, wt := WindowTestSetup(t)
	defer teardownTest(t)
	wt.definition = NewSlidingWindowPolicy(4, 3, 10)
	assert.Equal(t, Min_Value, wt.definition.floorFrameTs(Min_Value+2))
	assert.Equal(t, Max_Value, wt.definition.floorFrameTs(Max_Value))
}

func TestSlidingWindowPolicy_when_higherOutOfRange_then_maxValue(t *testing.T) {
	teardownTest, wt := WindowTestSetup(t)
	defer teardownTest(t)
	wt.definition = NewSlidingWindowPolicy(4, 2, 10)
	assert.Equal(t, Max_Value, wt.definition.higherFrameTs(Max_Value-1))
	assert.Equal(t, Min_Value+2, wt.definition.higherFrameTs(Min_Value))
}

func TestSlidingWindowPolicy_tumblingWindowDef(t *testing.T) {
	teardownTest, wt := WindowTestSetup(t)
	defer teardownTest(t)
	wt.definition = NewTumblingWithPolicy(123)
	assert.Equal(t, int64(123), wt.definition.frameSize)
	assert.Equal(t, int64(123), wt.definition.windowSize)
	assert.Equal(t, int64(0), wt.definition.frameOffset)
}

func TestSlidingWindowPolicy_toTumblingByFrame(t *testing.T) {
	teardownTest, wt := WindowTestSetup(t)
	defer teardownTest(t)
	wt.definition = NewSlidingWithPolicy(1000, 100)
	wt.definition = wt.definition.toTumblingByFrame()
	assert.Equal(t, int64(100), wt.definition.frameSize)
	assert.Equal(t, int64(100), wt.definition.windowSize)
}

func TestSlidingWindowPolicy_withOffset(t *testing.T) {
	teardownTest, wt := WindowTestSetup(t)
	defer teardownTest(t)
	wt.definition = NewSlidingWithPolicy(1000, 100)
	assert.Equal(t, int64(0), wt.definition.frameOffset)
	wt.definition = wt.definition.withOffset(10)
	assert.Equal(t, int64(10), wt.definition.frameOffset)
}

