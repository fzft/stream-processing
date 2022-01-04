package stream_processing

// LongAccumulator mutable container of  int64 value
type LongAccumulator struct {
	value int64
}

func NewLongAccumulator() *LongAccumulator {
	return &LongAccumulator{}
}

func NewLongAccumulatorWithValue(value int64) *LongAccumulator {
	return &LongAccumulator{value: value}
}

func (a *LongAccumulator) get() int64 {
	return a.value
}

func (a *LongAccumulator) set(value int64) *LongAccumulator {
	a.value = value
	return a
}

func (a *LongAccumulator) addAllowingOverflow(value int64) *LongAccumulator {
	a.value += value
	return a
}

func (a *LongAccumulator) addAllowingOverflowWithAnother(that *LongAccumulator) *LongAccumulator {
	a.value += that.value
	return a
}
func (a *LongAccumulator) subtractAllowingOverflow(value int64) *LongAccumulator {
	a.value -= value
	return a
}

func (a *LongAccumulator) subtractAllowingOverflowWithAnother(that *LongAccumulator) *LongAccumulator {
	a.value -= that.value
	return a
}
