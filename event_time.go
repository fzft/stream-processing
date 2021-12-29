package stream_processing

import "time"

// EventTimePolicy a holder of functions and parameters needs to handle event time and the associated watermark.
// this class should be used EventTimeMapper when implementing a source processor
type EventTimePolicy struct {
	DEFAULT_IDLE_TIMEOUT int64
	// timestampFn extracts the timestamp from an event in the stream
	timestampFn ToLongFunction

	// newWmPolicyFn a factory of watermark policy objects
	newWmPolicyFn SupplierEx

	// wrapFn a function that transforms a given event and its timestamp into the item to emit from the processor
	wrapFn ObjLongBiFunction

	// idleTimeoutMillis ...
	idleTimeoutMillis int64

	// watermarkThrottlingFrameSize ...
	watermarkThrottlingFrameSize int64

	// watermarkThrottlingFrameOffset
	watermarkThrottlingFrameOffset int64
}

func NewEventTimePolicy(timestampFn ToLongFunction, newWmPolicyFn SupplierEx, wrapFn ObjLongBiFunction, idleTimeoutMillis int64, watermarkThrottlingFrameSize int64, watermarkThrottlingFrameOffset int64) EventTimePolicy {
	return EventTimePolicy{timestampFn: timestampFn, newWmPolicyFn: newWmPolicyFn, wrapFn: wrapFn, idleTimeoutMillis: idleTimeoutMillis, watermarkThrottlingFrameSize: watermarkThrottlingFrameSize, watermarkThrottlingFrameOffset: watermarkThrottlingFrameOffset}
}

func NewEventTimePolicyNoWrapping(timestampFn ToLongFunction, newWmPolicyFn SupplierEx, idleTimeoutMillis int64, watermarkThrottlingFrameSize int64, watermarkThrottlingFrameOffset int64) EventTimePolicy {
	return EventTimePolicy{timestampFn: timestampFn, newWmPolicyFn: newWmPolicyFn, wrapFn: noWrapping{}, idleTimeoutMillis: idleTimeoutMillis, watermarkThrottlingFrameSize: watermarkThrottlingFrameSize, watermarkThrottlingFrameOffset: watermarkThrottlingFrameOffset}
}

// EventTimeMapper a utility that helps a source emit events according to a given EventTimePolicy. Generally this struct should be used if a source needs emit Watermark
type EventTimeMapper struct {
	NO_NATIVE_TIME int64
	EMPTY_LONGS    []int64

	idleTimeoutNanos int64
	timestampFn      ToLongFunction
	newWmPolicyFn    SupplierEx
	wrapFn           ObjLongBiFunction

	watermarkThrottlingFrame *SlidingWindowPolicy
	wmPolicies               []WatermarkPolicy
	traverser                *AppendableTraverser
	watermarks               []int64
	markIdleAt               []int64
	lastEmittedWm            int64
	topObservedWm            int64
	allAreIdle               bool
}

// NewEventTimeMapper ...
func NewEventTimeMapper(eventTimePolicy EventTimePolicy) *EventTimeMapper {
	m := new(EventTimeMapper)
	m.idleTimeoutNanos = time.UnixMilli(eventTimePolicy.idleTimeoutMillis).UnixNano()
	m.timestampFn = eventTimePolicy.timestampFn
	m.wrapFn = eventTimePolicy.wrapFn
	m.newWmPolicyFn = eventTimePolicy.newWmPolicyFn
	m.traverser = NewAppendableTraverser()
	if eventTimePolicy.watermarkThrottlingFrameSize != 0 {
		m.watermarkThrottlingFrame = NewTumblingWithPolicy(eventTimePolicy.watermarkThrottlingFrameSize).withOffset(eventTimePolicy.watermarkThrottlingFrameOffset)
	}
	return m
}

// flatMapIdle call this method when there is no event to emit
func (m *EventTimeMapper) flatMapIdle() Traverser {
	return m.flatMapEvent(time.Now().UnixNano(), nil, -1, Min_Value)
}

// flatMapEvent flat-map the given event by possibly prepending it with a watermark
func (m *EventTimeMapper) flatMapEvent(now int64, event interface{}, partitionIndex int, nativeEventTime int64) Traverser {
	if event == nil {
		m.handleNoEventInternal(now, Max_Value)
		return m.traverser
	}

	var eventTime int64
	if m.timestampFn != nil {
		eventTime = m.timestampFn.applyAsLong(event)
	} else {
		eventTime = nativeEventTime
		if eventTime == m.NO_NATIVE_TIME {
			panic("Neither timestampFn nor nativeEventTime specified")
		}
	}
	m.handleEventInternal(now, partitionIndex, eventTime)
	return m.traverser.append(m.wrapFn.apply(event, eventTime))
}

func (m *EventTimeMapper) handleEventInternal(now int64, partitionIndex int, eventTime int64) {
	m.wmPolicies[partitionIndex].reportEvent(eventTime)
	m.markIdleAt[partitionIndex] = now + m.idleTimeoutNanos
	m.allAreIdle = false
	m.handleNoEventInternal(now, eventTime)
}

func (m *EventTimeMapper) handleNoEventInternal(now int64, maxWmValue int64) {
	min := Max_Value
	for i := 0; i < len(m.watermarks); i++ {
		if m.idleTimeoutNanos > 0 && m.markIdleAt[i] <= now {
			continue
		}
		// the new watermark must not be less than the previous watermark and not more than maxWmValue
		m.watermarks[i] = Max64(m.watermarks[i], Min64(m.wmPolicies[i].getCurrentWatermark(), maxWmValue))
		m.topObservedWm = Max64(m.topObservedWm, m.watermarks[i])
		min = Min64(min, m.watermarks[i])
	}

	if min == Max_Value {
		if m.allAreIdle {
			return
		}

		// we've just become fully idle, forward the top now, if needed
		min = m.topObservedWm
		m.allAreIdle = true
	} else {
		m.allAreIdle = false
	}

	if min > m.lastEmittedWm {
		var newWm int64
		if m.watermarkThrottlingFrame != nil {
			newWm = m.watermarkThrottlingFrame.floorFrameTs(min)
		} else {
			newWm = Min_Value
		}
		if newWm > m.lastEmittedWm {
			m.traverser.append(NewWatermark(newWm))
			m.lastEmittedWm = newWm
		}
	}

	if m.allAreIdle {
		m.traverser.append(NewWatermark(Max_Value))
	}
}

// addPartitions package-visible for tests
// added partitions will be initially considered active and having watermark value equal to the last emitted watermark
func (m *EventTimeMapper) addPartitions(now int64, addedCount int) {
	oldPartitionCount := len(m.wmPolicies)
	newPartitionCount := oldPartitionCount + addedCount

	for i := oldPartitionCount; i < newPartitionCount; i++ {
		m.wmPolicies = append(m.wmPolicies, m.newWmPolicyFn.get().(WatermarkPolicy))
		m.watermarks = append(m.watermarks, Min_Value)
		m.markIdleAt = append(m.markIdleAt, now+m.idleTimeoutNanos)
	}
}

// removePartition removes a partition that will no longer have events. If we were waiting for a watermark from it, the returned traverser might contain a watermark to emit
// can not use arrayRemove cause no Generic !!!
func (m *EventTimeMapper) removePartition(now int64, i int) Traverser {
	copy(m.wmPolicies[i:], m.wmPolicies[i+1:])
	m.wmPolicies[len(m.wmPolicies)-1] = nil
	m.wmPolicies = m.wmPolicies[:len(m.wmPolicies)-1]

	copy(m.watermarks[i:], m.watermarks[i+1:])
	m.watermarks[len(m.watermarks)-1] = 0
	m.watermarks = m.watermarks[:len(m.watermarks)-1]

	copy(m.markIdleAt[i:], m.markIdleAt[i+1:])
	m.markIdleAt[len(m.markIdleAt)-1] = 0
	m.markIdleAt = m.markIdleAt[:len(m.markIdleAt)-1]

	m.handleNoEventInternal(now, Max_Value)
	return m.traverser
}

// partitionCount return the current parition count
func (m *EventTimeMapper) partitionCount() int {
	return len(m.wmPolicies)
}

// restoreWatermark watermark value from state snapshot
func (m *EventTimeMapper) restoreWatermark(partitionIndex int, wm int64) {
	m.watermarks[partitionIndex] = wm
	m.lastEmittedWm = Max_Value
	for _, watermark := range m.watermarks {
		m.lastEmittedWm = Min64(watermark, m.lastEmittedWm)
	}
}

func arrayRemove(slices []interface{}, index int) []interface{} {
	slices[index+1] = slices[len(slices)-1]
	slices[len(slices)-1] = nil
	slices = slices[:len(slices)-1]
	return slices
}
