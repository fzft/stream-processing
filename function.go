package stream_processing

type ApplyFn func(t interface{}) interface{}

type AcceptFn func(t interface{})

type BiAcceptFn func(t, u interface{})

type TestFn func(t interface{}) bool

type GetFn func() interface{}

type BiApplyFn func(t, u interface{}) interface{}

type ApplyAsLongFn func(t interface{}) int64

type BiTest func(t, u interface{}) bool

type ObjLongBiApplyFn func(t interface{}, u int64) interface{}

type TriApplyFn func(t0, t1, t2 interface{}) interface{}

type TriTestFn func(t0, t1, t2 interface{}) bool
