package stream_processing

var (
	TAG_0 = NewTag(0)
	TAG_1 = NewTag(1)
	TAG_2 = NewTag(2)
)

// Tag object useful as a key in heterogeneous maps. carries static type information in its type parameter
type Tag struct {
	index int
}

func NewTag(index int) Tag {
	return Tag{index: index}
}

// Tuple2 an immutable 2-tuple (pair) of statically typed fields
type Tuple2 struct {
	f0 interface{}
	f1 interface{}
}

// NewTuple2 return a new tuple with supplied values
func NewTuple2(f0 interface{}, f1 interface{}) Tuple2 {
	return Tuple2{f0: f0, f1: f1}
}

// Tuple3 an immutable 3-tuple (triple) of statically typed fields
type Tuple3 struct {
	f0 interface{}
	f1 interface{}
	f2 interface{}
}

// NewTuple3 return a new triple with supplied values
func NewTuple3(f0, f1, f2 interface{}) Tuple3 {
	return Tuple3{f0: f0, f1: f1, f2: f2}
}
