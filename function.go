package stream_processing

type FunctionEx interface {
	// apply ...
	apply(t interface{}) interface{}
}

type PredicateEx interface {
	// test ...
	test() bool

	// alwaysTrue ...
	alwaysTrue() PredicateEx

	// alwaysFalse ...
	alwaysFalse() PredicateEx

	// isEqual ...
	isEqual(o interface{}) PredicateEx

	// and ...
	and(o interface{}) PredicateEx

	// negate ...
	negate(o interface{}) PredicateEx

	// or ...
	or(o interface{}) PredicateEx
}

// SupplierEx ...
type SupplierEx interface {

	// getEx ...
	getEx() interface{}
	// get ...
	get() interface{}
}

type BiFunctionEx interface {
	applyEx(t, u interface{}) interface{}

	apply(t interface{}) interface{}

	andThen(after FunctionEx) BiFunctionEx
}

type ToLongFunctionEx interface {
	applyAsLongEx(t interface{}) int64

	apply(t interface{}) int64
}

type BiPredicateEx interface {
	testEx(var1, var2 interface{}) bool

	and(other BiPredicateEx) BiPredicateEx

	negate() BiPredicateEx

	or(o BiPredicateEx) BiPredicateEx
}

type ObjLongBiFunction interface {
	apply(t interface{}, u int64) interface{}
}

// TriFunction represents a three-arity function that accepts three arguments and produces a result
type TriFunction interface {
	applyEx(t0, t1, t2 interface{}) interface{}

	apply(t0, t1, t2 interface{}) interface{}
}

// TriPredicate a predicate which accepts three arguments
type TriPredicate interface {
	testEx(t0, t1, t2 interface{}) bool

	// and returns a composite predicate which evaluates the equivalent
	and(other TriPredicate) TriPredicate

	// negate ...
	negate() TriPredicate

	// or ...
	or() TriPredicate
}

type ConsumerFn interface {
	accept(t interface{}, value int)
}

type WholeItem struct {
}

func NewWholeItem() WholeItem {
	return WholeItem{}
}

func (w WholeItem) apply(t interface{}) interface{} {
	return t
}

type ConstantItem struct {
	constItem interface{}
}

func NewConstantItem(constItem interface{}) *ConstantItem {
	return &ConstantItem{constItem: constItem}
}

func (c ConstantItem) apply(t interface{}) interface{} {
	return c.constItem
}
