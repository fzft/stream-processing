package stream_processing

type FunctionEx interface {
	// applyEx ...
	applyEx(t interface{}) interface{}

	// apply ...
	apply(t interface{}) interface{}

	// identity ...
	identity() interface{}

	// compose ...
	compose(before FunctionEx) interface{}

	// andThen ...
	andThen(after FunctionEx) interface{}
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