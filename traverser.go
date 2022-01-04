package stream_processing

import (
	"container/list"
)

// Traverser a potentially infinite sequence of non null items
type Traverser interface {

	// next return the next item, removing it from the traverser.
	next() interface{}

	// mapX return a traverser that will emit the results of applying mapFn to the traverser' items
	// if mapFn return nil for an item, the return traverser drops it and immediately moves on to next item from this traverser
	mapX(mapFn ApplyFn) Traverser

	// filter return a traverser that will emit the same items as this traverser, but only those that pass the given predicate
	filter(filterFn TestFn) Traverser

	// append returns a traverser that will return all the items of this traverser, plus an additional item once this one returns
	append(item interface{}) Traverser

	// traverseItems return a traverser over the supplied arguments
	traverseItems(items ...interface{}) Traverser

	// flatMap ...
	flatMap(fn ApplyFn) Traverser
}

type AbstractTraverser struct {
	queue *list.List
}

func NewAbstractTraverser() *AbstractTraverser {
	return &AbstractTraverser{queue: list.New()}
}

func (t *AbstractTraverser) flatMap(mapFn ApplyFn) Traverser {
	newQ := list.New()
	for e := t.queue.Front(); e != nil; e = e.Next() {
		if v := mapFn(e.Value); v != nil {
			newQ.PushBack(v)
		} else {
			break
		}
	}
	t.queue = newQ
	return t
}

func (t *AbstractTraverser) traverseItems(items ...interface{}) Traverser {
	for _, item := range items {
		t.append(item)
	}
	return t
}

func (t *AbstractTraverser) append(item interface{}) Traverser {
	t.queue.PushBack(item)
	return t
}

func (t *AbstractTraverser) next() interface{} {
	if t.queue.Len() == 0 {
		return nil
	}
	el := t.queue.Front()
	t.queue.Remove(el)
	return el.Value
}

func (t *AbstractTraverser) mapX(mapFn ApplyFn) Traverser {
	newQ := list.New()
	for e := t.queue.Front(); e != nil; e = e.Next() {
		if v := mapFn(e.Value); v != nil {
			newQ.PushBack(v)
		}
	}
	t.queue = newQ
	return t
}

func (t *AbstractTraverser) filter(filterFn TestFn) Traverser {
	newQ := list.New()
	for e := t.queue.Front(); e != nil; e = e.Next() {
		if ok := filterFn(e.Value); ok {
			newQ.PushBack(e)
		}
	}
	t.queue = newQ
	return t
}

// AppendableTraverser a traverser with an internal container.list as deque. you can efficiently append item to it.
type AppendableTraverser struct {
	*AbstractTraverser
}

func NewAppendableTraverser() *AppendableTraverser {
	t := new(AppendableTraverser)
	t.AbstractTraverser = NewAbstractTraverser()
	return t
}

// isEmpty ...
func (t *AppendableTraverser) isEmpty() bool {
	return t.queue.Len() == 0
}

type ResultTraverser struct {
	*AbstractTraverser
}

func NewResultTraverser(m map[interface{}]interface{}) *ResultTraverser {
	t := new(ResultTraverser)
	t.queue = list.New()
	for k, v := range m {
		entry := MapEntry{key: k, value: v}
		t.queue.PushBack(entry)
	}
	return t
}

type ResettableSingletonTraverser struct {
	item interface{}
}

func (r *ResettableSingletonTraverser) accept(t interface{}) {
	r.item = t
}

func (r *ResettableSingletonTraverser) next() interface{} {
	v := r.item
	r.item = nil
	return v
}

func (r *ResettableSingletonTraverser) mapX(mapFn ApplyFn) Traverser {
	panic("implement me")
}

func (r *ResettableSingletonTraverser) filter(filterFn TestFn) Traverser {
	panic("implement me")
}
