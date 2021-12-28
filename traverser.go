package stream_processing

import "container/list"

// Traverser a potentially infinite sequence of non null items
type Traverser interface {
	next() interface{}
}

// AppendableTraverser a traverser with an internal container.list as deque. you can efficiently append item to it.
type AppendableTraverser struct {
	queue *list.List
}

func NewAppendableTraverser() *AppendableTraverser {
	return &AppendableTraverser{queue: list.New()}
}

// append ...
func (t *AppendableTraverser) append(item interface{}) *AppendableTraverser {
	t.queue.PushBack(item)
	return t
}

// next retrieves and returns the head of the queue
func (t *AppendableTraverser) next() interface{} {
	el := t.queue.Front()
	t.queue.Remove(el)
	return el.Value
}

// isEmpty ...
func (t *AppendableTraverser) isEmpty() bool {
	return t.queue.Len() == 0
}
