package stream_processing

import "container/list"

// Inbox a subset of queue restricted to the consumer side
type Inbox interface {
	isEmpty() bool

	// peek Retrieves, but does not remove
	peek() interface{}

	// poll Retrieves and removes the head of this inbox
	poll() interface{}

	// remove the head of this inbox
	remove()

	Iter() []interface{}

	clear()
}

// TestInbox implementation suitable to be used in tests
type TestInbox struct {
	queue *list.List
}

func NewTestInbox() *TestInbox {
	return &TestInbox{queue: list.New()}
}

func (t *TestInbox) isEmpty() bool {
	return t.queue.Len() == 0
}

func (t *TestInbox) peek() interface{} {
	return t.queue.Front().Value
}

func (t *TestInbox) poll() interface{} {
	if t.isEmpty() {
		return nil
	}
	elem := t.queue.Front()
	t.queue.Remove(elem)
	return elem.Value
}

func (t *TestInbox) remove() {
	if t.isEmpty() {
		return
	}
	elem := t.queue.Front()
	t.queue.Remove(elem)
}

func (t *TestInbox) Iter() []interface{} {
	var iterList []interface{}
	for e := t.queue.Front(); e != nil; e = e.Next() {
		iterList = append(iterList, e.Value)
	}
	return iterList
}

func (t *TestInbox) clear() {
	t.queue = list.New()
}
