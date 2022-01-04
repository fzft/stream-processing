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
