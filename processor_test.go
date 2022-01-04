package stream_processing

const (
	MOCK_ITEM           = "x"
	OUTBOX_BUCKET_COUNT = 4
	ORDINAL_0           = iota
	ORDINAL_1
	ORDINAL_2
	ORDINAL_3
	ORDINAL_4
	ORDINAL_5
)

var (
	ORDINALS_1_2 = []int{1, 2}
	ALL_ORDINALS = []int{0, 1, 2, 3, 4}
)

type ProcessorTest struct {

}
