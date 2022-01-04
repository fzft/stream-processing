package stream_processing


var (
	NO_PROGRESS = NewProgressState(false, false)
	MADE_PROGRESS = NewProgressState(true, false)
	DONE = NewProgressState(true, true)
	WAS_ALREADY_DONE = NewProgressState(false, true)
)


type ProgressState struct {
	madeProgress bool
	isDone bool
}

func NewProgressState(madeProgress bool, isDone bool) *ProgressState {
	return &ProgressState{madeProgress: madeProgress, isDone: isDone}
}


