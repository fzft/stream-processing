package stream_processing

func AnyMatch(streams []interface{}, matchFn func(v interface{}) bool) bool {
	for _, v := range streams {
		if matchFn(v) {
			return true
		}
	}
	return false
}


func Min(a, b int) int {
	if a <= b {
		return a
	}
	return b
}

func Max(a, b int) int {
	if a >= b {
		return a
	}
	return b
}

