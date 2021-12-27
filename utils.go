package stream_processing

const (
	Max_Value = 99999999
	Min_Value = -9999999
)

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

// SubtractClamped calculates a - b returns Max_Value or Min_Value
func SubtractClamped(a, b int64) int64 {
	diff := a - b
	if diffHadOverflow(a, b, diff) {
		if a >= 0 {
			return Max_Value
		} else {
			return Min_Value
		}
	} else {
		return diff
	}
}

// diffHadOverflow ...
func diffHadOverflow(a, b, diff int64) bool {
	return ((a ^ b) & (a ^ diff)) < 0
}
