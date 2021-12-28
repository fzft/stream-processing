package stream_processing

import "math"

const (
	Max_Value = int64(math.MaxInt64)
	Min_Value = int64(math.MinInt64)
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

// Min64 min version of int64
func Min64(a, b int64) int64 {
	if a <= b {
		return a
	}
	return b
}

// Max64 max version of int64
func Max64(a, b int64) int64 {
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

// sumHadOverflow ...
func sumHadOverflow(a, b, sum int64) bool {
	return ((a ^ sum) & (b ^ sum)) < 0
}

// addClamped ...
func addClamped(a, b int64) int64 {
	sum := a + b
	if sumHadOverflow(a, b, sum) {
		if a >= 0 {
			return Max_Value
		} else {
			return Min_Value
		}
	} else {
		return sum
	}
}

func floorMod(x, y int64) int64 {
	return x - floorDiv(x, y)*y
}

func floorDiv(x int64, y int64) int64 {
	r := x / y
	if (x^y) < 0 && (r*y != x) {
		r--
	}
	return r
}
