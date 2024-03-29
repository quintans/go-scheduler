package trigger

import (
	"fmt"
	"strconv"
)

func indexes(search, target []string) ([]int, error) {
	searchIndexes := make([]int, 0, len(search))
	for _, a := range search {
		index := intVal(target, a)
		if index == -1 {
			return nil, fmt.Errorf("invalid cron field: %s", a)
		}
		searchIndexes = append(searchIndexes, index)
	}

	return searchIndexes, nil
}

func sliceAtoi(sa []string) ([]int, error) {
	si := make([]int, 0, len(sa))
	for _, a := range sa {
		i, err := strconv.Atoi(a)
		if err != nil {
			return si, err
		}

		si = append(si, i)
	}

	return si, nil
}

func fillRange(from int, to int) ([]int, error) {
	if to < from {
		return nil, cronError("fillRange")
	}

	len := (to - from) + 1
	arr := make([]int, len)

	for i, j := from, 0; i <= to; i, j = i+1, j+1 {
		arr[j] = i
	}

	return arr, nil
}

func fillStep(from int, step int, max int) ([]int, error) {
	if max < from {
		return nil, cronError("fillStep")
	}

	len := ((max - from) / step) + 1
	arr := make([]int, len)

	for i, j := from, 0; i <= max; i, j = i+step, j+1 {
		arr[j] = i
	}

	return arr, nil
}

func normalize(field string, tr []string) int {
	i, err := strconv.Atoi(field)
	if err == nil {
		return i
	}

	return intVal(tr, field)
}

func inScope(i int, min int, max int) bool {
	if i >= min && i <= max {
		return true
	}

	return false
}

func cronError(cause string) error {
	return fmt.Errorf("invalid cron expression: %s", cause)
}

// Align single digit values (for the time.UnixDate format).
func alignDigit(next int, prefix string) string {
	if next < 10 {
		return prefix + strconv.Itoa(next)
	}

	return strconv.Itoa(next)
}

func step(prev int, next int, max int) int {
	diff := next - prev
	if diff < 0 {
		return diff + max
	}

	return diff
}

func intVal(target []string, search string) int {
	for i, v := range target {
		if v == search {
			return i
		}
	}

	return -1 // TODO: return error
}

// Unsafe strconv.Atoi
func atoi(str string) int {
	i, _ := strconv.Atoi(str)
	return i
}

func maxDays(month, year int) int {
	if month == 2 && isLeapYear(year) {
		return 29
	}

	return daysInMonth[month]
}

func isLeapYear(year int) bool {
	switch {
	case year%4 != 0:
		return false
	case year%100 != 0:
		return true
	case year%400 != 0:
		return false
	default:
		return true
	}
}
