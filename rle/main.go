// Assumptions: Concerned TS column is sorted and it's a columnar database.

package main

import (
	"fmt"
)

type Row struct {
	id    int
	value int
	ts    string
}

type TSRun struct {
	ts    string
	count int
}

var idList []int
var valueList []int
var tsRuns []TSRun
var tsRunEnds []int // tsRunEnds stores the end row index of each TS run (inclusive)

// appendRow populates the RLE encoding for the given ts.
// time complexity: O(1)
func appendRow(row Row) {
	idList = append(idList, row.id)
	valueList = append(valueList, row.value)

	if len(tsRuns) == 0 || tsRuns[len(tsRuns)-1].ts != row.ts {
		tsRuns = append(tsRuns, TSRun{
			ts:    row.ts,
			count: 1,
		})
		if len(tsRunEnds) == 0 {
			tsRunEnds = append(tsRunEnds, 1)
		} else {
			tsRunEnds = append(tsRunEnds, tsRunEnds[len(tsRunEnds)-1]+1)
		}
	} else {
		tsRuns[len(tsRuns)-1].count++
		tsRunEnds[len(tsRunEnds)-1]++
	}
}
func (t TSRun) String() string {
	return fmt.Sprintf("{TS: %s, Count: %d}", t.ts, t.count)
}

// reconstructRow reconstructs the row from the RLE encoding.
// time complexity: O(log n)
func reconstructRow(rowID int) Row {
	if rowID < 0 || rowID >= len(idList) {
		return Row{0, 0, ""}
	}
	ts := getTSFromRowIDFaster(rowID)
	return Row{idList[rowID], valueList[rowID], ts}
}

// getTSFromRowID implements point query.
// time complexity: O(n)
func getTSFromRowID(rowID int) string {
	if rowID < 0 || rowID >= len(idList) {
		return ""
	}

	for _, entry := range tsRuns {
		if entry.count >= rowID {
			return entry.ts
		}
		rowID -= entry.count
	}
	return ""
}

// getTSFromRowIDFaster implements point query using prefix sum and binary search.
// time complexity: O(log n)
func getTSFromRowIDFaster(rowID int) string {
	if rowID < 0 || rowID > tsRunEnds[len(tsRunEnds)-1] {
		return ""
	}

	low := 0
	high := len(tsRunEnds) - 1
	for low <= high {
		mid := (low + high) / 2
		if tsRunEnds[mid] >= rowID {
			high = mid - 1
		} else {
			low = mid + 1
		}
	}

	return tsRuns[low].ts
}

// getCountofTS implements count(ts) query.
// time complexity: O(n)
func getCountofTS(ts string) (int, error) {
	for _, entry := range tsRuns {
		if entry.ts == ts {
			return entry.count, nil
		}
	}
	return 0, fmt.Errorf("ts %s not found", ts)
}

// getCountofTSFaster implements count(ts) query using binary search.
// time complexity: O(log n)
func getCountofTSFaster(ts string) (int, error) {
	low := 0
	high := len(tsRuns) - 1
	for low <= high {
		mid := (low + high) / 2
		if tsRuns[mid].ts == ts {
			return tsRuns[mid].count, nil
		} else if tsRuns[mid].ts < ts {
			low = mid + 1
		} else {
			high = mid - 1
		}
	}
	return 0, fmt.Errorf("ts %s not found", ts)
}

func main() {
	// columns: id, value, ts.
	appendRow(Row{id: 1, value: 100, ts: "10:00:00"})
	appendRow(Row{id: 2, value: 200, ts: "10:00:00"})
	appendRow(Row{id: 3, value: 300, ts: "10:00:02"})
	appendRow(Row{id: 4, value: 400, ts: "10:00:02"})
	appendRow(Row{id: 5, value: 500, ts: "10:00:02"})
	appendRow(Row{id: 6, value: 600, ts: "10:00:03"})

	fmt.Println(tsRuns)
	fmt.Println(reconstructRow(1))
	fmt.Println(reconstructRow(7))

	fmt.Println(getTSFromRowID(1))
	fmt.Println(getTSFromRowID(5))
	fmt.Println(getTSFromRowID(6))

	fmt.Println(getTSFromRowIDFaster(1))
	fmt.Println(getTSFromRowIDFaster(5))
	fmt.Println(getTSFromRowIDFaster(6))

	count, err := getCountofTS("10:00:00")
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println(count)
	}

	count, err = getCountofTS("10:00:01")
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println(count)
	}

	count, err = getCountofTSFaster("10:00:00")
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println(count)
	}

	count, err = getCountofTSFaster("10:00:01")
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println(count)
	}

}
