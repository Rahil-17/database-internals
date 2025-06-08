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

type RLE struct {
	idList    []int
	valueList []int
	tsRuns    []TSRun
	tsRunEnds []int // rle.tsRunEnds stores the end row index of each TS run (inclusive)
}

// appendRow populates the RLE encoding for the given ts.
// time complexity: O(1)
func rleInit() (*RLE){
	return &RLE {
		idList : []int{},
		valueList: []int{},
		tsRuns: []TSRun{},
		tsRunEnds: []int{},
	}
}

func (rle *RLE) appendRow(row Row) {
	rle.idList = append(rle.idList, row.id)
	rle.valueList = append(rle.valueList, row.value)

	if len(rle.tsRuns) == 0 || rle.tsRuns[len(rle.tsRuns)-1].ts != row.ts {
		rle.tsRuns = append(rle.tsRuns, TSRun{
			ts:    row.ts,
			count: 1,
		})
		if len(rle.tsRunEnds) == 0 {
			rle.tsRunEnds = append(rle.tsRunEnds, 1)
		} else {
			rle.tsRunEnds = append(rle.tsRunEnds, rle.tsRunEnds[len(rle.tsRunEnds)-1]+1)
		}
	} else {
		rle.tsRuns[len(rle.tsRuns)-1].count++
		rle.tsRunEnds[len(rle.tsRunEnds)-1]++
	}
}

func (t TSRun) String() string {
	return fmt.Sprintf("{TS: %s, Count: %d}", t.ts, t.count)
}

// reconstructRow reconstructs the row from the RLE encoding.
// time complexity: O(log n)
func (rle *RLE) reconstructRow(rowID int) (Row, error) {
	if rowID <= 0 || rowID > len(rle.idList) {
		return Row{}, fmt.Errorf("row with id %d does not exist", rowID)
	}
	ts := rle.getTSFromRowIDFaster(rowID)
	return Row{rle.idList[rowID-1], rle.valueList[rowID-1], ts}, nil
}

// getTSFromRowID implements point query.
// time complexity: O(n)
func (rle *RLE) getTSFromRowID(rowID int) string {
	if rowID <= 0 || rowID > len(rle.idList) {
		return ""
	}

	for _, entry := range rle.tsRuns {
		if entry.count >= rowID {
			return entry.ts
		}
		rowID -= entry.count
	}
	return ""
}

// getTSFromRowIDFaster implements point query using prefix sum and binary search.
// time complexity: O(log n)
func (rle *RLE) getTSFromRowIDFaster(rowID int) string {
	if rowID <= 0 || rowID > rle.tsRunEnds[len(rle.tsRunEnds)-1] {
		return ""
	}

	low := 0
	high := len(rle.tsRunEnds) - 1
	for low <= high {
		mid := (low + high) / 2
		if rle.tsRunEnds[mid] >= rowID {
			high = mid - 1
		} else {
			low = mid + 1
		}
	}

	return rle.tsRuns[low].ts
}

// getCountofTS implements count(ts) query.
// time complexity: O(n)
func (rle *RLE) getCountofTS(ts string) (int, error) {
	for _, entry := range rle.tsRuns {
		if entry.ts == ts {
			return entry.count, nil
		}
	}
	return 0, fmt.Errorf("ts %s not found", ts)
}

// getCountofTSFaster implements count(ts) query using binary search.
// time complexity: O(log n)
func (rle *RLE) getCountofTSFaster(ts string) (int, error) {
	low := 0
	high := len(rle.tsRuns) - 1
	for low <= high {
		mid := (low + high) / 2
		if rle.tsRuns[mid].ts == ts {
			return rle.tsRuns[mid].count, nil
		} else if rle.tsRuns[mid].ts < ts {
			low = mid + 1
		} else {
			high = mid - 1
		}
	}
	return 0, fmt.Errorf("ts %s not found", ts)
}

func printCountOrError(count int, err error) {
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println(count)
	}
}

func main() {
	rle := rleInit()

	// columns: id, value, ts.
	rle.appendRow(Row{id: 1, value: 100, ts: "10:00:00"})
	rle.appendRow(Row{id: 2, value: 200, ts: "10:00:00"})
	rle.appendRow(Row{id: 3, value: 300, ts: "10:00:02"})
	rle.appendRow(Row{id: 4, value: 400, ts: "10:00:02"})
	rle.appendRow(Row{id: 5, value: 500, ts: "10:00:02"})
	rle.appendRow(Row{id: 6, value: 600, ts: "10:00:03"})

	fmt.Println(rle.tsRuns)

	fmt.Println(rle.reconstructRow(1))
	fmt.Println(rle.reconstructRow(7))

	fmt.Println(rle.getTSFromRowID(1))
	fmt.Println(rle.getTSFromRowID(5))
	fmt.Println(rle.getTSFromRowID(6))

	fmt.Println(rle.getTSFromRowIDFaster(1))
	fmt.Println(rle.getTSFromRowIDFaster(5))
	fmt.Println(rle.getTSFromRowIDFaster(6))

	printCountOrError(rle.getCountofTS("10:00:00"))
	printCountOrError(rle.getCountofTS("10:00:01"))
	printCountOrError(rle.getCountofTSFaster("10:00:00"))
	printCountOrError(rle.getCountofTSFaster("10:00:01"))
}
