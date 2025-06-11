package rle

import "fmt"

type Row struct {
	ID    int
	Value int
	TS    string
}

type TSRun struct {
	ts    string
	count int
}

type RLE struct {
	idList    []int
	valueList []int
	TSRuns    []TSRun
	tsRunEnds []int // rle.tsRunEnds stores the end row index of each TS run (inclusive)
}


func InitRLE() (*RLE){
	return &RLE {
		idList : []int{},
		valueList: []int{},
		TSRuns: []TSRun{},
		tsRunEnds: []int{},
	}
}

// AppendRow populates the RLE encoding for the given ts.
// time complexity: O(1)
func (rle *RLE) AppendRow(row Row) {
	rle.idList = append(rle.idList, row.ID)
	rle.valueList = append(rle.valueList, row.Value)

	if len(rle.TSRuns) == 0 || rle.TSRuns[len(rle.TSRuns)-1].ts != row.TS {
		rle.TSRuns = append(rle.TSRuns, TSRun{
			ts:    row.TS,
			count: 1,
		})
		if len(rle.tsRunEnds) == 0 {
			rle.tsRunEnds = append(rle.tsRunEnds, 1)
		} else {
			rle.tsRunEnds = append(rle.tsRunEnds, rle.tsRunEnds[len(rle.tsRunEnds)-1]+1)
		}
	} else {
		rle.TSRuns[len(rle.TSRuns)-1].count++
		rle.tsRunEnds[len(rle.tsRunEnds)-1]++
	}
}

func (t TSRun) String() string {
	return fmt.Sprintf("{TS: %s, Count: %d}", t.ts, t.count)
}

// ReconstructRow reconstructs the row from the RLE encoding.
// time complexity: O(log n)
func (rle *RLE) ReconstructRow(rowID int) (Row, error) {
	if rowID <= 0 || rowID > len(rle.idList) {
		return Row{}, fmt.Errorf("row with id %d does not exist", rowID)
	}
	ts := rle.GetTSFromRowIDFaster(rowID)
	return Row{rle.idList[rowID-1], rle.valueList[rowID-1], ts}, nil
}

// GetTSFromRowID implements point query.
// time complexity: O(n)
func (rle *RLE) GetTSFromRowID(rowID int) string {
	if rowID <= 0 || rowID > len(rle.idList) {
		return ""
	}

	for _, entry := range rle.TSRuns {
		if entry.count >= rowID {
			return entry.ts
		}
		rowID -= entry.count
	}
	return ""
}

// GetTSFromRowIDFaster implements point query using prefix sum and binary search.
// time complexity: O(log n)
func (rle *RLE) GetTSFromRowIDFaster(rowID int) string {
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

	return rle.TSRuns[low].ts
}

// GetCountofTS implements count(ts) query.
// time complexity: O(n)
func (rle *RLE) GetCountofTS(ts string) (int, error) {
	for _, entry := range rle.TSRuns {
		if entry.ts == ts {
			return entry.count, nil
		}
	}
	return 0, fmt.Errorf("ts %s not found", ts)
}

// GetCountofTSFaster implements count(ts) query using binary search.
// time complexity: O(log n)
func (rle *RLE) GetCountofTSFaster(ts string) (int, error) {
	low := 0
	high := len(rle.TSRuns) - 1
	for low <= high {
		mid := (low + high) / 2
		if rle.TSRuns[mid].ts == ts {
			return rle.TSRuns[mid].count, nil
		} else if rle.TSRuns[mid].ts < ts {
			low = mid + 1
		} else {
			high = mid - 1
		}
	}
	return 0, fmt.Errorf("ts %s not found", ts)
}
