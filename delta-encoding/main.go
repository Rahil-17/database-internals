// This program demonstrates delta encoding with checkpointing for a columnar time-series dataset.
// It compresses data by storing differences (deltas) in `value` and `ts` columns instead of absolute values.
//
// Key Concepts:
//   - Delta Encoding: Stores only the change (delta) from the previous value.
//   - Checkpointing: Periodically stores full (absolute) values to reduce reconstruction cost.
//   - Varint Estimation: Simulates how much space each value would take using variable-length encoding.
//
// Assumptions:
//   - Input rows are ordered by time (`ts` increases).
//   - `value` changes slowly or predictably between rows (typical in metric time series).
//   - Each row has a unique, incrementing `id`.
//
// This tool also benchmarks the space savings achieved via delta encoding.

package main

import (
	"encoding/binary"
	"fmt"
)

type Row struct {
	id    int
	value int64
	ts    int64
}

type DeltaEncoding struct {
	idList []int
	deltaValueList []int64
	deltaTsList []int64
	originalRows []Row
	lastValue int64
	lastTs int64
	checkpointInterval int
    checkpointValues   []int64  // absolute values at checkpoints
    checkpointTs       []int64  // absolute ts at checkpoints
}

func initDE() (*DeltaEncoding) {
	return &DeltaEncoding{
		idList:         []int{},
		deltaValueList: []int64{},
		deltaTsList:    []int64{},
		lastValue: 0,
		lastTs: 0,
		checkpointInterval: 4,
    	checkpointValues:   []int64{},
    	checkpointTs:       []int64{},
	}
}

// appendRow populates the Delta encoding for the given row.
// time complexity: O(1)
func (de *DeltaEncoding) appendRow(row Row) {
	if len(de.idList) == 0 {
		de.deltaValueList = append(de.deltaValueList, 0)
		de.deltaTsList = append(de.deltaTsList, 0)

		de.checkpointValues = append(de.checkpointValues, row.value)
		de.checkpointTs = append(de.checkpointTs, row.ts)
	} else {
		de.deltaValueList = append(de.deltaValueList, row.value-de.lastValue)
		de.deltaTsList = append(de.deltaTsList, row.ts-de.lastTs)
	}
	de.idList = append(de.idList, row.id)
	de.lastValue = row.value
	de.lastTs = row.ts
	de.originalRows = append(de.originalRows, row)

	// Checkpoint
	if len(de.idList) % de.checkpointInterval == 0 {
		de.checkpointValues = append(de.checkpointValues, row.value)
		de.checkpointTs = append(de.checkpointTs, row.ts)
	}
}

// verifyDeltaEncodingCorrectness checks whether the delta-encoded data can be fully
// and correctly reconstructed to match the original input rows.
//
// Note: This uses exact equality checks for all fields (id, value, ts).
// Since value and ts are stored as int64, there's no tolerance for minor differences.
// This is appropriate for strictly deterministic data like metrics or timestamps with fixed intervals.
//
// Returns true if all reconstructed rows exactly match the originals; false otherwise.
func (de *DeltaEncoding) verifyDeltaEncodingCorrectness() bool {
	deRows, err := de.reconstructTable()
	if err != nil {
		return false
	}
	for ind := range(len(de.originalRows)){
		if deRows[ind] != de.originalRows[ind] {
			return false
		}
	}
	return true
}

func (de *DeltaEncoding) reconstructTable() ([]Row, error) {
	rows := []Row{}
	for _, id := range(de.idList) {
		row, err := de.reconstructRow(id)
		if err!= nil {
			return nil, err
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func (de *DeltaEncoding) reconstructRow(rowID int) (Row, error) {
	if rowID <= 0 || rowID > len(de.idList) {
		return Row{}, fmt.Errorf("row with id %d does not exist", rowID)
	}
	row := Row{}
	row.id = rowID


	// Optimisation: Using checkpointing to avoid recalculation from the base value.
	checkpointIndex := (rowID-1)/de.checkpointInterval
	row.value = de.checkpointValues[checkpointIndex]
	row.ts = de.checkpointTs[checkpointIndex]

	rowIndex := rowID - 1
	for ind := checkpointIndex * de.checkpointInterval; ind <= rowIndex; ind++ {
		row.value += de.deltaValueList[ind]
		row.ts += de.deltaTsList[ind]
	}

	return row, nil
}

func varintEncodedSizeGeneric[T ~int | ~int64](data []T) int {
	buf := make([]byte, binary.MaxVarintLen64)
	total := 0
	for _, v := range data {
		n := binary.PutVarint(buf, int64(v))
		total += n
	}
	return total
}

func binaryEncodedSize(rows []Row) int {
	total := 0
	buf := make([]byte, binary.MaxVarintLen64)
	for _, row := range rows {
		// Estimate encoded size as if each field was varint-encoded separately.
		total += binary.PutVarint(buf, int64(row.id))
		total += binary.PutVarint(buf, row.value)
		total += binary.PutVarint(buf, row.ts)
	}
	return total
}

func (de *DeltaEncoding) printStats(){
	fmt.Printf("\n\nVarint Encoded Sizes:\n")

	totalVarintSize := varintEncodedSizeGeneric(de.idList) +
	varintEncodedSizeGeneric(de.deltaValueList) +
	varintEncodedSizeGeneric(de.deltaTsList)
	orignalSize := binaryEncodedSize(de.originalRows)

	fmt.Printf("Total compressed size (varint): %d bytes\n", totalVarintSize)
	fmt.Printf("Original size (varint): %d bytes\n", orignalSize)
	fmt.Printf("Saved: %d bytes (%.2f%%)\n", orignalSize - totalVarintSize,
  		float64(orignalSize - totalVarintSize)*100.0/float64(orignalSize))
}


func main() {
	de := initDE()

	// columns: id, value, ts.
	// Assume we are storing memory usage for every 2 sec interval
	de.appendRow(Row{id: 1, value: 10737418240, ts: 1000})   // base 10 GB
	de.appendRow(Row{id: 2, value: 10747914240, ts: 1002})   // +10 MB (small increase)
	de.appendRow(Row{id: 3, value: 10758390272, ts: 1004})   // +10 MB (steady rise)
	de.appendRow(Row{id: 4, value: 10758390272, ts: 1006})   // 0 (plateau)
	de.appendRow(Row{id: 5, value: 10727939072, ts: 1008})   // -29 MB (dip)
	de.appendRow(Row{id: 6, value: 10821304320, ts: 1010})   // +88 MB (spike)
	de.appendRow(Row{id: 7, value: 10569646080, ts: 1012})   // -252 MB (drop)
	de.appendRow(Row{id: 8, value: 10580344320, ts: 1014})   // +10 MB (noise)
	de.appendRow(Row{id: 9, value: 10569646080, ts: 1016})   // -10 MB (noise)
	de.appendRow(Row{id:10, value: 10569646080, ts: 1018})   // 0 (plateau again)

	// fmt.Println(de.checkpointValues, de.deltaValueList)
	fmt.Println(de.reconstructRow(1))
	fmt.Println(de.reconstructRow(3))
	fmt.Println(de.reconstructRow(5))
	fmt.Println(de.reconstructRow(10))

	fmt.Printf("\n\nIs delta encoding correct: %t\n", de.verifyDeltaEncodingCorrectness())

	de.printStats()
}