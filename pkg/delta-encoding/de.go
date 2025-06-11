package delta_encoding

import (
	"encoding/binary"
	"fmt"
)

type Row struct {
	ID    int
	Value int64
	TS    int64
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

func InitDE() (*DeltaEncoding) {
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

// AppendRow populates the Delta encoding for the given row.
// time complexity: O(1)
func (de *DeltaEncoding) AppendRow(row Row) {
	if len(de.idList) == 0 {
		de.deltaValueList = append(de.deltaValueList, 0)
		de.deltaTsList = append(de.deltaTsList, 0)

		de.checkpointValues = append(de.checkpointValues, row.Value)
		de.checkpointTs = append(de.checkpointTs, row.TS)
	} else {
		de.deltaValueList = append(de.deltaValueList, row.Value-de.lastValue)
		de.deltaTsList = append(de.deltaTsList, row.TS-de.lastTs)
	}
	de.idList = append(de.idList, row.ID)
	de.lastValue = row.Value
	de.lastTs = row.TS
	de.originalRows = append(de.originalRows, row)

	// Checkpoint
	if len(de.idList) % de.checkpointInterval == 0 {
		de.checkpointValues = append(de.checkpointValues, row.Value)
		de.checkpointTs = append(de.checkpointTs, row.TS)
	}
}

// VerifyDeltaEncodingCorrectness checks whether the delta-encoded data can be fully
// and correctly reconstructed to match the original input rows.
//
// Note: This uses exact equality checks for all fields (id, value, ts).
// Since value and ts are stored as int64, there's no tolerance for minor differences.
// This is appropriate for strictly deterministic data like metrics or timestamps with fixed intervals.
//
// Returns true if all reconstructed rows exactly match the originals; false otherwise.
func (de *DeltaEncoding) VerifyDeltaEncodingCorrectness() bool {
	deRows, err := de.ReconstructTable()
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

func (de *DeltaEncoding) ReconstructTable() ([]Row, error) {
	rows := []Row{}
	for _, id := range(de.idList) {
		row, err := de.ReconstructRow(id)
		if err!= nil {
			return nil, err
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func (de *DeltaEncoding) ReconstructRow(rowID int) (Row, error) {
	if rowID <= 0 || rowID > len(de.idList) {
		return Row{}, fmt.Errorf("row with id %d does not exist", rowID)
	}
	row := Row{}
	row.ID = rowID


	// Optimisation: Using checkpointing to avoid recalculation from the base value.
	checkpointIndex := (rowID-1)/de.checkpointInterval
	row.Value = de.checkpointValues[checkpointIndex]
	row.TS = de.checkpointTs[checkpointIndex]

	rowIndex := rowID - 1
	for ind := checkpointIndex * de.checkpointInterval; ind <= rowIndex; ind++ {
		row.Value += de.deltaValueList[ind]
		row.TS += de.deltaTsList[ind]
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
		total += binary.PutVarint(buf, int64(row.ID))
		total += binary.PutVarint(buf, row.Value)
		total += binary.PutVarint(buf, row.TS)
	}
	return total
}

func (de *DeltaEncoding) PrintStats() {
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
