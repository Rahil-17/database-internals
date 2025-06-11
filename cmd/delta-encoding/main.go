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
	"fmt"

	deltaEncoding "github.com/rahil/database-internals/pkg/delta-encoding"
)

func main() {
	de := deltaEncoding.InitDE()

	// columns: id, value, ts.
	// Assume we are storing memory usage for every 2 sec interval
	de.AppendRow(deltaEncoding.Row{ID: 1, Value: 10737418240, TS: 1000})   // base 10 GB
	de.AppendRow(deltaEncoding.Row{ID: 2, Value: 10747914240, TS: 1002})   // +10 MB (small increase)
	de.AppendRow(deltaEncoding.Row{ID: 3, Value: 10758390272, TS: 1004})   // +10 MB (steady rise)
	de.AppendRow(deltaEncoding.Row{ID: 4, Value: 10758390272, TS: 1006})   // 0 (plateau)
	de.AppendRow(deltaEncoding.Row{ID: 5, Value: 10727939072, TS: 1008})   // -29 MB (dip)
	de.AppendRow(deltaEncoding.Row{ID: 6, Value: 10821304320, TS: 1010})   // +88 MB (spike)
	de.AppendRow(deltaEncoding.Row{ID: 7, Value: 10569646080, TS: 1012})   // -252 MB (drop)
	de.AppendRow(deltaEncoding.Row{ID: 8, Value: 10580344320, TS: 1014})   // +10 MB (noise)
	de.AppendRow(deltaEncoding.Row{ID: 9, Value: 10569646080, TS: 1016})   // -10 MB (noise)
	de.AppendRow(deltaEncoding.Row{ID:10, Value: 10569646080, TS: 1018})   // 0 (plateau again)

	// fmt.Println(de.checkpointValues, de.deltaValueList)
	fmt.Println(de.ReconstructRow(1))
	fmt.Println(de.ReconstructRow(3))
	fmt.Println(de.ReconstructRow(5))
	fmt.Println(de.ReconstructRow(10))

	fmt.Printf("\n\nIs delta encoding correct: %t\n", de.VerifyDeltaEncodingCorrectness())

	de.PrintStats()
}