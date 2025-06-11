// Assumptions: Concerned TS column is sorted and it's a columnar database.

package main

import (
	"fmt"

	rle "github.com/rahil/database-internals/pkg/rle"
)


func printCountOrError(count int, err error) {
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println(count)
	}
}

func main() {
	rleInst := rle.InitRLE()

	// columns: id, value, ts.
	rleInst.AppendRow(rle.Row{ID: 1, Value: 100, TS: "10:00:00"})
	rleInst.AppendRow(rle.Row{ID: 2, Value: 200, TS: "10:00:00"})
	rleInst.AppendRow(rle.Row{ID: 3, Value: 300, TS: "10:00:02"})
	rleInst.AppendRow(rle.Row{ID: 4, Value: 400, TS: "10:00:02"})
	rleInst.AppendRow(rle.Row{ID: 5, Value: 500, TS: "10:00:02"})
	rleInst.AppendRow(rle.Row{ID: 6, Value: 600, TS: "10:00:03"})

	fmt.Println(rleInst.TSRuns)

	fmt.Println(rleInst.ReconstructRow(1))
	fmt.Println(rleInst.ReconstructRow(7))

	fmt.Println(rleInst.GetTSFromRowID(1))
	fmt.Println(rleInst.GetTSFromRowID(5))
	fmt.Println(rleInst.GetTSFromRowID(6))

	fmt.Println(rleInst.GetTSFromRowIDFaster(1))
	fmt.Println(rleInst.GetTSFromRowIDFaster(5))
	fmt.Println(rleInst.GetTSFromRowIDFaster(6))

	printCountOrError(rleInst.GetCountofTS("10:00:00"))
	printCountOrError(rleInst.GetCountofTS("10:00:01"))
	printCountOrError(rleInst.GetCountofTSFaster("10:00:00"))
	printCountOrError(rleInst.GetCountofTSFaster("10:00:01"))
}
