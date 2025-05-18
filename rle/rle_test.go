package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAllUtilities(t *testing.T) {
	rle := RLE{}

	rle.appendRow(Row{id: 1, value: 100, ts: "10:00:00"})
	rle.appendRow(Row{id: 2, value: 200, ts: "10:00:00"})
	rle.appendRow(Row{id: 3, value: 300, ts: "10:00:02"})
	rle.appendRow(Row{id: 4, value: 400, ts: "10:00:02"})
	rle.appendRow(Row{id: 5, value: 500, ts: "10:00:02"})
	rle.appendRow(Row{id: 6, value: 600, ts: "10:00:03"})


	t.Run("happy test", func(t *testing.T) {
	// Test getTSFromRowID for edge cases
	require.Equal(t, "10:00:00", rle.getTSFromRowID(1)) // first row
	require.Equal(t, "10:00:02", rle.getTSFromRowID(4)) // last row of 10:00:02
	require.Equal(t, "10:00:03", rle.getTSFromRowID(6)) // last row

	// Out of bounds for getTSFromRowID
	require.Equal(t, "", rle.getTSFromRowID(-1))
	require.Equal(t, "", rle.getTSFromRowID(8))

	// Test getTSFromRowIDFaster for all valid and edge cases
	require.Equal(t, "10:00:00", rle.getTSFromRowIDFaster(1))
	require.Equal(t, "10:00:02", rle.getTSFromRowIDFaster(3))
	require.Equal(t, "10:00:02", rle.getTSFromRowIDFaster(4))
	require.Equal(t, "10:00:03", rle.getTSFromRowIDFaster(6))
	require.Equal(t, "", rle.getTSFromRowIDFaster(-1))
	require.Equal(t, "", rle.getTSFromRowIDFaster(8))

	// Test reconstructRow for all valid and edge cases
	row, err := rle.reconstructRow(2)
	require.NoError(t, err)
	require.Equal(t, Row{id: 2, value: 200, ts: "10:00:00"}, row)

	row, err = rle.reconstructRow(6)
	require.NoError(t, err)
	require.Equal(t, Row{id: 6, value: 600, ts: "10:00:03"}, row)

	_, err = rle.reconstructRow(-1)
	require.Error(t, err)

	_, err = rle.reconstructRow(8)
	require.Error(t, err)

	// Test getCountofTS for edge case: first, last, and not found
	count, err := rle.getCountofTS("10:00:00")
	require.NoError(t, err)
	require.Equal(t, 2, count)

	count, err = rle.getCountofTS("10:00:03")
	require.NoError(t, err)
	require.Equal(t, 1, count)

	_, err = rle.getCountofTS("not-exist")
	require.Error(t, err)

	// Test getCountofTSFaster for edge case: first, last, and not found
	count, err = rle.getCountofTSFaster("10:00:00")
	require.NoError(t, err)
	require.Equal(t, 2, count)

	count, err = rle.getCountofTSFaster("10:00:03")
	require.NoError(t, err)
	require.Equal(t, 1, count)

	_, err = rle.getCountofTSFaster("not-exist")
	require.Error(t, err)

	// Test TSRun String method
	require.Equal(t, "{TS: 10:00:00, Count: 2}", rle.tsRuns[0].String())
	require.Equal(t, "{TS: 10:00:02, Count: 3}", rle.tsRuns[1].String())
	require.Equal(t, "{TS: 10:00:03, Count: 1}", rle.tsRuns[2].String())
	})

	t.Run("getCountofTS happy path", func(t *testing.T) {
		count, err := rle.getCountofTS("10:00:00")
		require.NoError(t, err)
		require.Equal(t, 2, count)

		count, err = rle.getCountofTS("10:00:02")
		require.NoError(t, err)
		require.Equal(t, 3, count)

		count, err = rle.getCountofTS("10:00:03")
		require.NoError(t, err)
		require.Equal(t, 1, count)
	})

	t.Run("getCountofTS sad path (not found)", func(t *testing.T) {
		count, err := rle.getCountofTS("10:00:01")
		require.Error(t, err)
		require.Equal(t, 0, count)
	})

	t.Run("getCountofTSFaster happy path", func(t *testing.T) {
		count, err := rle.getCountofTSFaster("10:00:00")
		require.NoError(t, err)
		require.Equal(t, 2, count)

		count, err = rle.getCountofTSFaster("10:00:02")
		require.NoError(t, err)
		require.Equal(t, 3, count)

		count, err = rle.getCountofTSFaster("10:00:03")
		require.NoError(t, err)
		require.Equal(t, 1, count)
	})

	t.Run("getCountofTSFaster sad path (not found)", func(t *testing.T) {
		count, err := rle.getCountofTSFaster("10:00:01")
		require.Error(t, err)
		require.Equal(t, 0, count)
	})
}
