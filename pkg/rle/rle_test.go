package rle

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAllUtilities(t *testing.T) {
	rle := RLE{}

	rle.AppendRow(Row{ID: 1, Value: 100, TS: "10:00:00"})
	rle.AppendRow(Row{ID: 2, Value: 200, TS: "10:00:00"})
	rle.AppendRow(Row{ID: 3, Value: 300, TS: "10:00:02"})
	rle.AppendRow(Row{ID: 4, Value: 400, TS: "10:00:02"})
	rle.AppendRow(Row{ID: 5, Value: 500, TS: "10:00:02"})
	rle.AppendRow(Row{ID: 6, Value: 600, TS: "10:00:03"})


	t.Run("happy test", func(t *testing.T) {
	// Test GetTSFromRowID for edge cases
	require.Equal(t, "10:00:00", rle.GetTSFromRowID(1)) // first row
	require.Equal(t, "10:00:02", rle.GetTSFromRowID(4)) // last row of 10:00:02
	require.Equal(t, "10:00:03", rle.GetTSFromRowID(6)) // last row

	// Out of bounds for GetTSFromRowID
	require.Equal(t, "", rle.GetTSFromRowID(-1))
	require.Equal(t, "", rle.GetTSFromRowID(8))

	// Test GetTSFromRowIDFaster for all valid and edge cases
	require.Equal(t, "10:00:00", rle.GetTSFromRowIDFaster(1))
	require.Equal(t, "10:00:02", rle.GetTSFromRowIDFaster(3))
	require.Equal(t, "10:00:02", rle.GetTSFromRowIDFaster(4))
	require.Equal(t, "10:00:03", rle.GetTSFromRowIDFaster(6))
	require.Equal(t, "", rle.GetTSFromRowIDFaster(-1))
	require.Equal(t, "", rle.GetTSFromRowIDFaster(8))

	// Test ReconstructRow for all valid and edge cases
	row, err := rle.ReconstructRow(2)
	require.NoError(t, err)
	require.Equal(t, Row{ID: 2, Value: 200, TS: "10:00:00"}, row)

	row, err = rle.ReconstructRow(6)
	require.NoError(t, err)
	require.Equal(t, Row{ID: 6, Value: 600, TS: "10:00:03"}, row)

	_, err = rle.ReconstructRow(-1)
	require.Error(t, err)

	_, err = rle.ReconstructRow(8)
	require.Error(t, err)

	// Test GetCountofTS for edge case: first, last, and not found
	count, err := rle.GetCountofTS("10:00:00")
	require.NoError(t, err)
	require.Equal(t, 2, count)

	count, err = rle.GetCountofTS("10:00:03")
	require.NoError(t, err)
	require.Equal(t, 1, count)

	_, err = rle.GetCountofTS("not-exist")
	require.Error(t, err)

	// Test GetCountofTSFaster for edge case: first, last, and not found
	count, err = rle.GetCountofTSFaster("10:00:00")
	require.NoError(t, err)
	require.Equal(t, 2, count)

	count, err = rle.GetCountofTSFaster("10:00:03")
	require.NoError(t, err)
	require.Equal(t, 1, count)

	_, err = rle.GetCountofTSFaster("not-exist")
	require.Error(t, err)

	// Test TSRun String method
	require.Equal(t, "{TS: 10:00:00, Count: 2}", rle.TSRuns[0].String())
	require.Equal(t, "{TS: 10:00:02, Count: 3}", rle.TSRuns[1].String())
	require.Equal(t, "{TS: 10:00:03, Count: 1}", rle.TSRuns[2].String())
	})

	t.Run("GetCountofTS happy path", func(t *testing.T) {
		count, err := rle.GetCountofTS("10:00:00")
		require.NoError(t, err)
		require.Equal(t, 2, count)

		count, err = rle.GetCountofTS("10:00:02")
		require.NoError(t, err)
		require.Equal(t, 3, count)

		count, err = rle.GetCountofTS("10:00:03")
		require.NoError(t, err)
		require.Equal(t, 1, count)
	})

	t.Run("GetCountofTS sad path (not found)", func(t *testing.T) {
		count, err := rle.GetCountofTS("10:00:01")
		require.Error(t, err)
		require.Equal(t, 0, count)
	})

	t.Run("GetCountofTSFaster happy path", func(t *testing.T) {
		count, err := rle.GetCountofTSFaster("10:00:00")
		require.NoError(t, err)
		require.Equal(t, 2, count)

		count, err = rle.GetCountofTSFaster("10:00:02")
		require.NoError(t, err)
		require.Equal(t, 3, count)

		count, err = rle.GetCountofTSFaster("10:00:03")
		require.NoError(t, err)
		require.Equal(t, 1, count)
	})

	t.Run("GetCountofTSFaster sad path (not found)", func(t *testing.T) {
		count, err := rle.GetCountofTSFaster("10:00:01")
		require.Error(t, err)
		require.Equal(t, 0, count)
	})
}
