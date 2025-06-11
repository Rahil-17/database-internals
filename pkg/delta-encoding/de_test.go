package delta_encoding

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDeltaEncoding(t *testing.T) {
	de := InitDE()

	// Test data with various patterns:
	// - Steady increase
	// - Plateau
	// - Sudden drop
	// - Spike
	// - Small fluctuations
	de.AppendRow(Row{ID: 1, Value: 10, TS: 1000})   // base value
	de.AppendRow(Row{ID: 2, Value: 20, TS: 1002})   // +10
	de.AppendRow(Row{ID: 3, Value: 30, TS: 1004})   // +10
	de.AppendRow(Row{ID: 4, Value: 30, TS: 1006})   // plateau
	de.AppendRow(Row{ID: 5, Value: 20, TS: 1008})   // -10
	de.AppendRow(Row{ID: 6, Value: 50, TS: 1010})   // +30 (spike)
	de.AppendRow(Row{ID: 7, Value: 10, TS: 1012})   // -40 (drop)
	de.AppendRow(Row{ID: 8, Value: 15, TS: 1014})   // +5
	de.AppendRow(Row{ID: 9, Value: 10, TS: 1016})   // -5
	de.AppendRow(Row{ID: 10, Value: 10, TS: 1018})  // plateau

	t.Run("ReconstructRow happy path", func(t *testing.T) {
		// Test first row
		row, err := de.ReconstructRow(1)
		require.NoError(t, err)
		require.Equal(t, Row{ID: 1, Value: 10, TS: 1000}, row)

		// Test middle row with plateau
		row, err = de.ReconstructRow(4)
		require.NoError(t, err)
		require.Equal(t, Row{ID: 4, Value: 30, TS: 1006}, row)

		// Test row after a spike
		row, err = de.ReconstructRow(6)
		require.NoError(t, err)
		require.Equal(t, Row{ID: 6, Value: 50, TS: 1010}, row)

		// Test last row
		row, err = de.ReconstructRow(10)
		require.NoError(t, err)
		require.Equal(t, Row{ID: 10, Value: 10, TS: 1018}, row)
	})

	t.Run("ReconstructRow error cases", func(t *testing.T) {
		// Test invalid row ID (negative)
		_, err := de.ReconstructRow(-1)
		require.Error(t, err)

		// Test invalid row ID (too large)
		_, err = de.ReconstructRow(11)
		require.Error(t, err)

		// Test zero row ID
		_, err = de.ReconstructRow(0)
		require.Error(t, err)
	})

	t.Run("Checkpointing", func(t *testing.T) {
		// Verify checkpoint values are stored correctly
		require.Equal(t, int64(10), de.checkpointValues[0])  // First checkpoint
		require.Equal(t, int64(30), de.checkpointValues[1])  // Second checkpoint
		require.Equal(t, int64(15), de.checkpointValues[2])  // Third checkpoint

		// Verify checkpoint timestamps
		require.Equal(t, int64(1000), de.checkpointTs[0])
		require.Equal(t, int64(1006), de.checkpointTs[1])
		require.Equal(t, int64(1014), de.checkpointTs[2])
	})

	t.Run("DeltaValueList", func(t *testing.T) {
		// Verify delta values are calculated correctly
		require.Equal(t, int64(0), de.deltaValueList[0])   // First row (no delta)
		require.Equal(t, int64(10), de.deltaValueList[1])  // +10
		require.Equal(t, int64(10), de.deltaValueList[2])  // +10
		require.Equal(t, int64(0), de.deltaValueList[3])   // Plateau
		require.Equal(t, int64(-10), de.deltaValueList[4]) // -10
		require.Equal(t, int64(30), de.deltaValueList[5])  // +30 (spike)
		require.Equal(t, int64(-40), de.deltaValueList[6]) // -40 (drop)
		require.Equal(t, int64(5), de.deltaValueList[7])   // +5
		require.Equal(t, int64(-5), de.deltaValueList[8])  // -5
		require.Equal(t, int64(0), de.deltaValueList[9])   // Plateau
	})

	t.Run("DeltaTsList", func(t *testing.T) {
		// Verify timestamp deltas are calculated correctly
		require.Equal(t, int64(0), de.deltaTsList[0])  // First row (no delta)
		require.Equal(t, int64(2), de.deltaTsList[1])  // +2 seconds
		require.Equal(t, int64(2), de.deltaTsList[2])  // +2 seconds
		require.Equal(t, int64(2), de.deltaTsList[3])  // +2 seconds
		require.Equal(t, int64(2), de.deltaTsList[4])  // +2 seconds
	})

	t.Run("EmptyDeltaEncoding", func(t *testing.T) {
		emptyDE := InitDE()
		require.Empty(t, emptyDE.deltaValueList)
		require.Empty(t, emptyDE.deltaTsList)
		require.Empty(t, emptyDE.checkpointValues)
		require.Empty(t, emptyDE.checkpointTs)
		require.True(t, emptyDE.VerifyDeltaEncodingCorrectness())
	})

	t.Run("VerifyDeltaEncodingCorrectness", func(t *testing.T) {
		// Test correctness of delta encoding
		require.True(t, de.VerifyDeltaEncodingCorrectness())

		// Test with corrupted data
		de.deltaValueList[2] = 999999 // Corrupt a delta value
		require.False(t, de.VerifyDeltaEncodingCorrectness())
	})
}