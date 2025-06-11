package delta_encoding

import (
	"testing"
)

func TestAllUtilities(t *testing.T) {
	de := initDE()

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

	t.Run("happy test", func(t *testing.T) {
		// require.Equal(t, de.originalRows[1], de.reconstructRow(2))
	})

	t.Run("reconstructRow sad path (not found)", func(t *testing.T) {
	})

	t.Run("verify reconstruct", func(t *testing.T) {
	})
}