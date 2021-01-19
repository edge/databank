package atomic

import (
	"testing"

	"github.com/edge/databank/pkg/tests"
)

func Test_WriteAndRead(t *testing.T) {
	dt := tests.NewDriverTester(New)
	dt.WriteAndRead(t)
}

func Test_Expiry(t *testing.T) {
	dt := tests.NewDriverTester(New)
	dt.Expiry(t)
}

func Test_TimedExpiry(t *testing.T) {
	dt := tests.NewDriverTester(New)
	dt.TimedExpiry(t)
}

func Test_Overwrite(t *testing.T) {
	dt := tests.NewDriverTester(New)
	dt.Overwrite(t)
}

func Test_Delete(t *testing.T) {
	dt := tests.NewDriverTester(New)
	dt.Delete(t)
}

func Test_Flush(t *testing.T) {
	dt := tests.NewDriverTester(New)
	dt.Flush(t)
}
