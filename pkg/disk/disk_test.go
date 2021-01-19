package disk

import (
	"os"
	"path"
	"testing"

	"github.com/edge/databank"
	"github.com/edge/databank/pkg/tests"
)

func newTestDatabank(c *databank.Config) databank.Databank {
	dc := NewConfig(path.Join(os.TempDir(), "edge", "databank-test"))
	return New(c, dc)
}

func Test_WriteAndRead(t *testing.T) {
	dt := tests.NewDriverTester(newTestDatabank)
	dt.WriteAndRead(t)
}

func Test_Expiry(t *testing.T) {
	dt := tests.NewDriverTester(newTestDatabank)
	dt.Expiry(t)
}

func Test_TimedExpiry(t *testing.T) {
	dt := tests.NewDriverTester(newTestDatabank)
	dt.TimedExpiry(t)
}

func Test_Overwrite(t *testing.T) {
	dt := tests.NewDriverTester(newTestDatabank)
	dt.Overwrite(t)
}

func Test_Delete(t *testing.T) {
	dt := tests.NewDriverTester(newTestDatabank)
	dt.Delete(t)
}

func Test_Flush(t *testing.T) {
	dt := tests.NewDriverTester(newTestDatabank)
	dt.Flush(t)
}
