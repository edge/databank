package proxy

import (
	"testing"

	"github.com/edge/databank"
	atomicdb "github.com/edge/databank/pkg/atomic"
	"github.com/edge/databank/pkg/tests"
)

func newSyncDatabank(c *databank.Config) databank.Databank {
	d := newSyncDriver()
	return databank.New(d, c)
}

func newSyncDriver() *SyncDriver {
	return NewSyncDriver(atomicdb.NewDriver(), atomicdb.NewDriver())
}

func Test_WriteAndRead(t *testing.T) {
	dt := tests.NewDriverTester(newSyncDatabank)
	dt.WriteAndRead(t)
}

func Test_Expiry(t *testing.T) {
	dt := tests.NewDriverTester(newSyncDatabank)
	dt.Expiry(t)
}

func Test_TimedExpiry(t *testing.T) {
	dt := tests.NewDriverTester(newSyncDatabank)
	dt.TimedExpiry(t)
}

func Test_Overwrite(t *testing.T) {
	dt := tests.NewDriverTester(newSyncDatabank)
	dt.Overwrite(t)
}

func Test_Delete(t *testing.T) {
	dt := tests.NewDriverTester(newSyncDatabank)
	dt.Delete(t)
}

func Test_Flush(t *testing.T) {
	dt := tests.NewDriverTester(newSyncDatabank)
	dt.Flush(t)
}
