package atomic

import (
	"testing"

	"github.com/edge/databank"
	"github.com/edge/databank/pkg/tests"
)

func newCompat() databank.Driver {
	return New()
}

func Test_AtomicDriver(t *testing.T) {
	dt := tests.NewTester(func() databank.Driver {
		return New()
	})
	dt.Run(t)
}
