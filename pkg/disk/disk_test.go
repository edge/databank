package disk

import (
	"fmt"
	"os"
	"path"
	"testing"

	"github.com/edge/databank"
	"github.com/edge/databank/pkg/tests"
)

func Test_DiskDriver(t *testing.T) {
	outDir := path.Join(os.TempDir(), "edge", "databank-test")
	fmt.Printf("Disk cache location: %s\n", outDir)

	dt := tests.NewTester(func() databank.Driver {
		dt, err := New(NewConfig(outDir))
		if err != nil {
			panic(err)
		}
		return dt
	})
	dt.Run(t)
}
