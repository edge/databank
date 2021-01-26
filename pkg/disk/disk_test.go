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
		return New(NewConfig(outDir))
	})
	dt.Run(t)
}
