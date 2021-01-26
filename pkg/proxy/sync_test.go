package proxy

import (
	"fmt"
	"os"
	"path"
	"testing"

	"github.com/edge/databank"
	atomicdb "github.com/edge/databank/pkg/atomic"
	"github.com/edge/databank/pkg/disk"
	"github.com/edge/databank/pkg/tests"
)

func Test_Proxy_SyncDriver(t *testing.T) {
	outDir := path.Join(os.TempDir(), "edge", "databank-test")
	fmt.Printf("Disk cache location: %s\n", outDir)

	dt := tests.NewTester(func() databank.Driver {
		return NewSync(
			atomicdb.New(),
			disk.New(disk.NewConfig(outDir)),
		)
	})
	dt.Run(t)
}
