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

	ad := atomicdb.New()
	dd, err := disk.New(disk.NewConfig(outDir))
	if err != nil {
		t.Error(err)
		return
	}

	dt := tests.NewTester(func() databank.Driver {
		return NewSync(ad, dd)
	})
	dt.Run(t)
}
