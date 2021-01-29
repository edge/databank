package tests

import (
	"fmt"
	"testing"
	"time"

	"github.com/edge/databank"
	"github.com/stretchr/testify/assert"
)

// DriverFactory is a function that provides a fully configured databank for unit testing.
type DriverFactory = func() databank.Driver

// Tester encapsulates a series of tests that can be run over a databank.
type Tester struct {
	driverFactory DriverFactory
	step          int
}

type testEntry struct {
	ID      string
	Key     string
	Size    int
	Content string
	Tags    map[string]string
}

// baseSleepDuration is the base unit for sleep operations. This helps keep temporal testing internally consistent.
const baseSleepDuration = 100 * time.Millisecond

var invalidIDs = []string{"false", "true", "null", "y10gu", "zzz"}

// testData provides some iterable
var testData = []testEntry{
	{"test", "test", 3, "abc", map[string]string{}},
	{"test2", "test2", 6, "defghi", map[string]string{}},
	{"test3", "test3", 44, "RWRnZSBuZXR3b3JrIGlzIGJlc3QgbmV0d29yayEhITE=", map[string]string{}},
	{"test4_16257516605739849767", "test4", 5, "jklmn", map[string]string{"tag1": "val1"}},
	{"test5_5091786465586096835", "test5", 3, "opq", map[string]string{"tag2": "val2", "tag1": "val1"}},
	{"test6_12385537651473712091", "test6", 4, "rstu", map[string]string{"vec": "54", "met": "100003", "zoop": "21.9"}},
}

// NewTester creates a databank tester. You must provide a factory function for creating said databank.
func NewTester(factory DriverFactory) *Tester {
	dt := &Tester{
		driverFactory: factory,
		step:          0,
	}
	return dt
}

// Run all basic tests in predefined sequence.
func (dt *Tester) Run(t *testing.T) {
	// setup. elides some error checking as we don't want to _properly_ test yet
	d := dt.newDatabank(databank.NewConfig())
	if ids, _ := d.Scan(); len(ids) > 0 {
		if ok := d.Flush(); !ok {
			t.Fatal("Unable to flush polluted databank before testing")
		}
	}

	// basic crud: write, scan, has, read, delete
	dt.testWrite(t, d)
	dt.testCount(t, d)
	dt.testScan(t, d)
	dt.testHas(t, d)
	dt.testRead(t, d)
	dt.testDelete(t, d)
	dt.testFlush(t, d)

	// test various handling of expiry
	// dt.testExpire(t, d)
}

func (dt *Tester) newDatabank(c *databank.Config) databank.Databank {
	return databank.New(c, dt.driverFactory())
}

// expect the current or next step number.
// This use a simple internal counter to help ensure tests are run in a compatible sequence, providing safety while developing tests.
func (dt *Tester) expect(step int) {
	if step == dt.step {
		return
	}
	next := dt.step + 1
	if next != step {
		panic(fmt.Errorf("Expected next step %d but encountered %d", step, next))
	}
	dt.step = next
}

func (dt *Tester) testCount(t *testing.T, d databank.Databank) {
	dt.expect(2)
	a := assert.New(t)
	n, ok := d.Count()
	a.Equal(true, ok)
	a.Equal(uint(len(testData)), n)
}

func (dt *Tester) testDelete(t *testing.T, d databank.Databank) {
	dt.expect(5)
	a := assert.New(t)

	delIDs := []string{"test", "test2"}
	for _, id := range delIDs {
		a.Equal(true, d.Delete(id))
	}

	ids, _ := d.Scan()
	a.Equal(len(testData)-len(delIDs), len(ids))
	for _, id := range ids {
		for _, did := range delIDs {
			a.NotEqual(id, did)
		}
	}
}

func (dt *Tester) testFlush(t *testing.T, d databank.Databank) {
	dt.expect(5)
	a := assert.New(t)
	a.Equal(true, d.Flush())
}

func (dt *Tester) testHas(t *testing.T, d databank.Databank) {
	dt.expect(3)
	a := assert.New(t)

	ids, _ := d.Scan()
	for _, id := range ids {
		a.Equal(true, d.Has(id))
	}

	for _, id := range invalidIDs {
		a.Equal(false, d.Has(id))
	}
}

func (dt *Tester) testRead(t *testing.T, d databank.Databank) {
	dt.expect(4)
	a := assert.New(t)

	for _, data := range testData {
		e, ok := d.Read(data.ID)
		a.Equal(true, ok)
		a.Equal(data.Key, e.Key)
		a.Equal([]byte(data.Content), e.Content)
		a.Equal(data.Size, e.Size)
	}
}

func (dt *Tester) testScan(t *testing.T, d databank.Databank) {
	dt.expect(2)
	a := assert.New(t)

	ids, ok := d.Scan()
	a.Equal(true, ok)
	a.Equal(len(testData), len(ids))

	n, _ := d.Count()
	a.Equal(n, uint(len(ids)))

	// bonus
	for i, id := range ids {
		if testData[i].ID != id {
			fmt.Println("Warning: your driver's Scan() method may not produce deterministic results")
			break
		}
	}
}

func (dt *Tester) testWrite(t *testing.T, d databank.Databank) {
	dt.expect(1)
	a := assert.New(t)

	for _, data := range testData {
		e := d.NewEntry(data.Key)
		if len(data.Tags) > 0 {
			e.Tags = data.Tags
		}
		e.Content = []byte(data.Content)
		a.Equal(data.ID, e.ID())
		a.Equal(true, d.Write(e))
		a.Equal(data.Size, e.Size)
	}
}
