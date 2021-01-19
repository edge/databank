package tests

import (
	"testing"
	"time"

	"github.com/edge/databank"
	"github.com/stretchr/testify/assert"
)

// DatabankFactory is a function that provides a fully configured databank for unit testing.
type DatabankFactory = func(*databank.Config) databank.Databank

// DriverTester encapsulates a series of tests that can be run with a databank.
type DriverTester struct {
	factory DatabankFactory
}

// baseSleepDuration is the base unit for sleep operations. This helps keep temporal testing internally consistent.
const baseSleepDuration = 100 * time.Millisecond

// NewDriverTester creates a databank tester. You must provide a factory function for creating said databank.
func NewDriverTester(factory DatabankFactory) *DriverTester {
	dt := &DriverTester{
		factory: factory,
	}
	return dt
}

// Delete test.
func (dt *DriverTester) Delete(t *testing.T) {
	a := assert.New(t)
	d := dt.factory(databank.NewConfig())

	d.WriteString("test", "abc")
	a.Equal(true, d.Delete("test"))
	a.Equal(false, d.Has("test"))
}

// Expiry test.
func (dt *DriverTester) Expiry(t *testing.T) {
	a := assert.New(t)
	d := dt.factory(databank.NewConfig())

	d.WriteString("test", "abc")
	a.Equal(true, d.Has("test"))
	a.Equal(true, d.Expire("test"))
	a.Equal(true, d.Has("test"))

	d.Flush()
}

// Flush test.
func (dt *DriverTester) Flush(t *testing.T) {
	a := assert.New(t)
	d := dt.factory(databank.NewConfig())

	d.WriteString("test", "abc")
	a.Equal(true, d.Flush())
	a.Equal(false, d.Has("test"))
}

// Overwrite test.
func (dt *DriverTester) Overwrite(t *testing.T) {
	a := assert.New(t)
	d := dt.factory(databank.NewConfig())

	d.WriteString("test", "abc")
	val, ok := d.ReadString("test")
	a.Equal(true, ok)
	a.Equal("abc", val)

	_, ok = d.WriteString("test", "def")
	a.Equal(true, ok)
	val, ok = d.ReadString("test")
	a.Equal(true, ok)
	a.Equal("def", val)

	d.Flush()
}

// TimedExpiry test.
func (dt *DriverTester) TimedExpiry(t *testing.T) {
	a := assert.New(t)
	c := databank.NewConfig()
	c.Lifetime = baseSleepDuration
	d := dt.factory(c)

	d.WriteString("test", "abc")
	time.Sleep(2 * baseSleepDuration)
	_, ok := d.ReadString("test")
	a.Equal(false, ok)

	d.Flush()
}

// WriteAndRead test.
func (dt *DriverTester) WriteAndRead(t *testing.T) {
	a := assert.New(t)
	d := dt.factory(databank.NewConfig())

	e, ok := d.WriteString("test", "abc")
	a.Equal(true, ok)
	a.Equal([]byte("abc"), e.Content)

	val, ok := d.ReadString("test")
	a.Equal(true, ok)
	a.Equal("abc", val)

	d.Flush()
}
