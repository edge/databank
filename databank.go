package databank

import (
	"fmt"
)

// Databank is a standard cache frontend for any backend Driver.
type Databank interface {
	// Cleanup all expired entries.
	Cleanup() (uint, bool)
	// Delete an entry.
	Delete(id string) bool
	// Expire an entry.
	Expire(id string) bool
	// Flush all entries.
	Flush() bool
	// Has an ID, i.e. entry exists in storage?
	// Note that an expired entry still 'exists' until it is deleted or flushed out.
	Has(id string) bool
	// NewEntry creates a preconfigured, empty Entry.
	NewEntry(key string) *Entry
	// Read an entry from storage.
	Read(id string) (*Entry, bool)
	// Restore entries from storage.
	Restore() bool
	// Review entries, automatically expiring them as necessary.
	Review() (uint, bool)
	// Scan for IDs.
	Scan() ([]string, bool)
	// Search entries.
	Search(q *Query) (map[string]*Entry, bool)
	// Write an entry to storage.
	Write(e *Entry) bool

	// Driver provides direct access to the backend storage API.
	Driver() Driver

	// ReadInt16 from storage.
	ReadInt16(id string) (int16, bool)
	// ReadInt32 from storage.
	ReadInt32(id string) (int32, bool)
	// ReadInt64 from storage.
	ReadInt64(id string) (int64, bool)
	// ReadString from storage.
	ReadString(id string) (string, bool)
	// ReadUint16 from storage.
	ReadUint16(id string) (uint16, bool)
	// ReadUint32 from storage.
	ReadUint32(id string) (uint32, bool)
	// ReadUint64 from storage.
	ReadUint64(id string) (uint64, bool)
	// WriteInt16 to storage.
	WriteInt16(id string, val int16) (*Entry, bool)
	// WriteInt32 to storage.
	WriteInt32(id string, val int32) (*Entry, bool)
	// WriteInt64 to storage.
	WriteInt64(id string, val int64) (*Entry, bool)
	// WriteString to storage.
	WriteString(id, val string) (*Entry, bool)
	// WriteUint16 to storage.
	WriteUint16(id string, val uint16) (*Entry, bool)
	// WriteUint32 to storage.
	WriteUint32(id string, val uint32) (*Entry, bool)
	// WriteUint64 to storage.
	WriteUint64(id string, val uint64) (*Entry, bool)
}

// Driver describes the storage API required by databank.
//
// Errors returned from a Driver implementation reflect critical operational problems, while Booleans indicate a normal state response.
// For example, Read() returns the Entry, a boolean confirming the entry was read successfully, and an error if there was an unexpected reason the Entry was not found e.g. connection failure.
// It is possible for Read() to return false and no error, indicating that the read failed but not unexpectedly.
type Driver interface {
	// Cleanup all expired entries.
	Cleanup() (uint, bool, []error)
	// Delete an entry.
	// The bool return reflects the entry's nonexistence in storage when this function returns.
	// Ergo, if the ID is not found, this function still returns true.
	Delete(id string) (bool, error)
	// Expire an entry.
	// The bool return reflects whether the entry is in an expired or otherwise unreachable state when this function returns.
	// Ergo, if the ID is not found, this function still returns true.
	Expire(id string) (bool, error)
	// Flush all entries.
	Flush() (bool, []error)
	// Has an ID, i.e. entry exists in storage?
	// Note that an expired entry still 'exists' until it is deleted or flushed out.
	Has(id string) (bool, error)
	// Read an entry from storage.
	Read(id string) (*Entry, bool, error)
	// Restore entries from storage.
	Restore() (bool, error)
	// Review entries, automatically expiring them as necessary.
	Review() (uint, bool, []error)
	// Scan for IDs.
	Scan() ([]string, bool, error)
	// Search entries.
	Search(q *Query) (map[string]*Entry, bool, error)
	// Write an entry to storage.
	Write(e *Entry) (bool, error)
}

// Query TODO
type Query struct{}

// databank is the internal implementation of Databank.
type databank struct {
	config *Config
	driver Driver
}

// New Databank.
func New(d Driver, c *Config) Databank {
	var config *Config
	if c != nil {
		config = c
	} else {
		config = NewConfig()
	}
	return &databank{
		config: config,
		driver: d,
	}
}

func (d *databank) Cleanup() (uint, bool) {
	n, ok, errs := d.driver.Cleanup()
	for _, err := range errs {
		if err != nil {
			d.report(err)
		}
	}
	return n, ok
}

func (d *databank) Delete(id string) bool {
	ok, err := d.driver.Delete(id)
	if err != nil {
		d.report(err)
	}
	return ok
}

func (d *databank) Driver() Driver {
	return d.driver
}

func (d *databank) Expire(id string) bool {
	ok, err := d.driver.Expire(id)
	if err != nil {
		d.report(err)
	}
	return ok
}

func (d *databank) Flush() bool {
	ok, errs := d.driver.Flush()
	for _, err := range errs {
		if err != nil {
			d.report(err)
		}
	}
	return ok
}

func (d *databank) Has(id string) bool {
	ok, err := d.driver.Has(id)
	if err != nil {
		d.report(err)
	}
	return ok
}

func (d *databank) NewEntry(key string) *Entry {
	return NewEntry(key, d.config.Lifetime)
}

func (d *databank) Read(id string) (*Entry, bool) {
	e, ok, err := d.driver.Read(id)
	if err != nil {
		d.report(err)
	}
	if ok && !d.config.Hot {
		if e.MaybeExpire() {
			d.Write(e)
			return nil, false
		}
	}
	return e, ok
}

func (d *databank) Restore() bool {
	ok, err := d.driver.Restore()
	if err != nil {
		d.report(err)
	}
	return ok
}

func (d *databank) Review() (uint, bool) {
	n, ok, errs := d.driver.Review()
	for _, err := range errs {
		if err != nil {
			d.report(err)
		}
	}
	return n, ok
}

func (d *databank) Scan() ([]string, bool) {
	ids, ok, err := d.driver.Scan()
	if err != nil {
		d.report(err)
	}
	return ids, ok
}

func (d *databank) Search(q *Query) (map[string]*Entry, bool) {
	results, ok, err := d.driver.Search(q)
	if err != nil {
		d.report(err)
	}
	return results, ok
}

func (d *databank) Write(e *Entry) bool {
	e.CalculateSize()
	ok, err := d.driver.Write(e)
	if err != nil {
		d.report(err)
	}
	return ok
}

// TODO improve
func (d *databank) report(err error) {
	fmt.Println(err)
}
