package databank

// Databank is a standard cache frontend for any backend Driver.
type Databank interface {
	// Cleanup all expired entries.
	Cleanup() (uint, bool)
	// Count total number of entries.
	// Note that this includes expired entries.
	Count() (uint, bool)
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
	// Review entries, automatically expiring them as necessary.
	Review() (uint, bool)
	// Scan for IDs.
	Scan() ([]string, bool)
	// Search entries.
	Search(q *Query) (map[string]*Entry, bool)
	// Write an entry to storage.
	Write(e *Entry) bool

	// Driver provides direct access to the backend storage API, bypassing standard Databank features and middlewares.
	// This is only advised for use in tests.
	// Production code should use Databank's abstractions.
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
	WriteInt16(key string, val int16) (*Entry, bool)
	// WriteInt32 to storage.
	WriteInt32(key string, val int32) (*Entry, bool)
	// WriteInt64 to storage.
	WriteInt64(key string, val int64) (*Entry, bool)
	// WriteString to storage.
	WriteString(key, val string) (*Entry, bool)
	// WriteUint16 to storage.
	WriteUint16(key string, val uint16) (*Entry, bool)
	// WriteUint32 to storage.
	WriteUint32(key string, val uint32) (*Entry, bool)
	// WriteUint64 to storage.
	WriteUint64(key string, val uint64) (*Entry, bool)
}

// Driver describes the storage API required by databank.
//
// Errors returned from a Driver implementation reflect critical operational problems, while Booleans indicate a normal state response.
// For example, Read() returns the Entry, a boolean confirming the entry was read successfully, and an error if there was an unexpected reason the Entry was not found e.g. connection failure.
// It is possible for Read() to return false and no error, indicating that the read failed but not unexpectedly.
type Driver interface {
	// Cleanup all expired entries.
	Cleanup() (uint, bool, []error)
	// Count total number of entries in storage.
	// Note that this includes expired entries.
	Count() (uint, bool, error)
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

// New standard Databank with your config and driver.
// Once initialised, the Databank's settings and structure cannot be altered.
func New(c *Config, d Driver) Databank {
	// TODO this shouldn't be optional - force explicit invocation
	var config *Config
	if c != nil {
		config = c
	} else {
		config = NewConfig()
	}
	db := &databank{
		config: config,
		driver: d,
	}
	return db
}

func (d *databank) Cleanup() (uint, bool) {
	n, ok, _ := d.driver.Cleanup()
	return n, ok
}

func (d *databank) Count() (uint, bool) {
	n, ok, _ := d.driver.Count()
	return n, ok
}

func (d *databank) Delete(id string) bool {
	ok, _ := d.driver.Delete(id)
	return ok
}

func (d *databank) Driver() Driver {
	return d.driver
}

func (d *databank) Expire(id string) bool {
	ok, _ := d.driver.Expire(id)
	return ok
}

func (d *databank) Flush() bool {
	ok, _ := d.driver.Flush()
	return ok
}

func (d *databank) Has(id string) bool {
	ok, _ := d.driver.Has(id)
	return ok
}

func (d *databank) NewEntry(key string) *Entry {
	return NewEntry(key, d.config.Lifetime)
}

func (d *databank) Read(id string) (*Entry, bool) {
	e, ok, _ := d.driver.Read(id)
	if ok && !d.config.Hot {
		if e.MaybeExpire() {
			d.Write(e)
			return nil, false
		}
	}
	return e, ok
}

func (d *databank) Review() (uint, bool) {
	n, ok, _ := d.driver.Review()
	return n, ok
}

func (d *databank) Scan() ([]string, bool) {
	ids, ok, _ := d.driver.Scan()
	return ids, ok
}

func (d *databank) Search(q *Query) (map[string]*Entry, bool) {
	results, ok, _ := d.driver.Search(q)
	return results, ok
}

func (d *databank) Write(e *Entry) bool {
	e.CalculateSize()
	ok, _ := d.driver.Write(e)
	return ok
}
