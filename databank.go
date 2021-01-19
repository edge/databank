package databank

import "time"

// Databank is a standard databank frontend containing metadata handling, allowing a backend driver to simply focus on reading/writing to its storage.
type Databank struct {
	l Lifetime
	d Driver
}

// Driver describes a backend that can be used by the Databank frontend.
type Driver interface {
	Delete(k string) bool
	Exists(k string) bool
	Expire(k string) bool
	Flush() bool
	FilterKeys(f Filter) []string
	Read(k string) (Entry, bool)
	Write(k string, e Entry) bool
}

// Entry represents a single data entry and its metadata.
type Entry struct {
	Created time.Time
	Expired bool
	Value   interface{}
}

// Filter represents parameters for internal filtering of data entries. This is used by the Driver implementation.
type Filter struct {
	UseBefore bool
	Before    time.Time

	IncludeExpired bool
	IncludeLive    bool
}

// Lifetime represents a lifetime configuration that applies to all entries in the same databank.
type Lifetime struct {
	Expiry   time.Duration
	Infinite bool
}

// New databank with given driver and lifetime configuration.
func New(d Driver, l Lifetime) *Databank {
	return &Databank{l, d}
}

// Delete a data entry by key.
func (c *Databank) Delete(k string) bool {
	return c.d.Delete(k)
}

// DeleteAllExpired data entries.
func (c *Databank) DeleteAllExpired() bool {
	for _, k := range c.FilterExpiredKeys() {
		c.Delete(k)
	}
	return true
}

// Exists - checks whether a data entry exists for a key.
//
// Note that an expired data entry still 'exists' - it just isn't readable.
func (c *Databank) Exists(k string) bool {
	return c.d.Exists(k)
}

// Expire a data entry by key.
func (c *Databank) Expire(k string) bool {
	return c.d.Expire(k)
}

// ExpireAllExpiring data entries (that are due to expire but have not yet).
func (c *Databank) ExpireAllExpiring() bool {
	for _, k := range c.FilterExpiringKeys() {
		c.Expire(k)
	}
	return true
}

// Flush all entries from databank.
func (c *Databank) Flush() bool {
	return c.d.Flush()
}

// FilterExpiredKeys retrieves a filtered subset of keys in storage that have expired.
func (c *Databank) FilterExpiredKeys() []string {
	p := Filter{
		UseBefore:      false,
		IncludeExpired: true,
		IncludeLive:    false,
	}
	return c.FilterKeys(p)
}

// FilterExpiringKeys retrieves a filtered subset of keys in storage that are due to expire.
func (c *Databank) FilterExpiringKeys() []string {
	if c.l.Infinite {
		return []string{}
	}
	p := Filter{
		UseBefore:      true,
		Before:         c.getCurrentExpiryTime(),
		IncludeExpired: false,
		IncludeLive:    true,
	}
	return c.FilterKeys(p)
}

// FilterKeys retrieves a filtered subset of keys in storage.
func (c *Databank) FilterKeys(f Filter) []string {
	return c.d.FilterKeys(f)
}

// Read a data entry by its key.
func (c *Databank) Read(k string) (Entry, bool) {
	if e, ok := c.d.Read(k); ok {
		if !c.maybeExpire(k, e) {
			return e, true
		}
	}
	return Entry{}, false
}

// ReadValue reads a data entry's value by key. This convenience function elides Entry metadata.
func (c *Databank) ReadValue(k string) (interface{}, bool) {
	if e, ok := c.Read(k); ok {
		return e.Value, ok
	}
	return nil, false
}

// Write a data entry with a key.
//
// If the key is already in use, the existing data entry will be overwritten.
func (c *Databank) Write(k string, e Entry) bool {
	return c.d.Write(k, e)
}

// WriteValue writes a data entry's value by key. This convenience function constructs an Entry with a current timestamp.
//
// If the key is already in use, the existing data entry will be overwritten.
func (c *Databank) WriteValue(k string, v interface{}) bool {
	return c.Write(k, Entry{
		Created: time.Now(),
		Expired: false,
		Value:   v,
	})
}

// getCurrentExpiryTime gets the current time before which entries are considered to be expired.
func (c *Databank) getCurrentExpiryTime() time.Time {
	if c.l.Infinite {
		return time.Time{}
	}
	return time.Now().Add(-c.l.Expiry)
}

// maybeExpire expires an entry automatically if it is older than the lifetime allows.
func (c *Databank) maybeExpire(k string, e Entry) bool {
	if e.Expired {
		return true
	}
	if c.l.Infinite {
		return false
	}
	if e.Created.Before(c.getCurrentExpiryTime()) {
		return c.Expire(k)
	}
	return false
}
