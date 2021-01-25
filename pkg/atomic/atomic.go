package atomic

import (
	"github.com/edge/atomicstore"
	"github.com/edge/databank"
)

// Driver is the atomic implementation of databank.Driver.
type Driver struct {
	store *atomicstore.Store
}

// New databank with an atomic Driver backend.
func New(c *databank.Config) databank.Databank {
	return databank.New(NewDriver(), c)
}

// NewDriver creates an atomic Driver.
func NewDriver() *Driver {
	return &Driver{
		store: atomicstore.New(true),
	}
}

// Cleanup all expired entries.
func (d *Driver) Cleanup() (uint, bool, []error) {
	var deleted uint
	errs := []error{}
	d.store.Range(func(id, value interface{}) bool {
		e := value.(databank.Entry)
		if e.Meta.Expired {
			ok, err := d.Delete(id.(string))
			if err != nil {
				errs = append(errs, err)
			}
			if ok {
				deleted++
			}
			return ok
		}
		return true
	})
	return deleted, true, errs
}

// Delete an entry.
// The bool return reflects the entry's nonexistence in storage when this function returns.
// Ergo, if the ID is not found, this function still returns true.
func (d *Driver) Delete(id string) (bool, error) {
	d.store.Delete(id)
	return true, nil
}

// Expire an entry.
// The bool return reflects whether the entry is in an expired or otherwise unreachable state when this function returns.
// Ergo, if the ID is not found, this function still returns true.
func (d *Driver) Expire(id string) (bool, error) {
	e, ok, err := d.Read(id)
	if err != nil {
		return false, err
	}
	if ok {
		e.Expire()
		return d.Write(e)
	}
	return true, nil
}

// Flush all entries.
func (d *Driver) Flush() (bool, []error) {
	d.store.Flush()
	return true, []error{}
}

// Has an ID, i.e. entry exists in storage?
// Note that an expired entry still 'exists' until it is deleted or flushed out.
func (d *Driver) Has(id string) (bool, error) {
	for storedID := range d.store.GetKeyMap() {
		if storedID == id {
			return true, nil
		}
	}
	return false, nil
}

// Read an entry from storage.
func (d *Driver) Read(id string) (*databank.Entry, bool, error) {
	if e, ok := d.store.Get(id); ok {
		return e.(*databank.Entry), ok, nil
	}
	return nil, false, nil
}

// Review entries, automatically expiring them as necessary.
func (d *Driver) Review() (uint, bool, []error) {
	var expired uint
	d.store.Range(func(_, value interface{}) bool {
		e := value.(*databank.Entry)
		if e.ShouldExpire() {
			e.Expire()
			ok, _ := d.Write(e)
			if ok {
				expired++
			}
			return ok
		}
		return true
	})
	return expired, true, []error{}
}

// Scan for IDs.
func (d *Driver) Scan() ([]string, bool, error) {
	keys := []string{}
	for key := range d.store.GetKeyMap() {
		keys = append(keys, key)
	}
	return keys, true, nil
}

// Search entries.
//
// TODO
func (d *Driver) Search(q *databank.Query) (map[string]*databank.Entry, bool, error) {
	return map[string]*databank.Entry{}, false, nil
}

// Write an entry to storage.
func (d *Driver) Write(e *databank.Entry) (bool, error) {
	d.store.Insert(e.ID(), e)
	return true, nil
}
