package atomic

import (
	"github.com/edge/atomicstore"
	"github.com/edge/databank"
)

// driver is the atomic implementation of databank.Driver.
type driver struct {
	store *atomicstore.Store
}

// New databank with an atomic Driver backend.
func New(c *databank.Config) databank.Databank {
	return databank.New(NewDriver(), c)
}

// NewDriver creates an atomic Driver.
func NewDriver() databank.Driver {
	return &driver{
		store: atomicstore.New(true),
	}
}

// Cleanup all expired entries.
func (d *driver) Cleanup() (uint, bool, []error) {
	var deleted uint
	errs := []error{}
	d.store.Range(func(key, value interface{}) bool {
		e := value.(databank.Entry)
		if e.Meta.Expired {
			ok, err := d.Delete(key.(string))
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

// Delete a data entry by key.
func (d *driver) Delete(id string) (bool, error) {
	d.store.Delete(id)
	return true, nil
}

// Expire a data entry by key.
func (d *driver) Expire(id string) (bool, error) {
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
func (d *driver) Flush() (bool, []error) {
	d.store.Flush()
	return true, []error{}
}

// Has an ID, i.e. entry exists in storage?
// Note that an expired entry still 'exists' until it is deleted or flushed out.
func (d *driver) Has(id string) (bool, error) {
	for storedID := range d.store.GetKeyMap() {
		if storedID == id {
			return true, nil
		}
	}
	return false, nil
}

// Read a data entry by its key.
func (d *driver) Read(id string) (*databank.Entry, bool, error) {
	if e, ok := d.store.Get(id); ok {
		return e.(*databank.Entry), ok, nil
	}
	return nil, false, nil
}

func (d *driver) Restore() (bool, error) {
	return true, nil
}

// Review entries, automatically expiring them as necessary.
func (d *driver) Review() (uint, bool, []error) {
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

func (d *driver) Scan() ([]string, bool, error) {
	keys := []string{}
	for key := range d.store.GetKeyMap() {
		keys = append(keys, key)
	}
	return keys, true, nil
}

// Search entries.
//
// TODO
func (d *driver) Search(q *databank.Query) (map[string]*databank.Entry, bool, error) {
	return map[string]*databank.Entry{}, false, nil
}

// Write a data entry with a key.
func (d *driver) Write(e *databank.Entry) (bool, error) {
	d.store.Insert(e.ID(), e)
	return true, nil
}
