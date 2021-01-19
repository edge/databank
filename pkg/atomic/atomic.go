package atomic

import (
	"github.com/edge/atomicstore"
	"github.com/edge/databank"
)

// Driver provides a Databank backend based on atomic store.
type Driver struct {
	s *atomicstore.Store
}

// New databank with an atomic Driver backend.
func New(l databank.Lifetime) *databank.Databank {
	d := &Driver{
		s: atomicstore.New(true),
	}
	return databank.New(d, l)
}

// Delete a data entry by key.
func (d *Driver) Delete(k string) bool {
	d.s.Delete(k)
	return true
}

// Exists - checks whether a data entry exists for a key.
func (d *Driver) Exists(k string) bool {
	_, ok := d.Read(k)
	return ok
}

// Expire a data entry by key.
func (d *Driver) Expire(k string) bool {
	if e, ok := d.Read(k); ok {
		e.Expired = true
		return d.Write(k, e)
	}
	return true
}

// Flush all entries from storage.
func (d *Driver) Flush() bool {
	d.s.Flush()
	return true
}

// FilterKeys retrieves a filtered subset of keys in storage.
func (d *Driver) FilterKeys(p databank.Filter) []string {
	filtered := []string{}
	for k := range d.s.GetKeyMap() {
		e, ok := d.Read(k)
		if !ok {
			continue
		}
		if !d.filterEntry(p, e) {
			continue
		}
		filtered = append(filtered, k)
	}
	return filtered
}

// Read a data entry by its key.
func (d *Driver) Read(k string) (databank.Entry, bool) {
	if e, ok := d.s.Get(k); ok {
		return e.(databank.Entry), ok
	}
	return databank.Entry{}, false
}

// Write a data entry with a key.
func (d *Driver) Write(k string, e databank.Entry) bool {
	d.s.Insert(k, e)
	return true
}

// filterEntry checks whether an entry satisfies filter constraints.
func (d *Driver) filterEntry(p databank.Filter, e databank.Entry) bool {
	if e.Expired && !p.IncludeExpired {
		return false
	}
	if !e.Expired && !p.IncludeLive {
		return false
	}
	if p.UseBefore {
		return e.Created.Before(p.Before)
	}
	return true
}
