package proxy

import (
	"github.com/edge/databank"
)

// SyncDriver is a synchronous proxying implementation of databank.Driver.
// It takes any number of other drivers, and mirrors read/write operations sequentially across them.
//
// The order of drivers is important. You should generally sort drivers from least to most authoritative, and we call the last driver the 'authority driver'.
// For example, the first driver might be an in-memory driver, and the second a disk driver.
// This would mean that read operations hit the in-memory driver first, whereas a delete operation works backward from the disk driver.
// This combines the performance benefit of a fast memory storage with the persistence of a disk store.
//
// Principally, SyncDriver 'writes forward' and 'reads backward'.
// Read the documentation for each function for more detail on internal behaviours.
type SyncDriver struct {
	drivers []databank.Driver
}

// NewSync creates a SyncDriver.
func NewSync(drivers ...databank.Driver) *SyncDriver {
	return &SyncDriver{drivers}
}

// Cleanup all expired entries.
//
// SyncDriver works backwards from the authority driver to ensure that front drivers cannot recover data mid-cleanup.
// Errors encountered are aggregated, but do not stop the iterator.
//
// Note that SyncDriver implements this function internally and does not use the Cleanup function of its configured drivers.
//
// TODO This is not performant as driver search [for expired entries] does not work yet.
// This should be improved by substituting Search() for Scan() ASAP.
func (d *SyncDriver) Cleanup() (uint, bool, []error) {
	var deleted uint
	okResult := true
	errors := []error{}

	for i := range d.drivers {
		driver := d.drivers[len(d.drivers)-(i+1)]
		ids, ok, err := driver.Scan()
		if err != nil {
			errors = append(errors, err)
			okResult = false
			continue
		}
		if !ok {
			okResult = false
			continue
		}
		for _, id := range ids {
			e, ok, err := driver.Read(id)
			if err != nil {
				errors = append(errors, err)
				okResult = false
				continue
			}
			if !ok {
				// this just means the ID was not found; ignore
				continue
			}
			if !e.Meta.Expired {
				continue
			}
			ok, err = d.Delete(id)
			if err != nil {
				errors = append(errors, err)
				okResult = false
				continue
			}
			if !ok {
				okResult = false
				continue
			}
			deleted++
		}
	}
	return deleted, okResult, errors
}

// Delete an entry.
//
// SyncDriver works backwards from the authority driver to ensure that front drivers cannot recover data mid-delete.
//
// Errors encountered by any driver do not stop the iterator, but are not aggregated; only the last error will be returned.
// If you need more detail of errors, calling DeleteAggregate() instead will retrieve a separate result for each driver contained in this one.
func (d *SyncDriver) Delete(id string) (bool, error) {
	oks, errors := d.DeleteAggregate(id)
	ok := len(oks) > 0
	for _, driverOK := range oks {
		if !driverOK {
			ok = false
		}
	}
	le := len(errors)
	if le > 0 {
		return ok, errors[le-1]
	}
	return ok, nil
}

// DeleteAggregate deletes an entry, aggregating the status and error responses of each driver.
//
// SyncDriver works backwards from the authority driver to ensure that front drivers cannot recover data mid-delete.
//
// Errors encountered by any driver do not stop the iterator.
// The status and error returned from each driver are aggregated, and returned in a pair of arrays.
// No metadata is provided for the drivers so you will
func (d *SyncDriver) DeleteAggregate(id string) ([]bool, []error) {
	errors := []error{}
	oks := []bool{}
	for i := range d.drivers {
		driver := d.drivers[len(d.drivers)-(i+1)]
		ok, err := driver.Delete(id)
		oks = append(oks, ok)
		errors = append(errors, err)
	}
	return oks, errors
}

// Expire an entry.
//
// SyncDriver writes to each driver sequentially.
// Its bool return reflects whether ALL drivers expired their entry successfully.
// If any driver fails to expire their data, this will be false, even if no error was raised.
//
// Note that SyncDriver implements this function internally and does not use the Expire function of its configured drivers.
func (d *SyncDriver) Expire(id string) (bool, error) {
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
//
// SyncDriver works backwards from the authority driver to ensure that front drivers cannot recover data mid-flush.
// Errors encountered are aggregated, but do not stop the iterator.
func (d *SyncDriver) Flush() (bool, []error) {
	errors := []error{}
	okResult := true
	for i := range d.drivers {
		driver := d.drivers[len(d.drivers)-(i+1)]
		ok, errs := driver.Flush()
		if len(errs) > 0 {
			okResult = false
			for _, err := range errs {
				errors = append(errors, err)
			}
			continue
		}
		if !ok {
			okResult = false
		}
	}
	return okResult, errors
}

// Has an ID, i.e. entry exists in storage?
// Note that an expired entry still 'exists' until it is deleted or flushed out.
//
// SyncDriver is naïve and takes the first positive response, assuming that drivers further back should hold the same ID.
// If it encounters any error, the iterator stops and that error is returned.
func (d *SyncDriver) Has(id string) (bool, error) {
	for _, driver := range d.drivers {
		ok, err := driver.Has(id)
		if err != nil {
			return false, err
		}
		if ok {
			return true, nil
		}
	}
	return false, nil
}

// Read an entry from storage.
//
// SyncDriver tries to read from each driver in sequence until it finds a hit.
// If a hit is found after consulting multiple drivers, the value is silently written back into each driver that failed to read.
// If an error is returned by any driver, the iterator stops and that error is returned.
//
// Errors encountered during writeback are ignored - SyncDriver is naïve and trusts that the prior drivers work, since they didn't return errors the first time.
func (d *SyncDriver) Read(id string) (*databank.Entry, bool, error) {
	failed := []*databank.Driver{}
	var result *databank.Entry
	for _, driver := range d.drivers {
		e, ok, err := driver.Read(id)
		if err != nil {
			return nil, false, err
		}
		if ok {
			result = e
			break
		}
		failed = append(failed, &driver)
	}
	if result == nil {
		return nil, false, nil
	}
	f := len(failed)
	if f > 0 {
		for i := range failed {
			driver := *failed[f-(i+1)]
			driver.Write(result)
		}
	}
	return result, true, nil
}

// Restore entries from storage.
//
// SyncDriver loads all data from the authoritative driver and copies it backward.
// Its bool return reflects whether ALL entries were successfully retrieved and copied.
// Errors encountered are aggregated, but do not stop the iterator.
//
// Usage of this function may not be advisable depending on the size of your data source.
func (d *SyncDriver) Restore() (bool, []error) {
	ids, ok, err := d.Scan()
	if err != nil {
		return false, []error{err}
	}
	if !ok {
		return false, []error{}
	}
	errors := []error{}
	okResult := true
	for _, id := range ids {
		e, ok, err := d.authority().Read(id)
		if err != nil {
			errors = append(errors, err)
			continue
		}
		if !ok {
			okResult = false
			continue
		}
		for i := range d.drivers {
			if i == 0 {
				continue
			}
			driver := d.drivers[len(d.drivers)-(i+1)]
			ok, err := driver.Write(e)
			if err != nil {
				errors = append(errors, err)
			}
			if !ok {
				okResult = false
			}
		}
	}
	return okResult, errors
}

// Review entries, automatically expiring them as necessary.
//
// SyncDriver works backwards from the authority driver to ensure that front drivers cannot recover data mid-review.
// Errors encountered are aggregated, but do not stop the iterator.
//
// Note that SyncDriver implements this function internally and does not use the Review function of its configured drivers.
//
// TODO This is not performant as driver search [for living entries] does not work yet.
// This should be improved by substituting Search() for Scan() ASAP.
func (d *SyncDriver) Review() (uint, bool, []error) {
	var expired uint
	okResult := true
	errors := []error{}

	for i := range d.drivers {
		driver := d.drivers[len(d.drivers)-(i+1)]
		ids, ok, err := driver.Scan()
		if err != nil {
			errors = append(errors, err)
			okResult = false
			continue
		}
		if !ok {
			okResult = false
			continue
		}
		for _, id := range ids {
			e, ok, err := driver.Read(id)
			if err != nil {
				errors = append(errors, err)
				okResult = false
				continue
			}
			if !ok {
				// this just means the ID was not found; ignore
				continue
			}
			if e.MaybeExpire() {
				ok, err = d.Write(e)
				if err != nil {
					errors = append(errors, err)
					okResult = false
					continue
				}
				if !ok {
					okResult = false
					continue
				}
				expired++
			}
		}
	}
	return expired, okResult, errors
}

// Scan for IDs.
//
// SyncDriver goes straight to the authority driver when scanning for IDs.
// All other drivers are by definition not authoritative, and are ignored by this implementation.
func (d *SyncDriver) Scan() ([]string, bool, error) {
	return d.authority().Scan()
}

// Search entries.
//
// TODO
func (d *SyncDriver) Search(q *databank.Query) (map[string]*databank.Entry, bool, error) {
	return map[string]*databank.Entry{}, false, nil
}

// Write an entry to storage.
//
// SyncDriver writes to each driver sequentially.
// Its bool return reflects whether ALL drivers wrote successfully.
// If any driver fails to write, this will be false, even if no error was raised.
//
// If a write error is encountered in any driver, the iterator stops, prior writes are silently rolled back, and that error is returned.
// Errors encountered during rollback are ignored - SyncDriver is naïve and trusts that the prior drivers will work.
func (d *SyncDriver) Write(e *databank.Entry) (bool, error) {
	id := e.ID()
	origE, _, err := d.Read(id)
	if err != nil {
		return false, err
	}

	var errResult error
	okResult := true
	written := []*databank.Driver{}
	for _, driver := range d.drivers {
		ok, err := driver.Write(e)
		if err != nil {
			errResult = err
			okResult = false
			break
		}
		if !ok {
			okResult = false
			continue
		}
		written = append(written, &driver)
	}
	if errResult != nil {
		w := len(written)
		for i := range written {
			driver := *written[w-(i+1)]
			if origE != nil {
				driver.Write(origE)
			} else {
				driver.Delete(id)
			}
		}
	}
	return okResult, errResult
}

// authority driver shorthand.
func (d *SyncDriver) authority() databank.Driver {
	return d.drivers[len(d.drivers)-1]
}
