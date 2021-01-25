package proxy

import (
	"github.com/edge/databank"
)

// SyncDriver is a synchronous proxying implementation of databank.Driver.
// It takes any number of other drivers, and mirrors read/write operations sequentially across them.
//
// The order of drivers is important: they are always accessed from first to last when reading.
// Therefore, you should generally sort the 'more local' drivers first, e.g. memory then disk, rather than the reverse.
// This improves the redundancy and performance of read operations.
type SyncDriver struct {
	drivers []databank.Driver
}

// NewSyncDriver creates an atomic Driver.
func NewSyncDriver(drivers ...databank.Driver) *SyncDriver {
	return &SyncDriver{drivers}
}

// Cleanup all expired entries.
//
// SyncDriver iterates through all drivers in reverse sequence from back to front.
// This ensures that fast front drivers cannot recover values from slower back drivers mid-delete.
//
// Errors encountered are aggregated, but do not stop the iterator.
//
// Note that due to implementation requirements, SyncDriver implements this function internally and does not use the Cleanup function of its configured drivers.
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
// SyncDriver iterates through all drivers in reverse sequence from back to front.
// This ensures that fast front drivers cannot recover values from slower back drivers mid-delete.
//
// Errors encountered by any driver do not stop the iterator, but due to type constraints are not aggregated; only the last error will be returned.
func (d *SyncDriver) Delete(id string) (bool, error) {
	var errResult error
	okResult := true
	for i := range d.drivers {
		driver := d.drivers[len(d.drivers)-(i+1)]
		ok, err := driver.Delete(id)
		if err != nil {
			errResult = err
			okResult = false
		}
		if !ok {
			okResult = false
		}
	}
	return okResult, errResult
}

// Expire an entry.
//
// SyncDriver writes to each driver sequentially.
// Its bool return reflects whether ALL drivers expired their entry successfully.
// If any driver fails to expire their data, this will be false, even if no error was raised.
//
// Note that due to implementation requirements, SyncDriver implements this function internally and does not use the Expire function of its configured drivers.
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
// SyncDriver flushes all drivers in reverse sequence from back to front.
// This ensures that fast front drivers cannot recover values from slower back drivers mid-flush.
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
// SyncDriver is naïve and takes the first positive response, assuming that subsequent drivers should hold the same ID.
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

// Read a data entry by its key.
//
// SyncDriver tries to read from each driver in sequence until it finds a hit.
// If a hit is found after consulting multiple drivers, the value is silently written back into each driver that failed to read.
// Errors are encountered during writeback are ignored - SyncDriver is naïve and trusts that the prior drivers will work.
//
// If an error is returned by any driver, the iterator stops and that error is returned.
func (d *SyncDriver) Read(id string) (*databank.Entry, bool, error) {
	failed := []*databank.Driver{}
	var result *databank.Entry
	var errResult error
	for _, driver := range d.drivers {
		e, ok, err := driver.Read(id)
		if err != nil {
			errResult = err
			break
		}
		if ok {
			result = e
			break
		}
		failed = append(failed, &driver)
	}
	if errResult != nil {
		return nil, false, errResult
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
// TODO
func (d *SyncDriver) Restore() (bool, error) {
	return false, nil
}

// Review entries, automatically expiring them as necessary.
//
// SyncDriver iterates through all drivers in reverse sequence from back to front.
// This ensures that fast front drivers cannot recover values from slower back drivers mid-review.
//
// Errors encountered are aggregated, but do not stop the iterator.
//
// Note that due to implementation requirements, SyncDriver implements this function internally and does not use the Review function of its configured drivers.
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
// SyncDriver goes straight to the back of the queue when scanning for IDs.
// The last driver configured is considered to be authoritative, as it is where data is ultimately recovered from if all prior drivers fail.
// For example, a cold cache might be restored from a disk driver configured as backend.
// All other drivers in between the caller and the last cache are by definition not authoritative, and so are ignored by this implementation.
func (d *SyncDriver) Scan() ([]string, bool, error) {
	last := d.drivers[len(d.drivers)-1]
	return last.Scan()
}

// Search entries.
//
// TODO
func (d *SyncDriver) Search(q *databank.Query) (map[string]*databank.Entry, bool, error) {
	return map[string]*databank.Entry{}, false, nil
}

// Write a data entry with a key.
//
// SyncDriver writes to each driver sequentially.
// Its bool return reflects whether ALL drivers wrote successfully.
// If any driver fails to write, this will be false, even if no error was raised.
//
// If a write error is encountered in any driver, the iterator stops, prior writes are silently rolled back, and that error is returned.
// Errors are encountered during rollback are ignored - SyncDriver is naïve and trusts that the prior drivers will work.
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
		if ok {
			written = append(written, &driver)
		} else {
			okResult = false
		}
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
