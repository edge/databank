package disk

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"regexp"

	"github.com/edge/databank"
)

// Config for disk Driver.
type Config struct {
	DirMode os.FileMode
	Path    string
}

// Driver is the disk implementation of databank.Driver.
type Driver struct {
	config *Config
}

var filesafeRegexp = regexp.MustCompile("[^A-z0-9\\-\\_\\.]")

// New creates a disk Driver.
func New(c *Config) *Driver {
	return &Driver{
		config: c,
	}
}

// NewConfig creates a disk Driver configuration with sensible defaults.
func NewConfig(path string) *Config {
	return &Config{
		DirMode: 0755,
		Path:    path,
	}
}

// Cleanup all expired entries.
//
// TODO improve performance
func (d *Driver) Cleanup() (uint, bool, []error) {
	var deleted uint
	errs := []error{}
	ids, ok, err := d.Scan()
	if err != nil {
		return deleted, false, []error{err}
	}
	if ok {
		for _, id := range ids {
			// TODO can improve reporting
			e, ok2, err := d.Read(id)
			if err != nil {
				errs = append(errs, err)
			}
			if !ok2 {
				continue
			}
			if e.Meta.Expired {
				ok3, err := d.Delete(e.ID())
				if err != nil {
					errs = append(errs, err)
				}
				if ok3 {
					deleted++
				}
			}
		}
	}
	return deleted, ok, errs
}

// Delete an entry.
// The bool return reflects the entry's nonexistence in storage when this function returns.
// Ergo, if the ID is not found, this function still returns true.
func (d *Driver) Delete(id string) (bool, error) {
	ok, err := d.Has(id)
	if err != nil {
		return false, err
	}
	if !ok {
		return true, nil
	}
	fn := d.FilepathByID(id)
	err = os.Remove(fn)
	return err == nil, err
}

// Expire an entry.
// The bool return reflects whether the entry is in an expired or otherwise unreachable state when this function returns.
// Ergo, if the ID is not found, this function still returns true.
func (d *Driver) Expire(id string) (bool, error) {
	e, ok, err := d.Read(id)
	if err != nil {
		return false, err
	}
	if !ok {
		return true, nil
	}
	e.Expire()
	return d.Write(e)
}

// Filepath gets the storage path on disk for an Entry.
func (d *Driver) Filepath(e *databank.Entry) string {
	return d.FilepathByID(e.ID())
}

// FilepathByID gets the storage path on disk for an ID.
func (d *Driver) FilepathByID(id string) string {
	return path.Join(d.config.Path, filesafe(id))
}

// Flush all entries.
func (d *Driver) Flush() (bool, []error) {
	ids, ok, err := d.Scan()
	if err != nil {
		return ok, []error{err}
	}
	errs := []error{}
	for _, id := range ids {
		// TODO can improve reporting
		_, err := d.Delete(id)
		if err != nil {
			errs = append(errs, err)
		}
	}
	return true, errs
}

// Has an ID, i.e. entry exists in storage?
// Note that an expired entry still 'exists' until it is deleted or flushed out.
func (d *Driver) Has(id string) (bool, error) {
	f := d.FilepathByID(id)
	stat, err := os.Stat(f)
	if os.IsNotExist(err) {
		return false, nil
	} else if err != nil {
		return false, err
	}
	if !stat.Mode().IsRegular() {
		return false, fmt.Errorf("%s is not a regular file", filepath.Base(f))
	}
	return true, nil
}

// Read an entry from storage.
func (d *Driver) Read(id string) (*databank.Entry, bool, error) {
	if ok, err := d.Has(id); !ok {
		return nil, ok, err
	}
	f := d.FilepathByID(id)
	b, err := ioutil.ReadFile(f)
	if err != nil {
		return nil, false, err
	}
	if os.IsNotExist(err) {
		return nil, false, nil
	} else if err != nil {
		return nil, false, err
	}
	e := databank.NewEntry("", 0)
	if err := json.Unmarshal(b, e); err != nil {
		return nil, false, err
	}
	return e, true, err
}

// Review entries, automatically expiring them as necessary.
func (d *Driver) Review() (uint, bool, []error) {
	var expired uint
	errs := []error{}
	ids, ok, err := d.Scan()
	if err != nil {
		return expired, false, []error{err}
	}
	if ok {
		for _, id := range ids {
			// TODO can improve reporting
			e, ok2, err := d.Read(id)
			if err != nil {
				errs = append(errs, err)
				continue
			}
			if !ok2 {
				continue
			}
			if e.MaybeExpire() {
				ok3, err := d.Write(e)
				if err != nil {
					errs = append(errs, err)
				}
				if ok3 {
					expired++
				}
			}
		}
	}
	return expired, ok, errs
}

// Scan for IDs.
func (d *Driver) Scan() ([]string, bool, error) {
	keys := []string{}
	err := filepath.Walk(d.config.Path, func(path string, info os.FileInfo, err error) error {
		if info.Mode().IsRegular() {
			keys = append(keys, filepath.Base(path))
		}
		return nil
	})
	return keys, true, err
}

// Search entries.
//
// TODO
func (d *Driver) Search(q *databank.Query) (map[string]*databank.Entry, bool, error) {
	return map[string]*databank.Entry{}, false, nil
}

// Write an entry to storage.
func (d *Driver) Write(e *databank.Entry) (bool, error) {
	file, err := d.open(e)
	if err != nil {
		return false, err
	}
	defer file.Close()
	b, err := json.Marshal(e)
	if err != nil {
		return false, err
	}
	n, err := file.Write(b)
	if err != nil {
		return false, err
	}
	lb := len(b)
	if n < lb {
		err := fmt.Errorf("Written length %d does not match data length %d", n, lb)
		return false, err
	}
	return true, nil
}

// open a file for writing.
func (d *Driver) open(e *databank.Entry) (*os.File, error) {
	stat, err := os.Stat(d.config.Path)
	if os.IsNotExist(err) {
		if err := os.MkdirAll(d.config.Path, d.config.DirMode); err != nil {
			return nil, err
		}
	} else {
		if stat.Mode().IsRegular() {
			return nil, fmt.Errorf("%s is a file", d.config.Path)
		}
		if !stat.IsDir() {
			return nil, fmt.Errorf("%s is not a directory", d.config.Path)
		}
	}
	return os.Create(d.Filepath(e))
}

// filesafe provides a one-way transformation from an ID to a path-safe filename.
// IDs are not expected to contain unsafe characters, but better safe than sorry.
//
// This transformation can increase the likelihood of filename conflicts if you use an unusual naming convention.
// To prevent this, don't use an unusual naming convention.
func filesafe(id string) string {
	safe := filesafeRegexp.ReplaceAll([]byte(id), []byte("_"))
	return string(safe)
}
