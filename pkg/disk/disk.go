package disk

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/edge/databank"
)

// Config for disk Driver.
type Config struct {
	DirMode os.FileMode
	Path    string
}

// driver is the disk implementation of databank.Driver.
type driver struct {
	config *Config
}

// New databank with a disk Driver backend.
func New(c *databank.Config, dc *Config) databank.Databank {
	return databank.New(NewDriver(dc), c)
}

// NewConfig creates a disk Driver configuration with sensible defaults.
func NewConfig(path string) *Config {
	return &Config{
		DirMode: 0755,
		Path:    path,
	}
}

// NewDriver creates a disk Driver.
func NewDriver(c *Config) databank.Driver {
	return &driver{
		config: c,
	}
}

// Cleanup all expired entries.
//
// TODO improve performance
func (d *driver) Cleanup() (uint, bool, []error) {
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

// Delete a data entry by key.
func (d *driver) Delete(id string) (bool, error) {
	ok, err := d.Has(id)
	if err != nil {
		return false, err
	}
	if !ok {
		return true, nil
	}
	fn := d.idfilepath(id)
	err = os.Remove(fn)
	return err == nil, err
}

// // Expire a data entry by key.
func (d *driver) Expire(id string) (bool, error) {
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

// // Flush all entries.
func (d *driver) Flush() (bool, []error) {
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

// // Has an ID, i.e. entry exists in storage?
// // Note that an expired entry still 'exists' until it is deleted or flushed out.
func (d *driver) Has(id string) (bool, error) {
	stat, err := os.Stat(d.idfilepath(id))
	return !os.IsNotExist(err) || stat != nil && !stat.IsDir(), err
}

// Read a data entry by its key.
//
// TODO change stuff to use more pointers for Entry storage
func (d *driver) Read(id string) (*databank.Entry, bool, error) {
	if ok, err := d.Has(id); !ok {
		return nil, ok, err
	}
	b, err := ioutil.ReadFile(d.idfilepath(id))
	if err != nil {
		return nil, false, err
	}
	e := databank.NewEntry("", 0)
	if err := json.Unmarshal(b, e); err != nil {
		return nil, false, err
	}
	return e, true, err
}

func (d *driver) Restore() (bool, error) {
	ids, ok, err := d.Scan()
	if err != nil {
		return false, err
	}
	if ok {
		for _, id := range ids {
			// TODO can improve reporting
			d.Read(id)
		}
	}
	return ok, nil
}

// Review entries, automatically expiring them as necessary.
func (d *driver) Review() (uint, bool, []error) {
	var expired uint
	errs := []error{}
	ids, ok, err := d.Scan()
	if err != nil {
		return expired, false, []error{err}
	}
	if ok {
		fmt.Println(ids)
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

// TODO
func (d *driver) Scan() ([]string, bool, error) {
	keys := []string{}
	err := filepath.Walk(d.config.Path, func(path string, info os.FileInfo, err error) error {
		keys = append(keys, strings.Replace(path, d.config.Path, "", 1))
		return nil
	})
	return keys, true, err
}

// Search entries.
//
// TODO
func (d *driver) Search(q *databank.Query) (map[string]*databank.Entry, bool, error) {
	return map[string]*databank.Entry{}, false, nil
}

// Write a data entry with a key.
func (d *driver) Write(e *databank.Entry) (bool, error) {
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

// idfilepath gets the storage path on disk for an Entry.
func (d *driver) filepath(e *databank.Entry) string {
	return d.idfilepath(e.ID())
}

// idfilepath gets the storage path on disk for an ID.
func (d *driver) idfilepath(id string) string {
	return path.Join(d.config.Path, filesafe(id))
}

func (d *driver) open(e *databank.Entry) (*os.File, error) {
	stat, err := os.Stat(d.config.Path)
	if os.IsNotExist(err) || stat == nil {
		if err := os.MkdirAll(d.config.Path, d.config.DirMode); err != nil {
			return nil, err
		}
	} else if !stat.IsDir() {
		return nil, fmt.Errorf("%s is a file", d.config.Path)
	}
	return os.Create(d.filepath(e))
}

// filesafe provides a one-way transformation from an ID (which shouldn't, but may, contain invalid characters for a filename) to a path-safe filename.
func filesafe(id string) string {
	// TODO actually make it filesafe!
	return id
}
