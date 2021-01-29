package databank

// Middleware for databank can trigger side effects on operations, such as logging or metrics tracking.
// It can even manipulate arguments and return values.
//
// All functions receive a 'next' argument that should be called at some point to progress deeper into the call stack.
// If the next function is not called, the driver and any deeper middleware will not be invoked.
// The simplest (and least useful) middleware implementation looks like this:
//
//   func (*MyMiddleware) Read(id string, next func(id string) (*Entry, bool, error)) (*Entry, bool, error) {
//     return next(id)
//   }
//
// Middleware is powerful and should be used with care!
type Middleware interface {
	Cleanup(next func() (uint, bool, []error)) (uint, bool, []error)
	Count(next func() (uint, bool, error)) (uint, bool, error)
	Delete(id string, next func(id string) (bool, error)) (bool, error)
	Expire(id string, next func(id string) (bool, error)) (bool, error)
	Flush(next func() (bool, []error)) (bool, []error)
	Has(id string, next func(id string) (bool, error)) (bool, error)
	Read(id string, next func(id string) (*Entry, bool, error)) (*Entry, bool, error)
	Review(next func() (uint, bool, []error)) (uint, bool, []error)
	Scan(next func() ([]string, bool, error)) ([]string, bool, error)
	Search(q *Query, next func(q *Query) (map[string]*Entry, bool, error)) (map[string]*Entry, bool, error)
	Write(e *Entry, next func(e *Entry) (bool, error)) (bool, error)
}

// masterMiddleware is used internally by databank to structure middleware call paths.
// See installMiddleware() for more detail.
type masterMiddleware struct {
	Cleanup func() (uint, bool, []error)
	Count   func() (uint, bool, error)
	Delete  func(id string) (bool, error)
	Expire  func(id string) (bool, error)
	Flush   func() (bool, []error)
	Has     func(id string) (bool, error)
	Read    func(id string) (*Entry, bool, error)
	Review  func() (uint, bool, []error)
	Scan    func() ([]string, bool, error)
	Search  func(q *Query) (map[string]*Entry, bool, error)
	Write   func(e *Entry) (bool, error)
}

// installMiddleware configures the databank's middleware object.
// It can take any number of middlewares in the setup, including zero.
//
// This is a private function normally called from New() and should not be used anywhere else.
func (d *databank) installMiddleware(middlewares []Middleware) {
	cleanup := []func() (uint, bool, []error){d.driver.Cleanup}
	count := []func() (uint, bool, error){d.driver.Count}
	delete := []func(id string) (bool, error){d.driver.Delete}
	expire := []func(id string) (bool, error){d.driver.Expire}
	flush := []func() (bool, []error){d.driver.Flush}
	has := []func(id string) (bool, error){d.driver.Has}
	read := []func(id string) (*Entry, bool, error){d.driver.Read}
	review := []func() (uint, bool, []error){d.driver.Review}
	scan := []func() ([]string, bool, error){d.driver.Scan}
	search := []func(q *Query) (map[string]*Entry, bool, error){d.driver.Search}
	write := []func(e *Entry) (bool, error){d.driver.Write}

	lm := len(middlewares)
	for i := 0; i < lm; i++ {
		m := middlewares[i]
		lcleanup := cleanup[i]
		cleanup = append(cleanup, func() (uint, bool, []error) {
			return m.Cleanup(lcleanup)
		})
		lcount := count[i]
		count = append(count, func() (uint, bool, error) {
			return m.Count(lcount)
		})
		ldelete := delete[i]
		delete = append(delete, func(id string) (bool, error) {
			return m.Delete(id, ldelete)
		})
		lexpire := expire[i]
		expire = append(expire, func(id string) (bool, error) {
			return m.Expire(id, lexpire)
		})
		lflush := flush[i]
		flush = append(flush, func() (bool, []error) {
			return m.Flush(lflush)
		})
		lhas := has[i]
		has = append(has, func(id string) (bool, error) {
			return m.Has(id, lhas)
		})
		lread := read[i]
		read = append(read, func(id string) (*Entry, bool, error) {
			return m.Read(id, lread)
		})
		lreview := review[i]
		review = append(review, func() (uint, bool, []error) {
			return m.Review(lreview)
		})
		lscan := scan[i]
		scan = append(scan, func() ([]string, bool, error) {
			return m.Scan(lscan)
		})
		lsearch := search[i]
		search = append(search, func(q *Query) (map[string]*Entry, bool, error) {
			return m.Search(q, lsearch)
		})
		lwrite := write[i]
		write = append(write, func(e *Entry) (bool, error) {
			return m.Write(e, lwrite)
		})
	}

	n := len(cleanup) - 1
	d.middleware = &masterMiddleware{
		Cleanup: cleanup[n],
		Count:   count[n],
		Delete:  delete[n],
		Expire:  expire[n],
		Flush:   flush[n],
		Has:     has[n],
		Read:    read[n],
		Review:  review[n],
		Scan:    scan[n],
		Search:  search[n],
		Write:   write[n],
	}
}
