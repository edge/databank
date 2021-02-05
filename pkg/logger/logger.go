package logger

import (
	"fmt"
	"strings"

	"github.com/edge/databank"
	"github.com/edge/logger"
)

// Middleware is a logging middleware that wraps another driver.
// This performs simple activity logging, typically to stdout/stderr.
//
// See https://github.com/edge/logger for more information about Edge logger.
type Middleware struct {
	l    *logger.Instance
	next databank.Driver

	context  string
	severity logger.Severity
}

// NewMiddleware creates a new logging Middleware.
// It must be preconfigured with a context name and severity.
// INFO, DEBUG or TRACE is suggested, depending on your requirements.
// This context and severity will be used for all logs except errors, for which ERROR severity is forced.
func NewMiddleware(c string, s logger.Severity, l *logger.Instance, next databank.Driver) *Middleware {
	return &Middleware{
		l:    l,
		next: next,

		context:  c,
		severity: s,
	}
}

// Cleanup logs a cleanup operation.
func (d *Middleware) Cleanup() (uint, bool, []error) {
	n, ok, errs := d.next.Cleanup()
	le := d.l.Context(d.context).Label("operation", "cleanup")
	d.log(le, d.flatten(errs), ok, fmt.Sprintf("%d entries deleted", n), "cleanup fail")
	return n, ok, errs
}

// Count logs a count operation.
func (d *Middleware) Count() (uint, bool, error) {
	n, ok, err := d.next.Count()
	le := d.l.Context(d.context).Label("operation", "count")
	d.log(le, err, ok, fmt.Sprintf("counted %d entries", n), "count fail")
	return n, ok, err
}

// Delete logs a delete operation.
func (d *Middleware) Delete(id string) (bool, error) {
	ok, err := d.next.Delete(id)
	le := d.l.Context(d.context).Label("operation", "delete").Label("id", id)
	d.log(le, err, ok, "ok", "delete fail")
	return ok, err
}

// Expire logs an expire operation.
func (d *Middleware) Expire(id string) (bool, error) {
	ok, err := d.next.Expire(id)
	le := d.l.Context(d.context).Label("operation", "expire").Label("id", id)
	d.log(le, err, ok, "ok", "expire fail")
	return ok, err
}

// Flush logs a flush operation.
func (d *Middleware) Flush() (bool, []error) {
	ok, errs := d.next.Flush()
	le := d.l.Context(d.context).Label("operation", "flush")
	d.log(le, d.flatten(errs), ok, "flushed", "flush fail")
	return ok, errs
}

// Has logs a has operation.
func (d *Middleware) Has(id string) (bool, error) {
	ok, err := d.next.Has(id)
	le := d.l.Context(d.context).Label("operation", "has").Label("id", id)
	d.log(le, err, ok, "hit", "miss")
	return ok, err
}

// Read logs a read operation.
func (d *Middleware) Read(id string) (*databank.Entry, bool, error) {
	e, ok, err := d.next.Read(id)
	le := d.l.Context(d.context).Label("operation", "read").Label("id", id)
	d.log(le, err, ok, "hit", "miss")
	return e, ok, err
}

// Review logs a review operation.
func (d *Middleware) Review() (uint, bool, []error) {
	n, ok, errs := d.next.Review()
	le := d.l.Context(d.context).Label("operation", "review")
	d.log(le, d.flatten(errs), ok, fmt.Sprintf("%d entries expired", n), "review fail")
	return n, ok, errs
}

// Scan logs a scan operation.
func (d *Middleware) Scan() ([]string, bool, error) {
	ids, ok, err := d.next.Scan()
	le := d.l.Context(d.context).Label("operation", "scan")
	d.log(le, err, ok, fmt.Sprintf("%d entries found", len(ids)), "scan fail")
	return ids, ok, err
}

// Search logs a search operation.
func (d *Middleware) Search(q *databank.Query) (map[string]*databank.Entry, bool, error) {
	entries, ok, err := d.next.Search(q)
	le := d.l.Context(d.context).Label("operation", "search")
	d.log(le, err, ok, fmt.Sprintf("%d entries found", len(entries)), "scan fail")
	return entries, ok, err
}

// Write logs a write operation.
func (d *Middleware) Write(e *databank.Entry) (bool, error) {
	ok, err := d.next.Write(e)
	le := d.l.Context(d.context).Label("operation", "write").Label("id", e.ID())
	d.log(le, err, ok, "ok", "write fail")
	return ok, err
}

// flatten an array of errors into one, while keeping all their messages in the original sequence.
func (d *Middleware) flatten(errs []error) error {
	if len(errs) == 0 {
		return nil
	}
	strs := []string{}
	for _, err := range errs {
		strs = append(strs, fmt.Sprintf("%s", err))
	}
	return fmt.Errorf("%d errors: %s", len(strs), strings.Join(strs, "; "))
}

// log an entry. Simple convenience method.
func (d *Middleware) log(le *logger.Entry, err error, ok bool, okText string, failText string) {
	if err != nil {
		le.Severity = logger.Error
		le.Message = []interface{}{fmt.Sprint(err)}
	} else {
		le.Severity = d.severity
		if ok {
			le.Message = []interface{}{okText}
		} else {
			le.Message = []interface{}{failText}
		}
	}
	d.l.Log(le)
}
