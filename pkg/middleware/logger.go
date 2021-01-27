package middleware

import (
	"fmt"

	"github.com/edge/databank"
	"github.com/edge/logger"
)

// LoggerMiddleware is a wrapper for Edge logger that implements databank.Middleware.
// This performs simple activity logging, typically to stdout/stderr.
//
// See https://github.com/edge/logger for more information about Edge logger.
type LoggerMiddleware struct {
	l *logger.Instance

	context  string
	severity logger.Severity
}

// NewLoggerMiddleware creates a new LoggerMiddleware.
// It must be preconfigured with a context name and severity.
// These will be used for all logs except errors, for which ERROR severity is forced.
func NewLoggerMiddleware(l *logger.Instance, c string, s logger.Severity) *LoggerMiddleware {
	return &LoggerMiddleware{
		l:        l,
		context:  c,
		severity: s,
	}
}

// Cleanup logs a cleanup operation.
func (d *LoggerMiddleware) Cleanup(next func() (uint, bool, []error)) (uint, bool, []error) {
	n, ok, err := next()
	le := d.l.Context(d.context).Label("event", "cleanup")
	if err != nil {
		le.Error(err)
	} else {
		msg := fmt.Sprintf("%d entries deleted", n)
		d.log(le, ok, msg, "cleanup fail")
	}
	return n, ok, err
}

// Delete logs a delete operation.
func (d *LoggerMiddleware) Delete(id string, next func(id string) (bool, error)) (bool, error) {
	ok, err := next(id)
	le := d.l.Context(d.context).Label("event", "delete").Label("id", id)
	if err != nil {
		le.Error(err)
	} else {
		d.log(le, ok, "ok", "delete fail")
	}
	return ok, err
}

// Expire logs an expire operation.
func (d *LoggerMiddleware) Expire(id string, next func(id string) (bool, error)) (bool, error) {
	ok, err := next(id)
	le := d.l.Context(d.context).Label("event", "expire").Label("id", id)
	if err != nil {
		le.Error(err)
	} else {
		d.log(le, ok, "ok", "expire fail")
	}
	return ok, err
}

// Flush logs a flush operation.
func (d *LoggerMiddleware) Flush(next func() (bool, []error)) (bool, []error) {
	ok, err := next()
	le := d.l.Context(d.context).Label("event", "flush")
	if err != nil {
		le.Error(err)
	} else {
		d.log(le, ok, "flushed", "flush fail")
	}
	return ok, err
}

// Has logs a has operation.
func (d *LoggerMiddleware) Has(id string, next func(id string) (bool, error)) (bool, error) {
	ok, err := next(id)
	le := d.l.Context(d.context).Label("event", "has").Label("id", id)
	if err != nil {
		le.Error(err)
	} else {
		d.log(le, ok, "ok", "has fail")
	}
	return ok, err
}

// Read logs a read operation.
func (d *LoggerMiddleware) Read(id string, next func(id string) (*databank.Entry, bool, error)) (*databank.Entry, bool, error) {
	e, ok, err := next(id)
	le := d.l.Context(d.context).Label("event", "read").Label("id", id)
	if err != nil {
		le.Error(err)
	} else {
		d.log(le, ok, "hit", "miss")
	}
	return e, ok, err
}

// Review logs a review operation.
func (d *LoggerMiddleware) Review(next func() (uint, bool, []error)) (uint, bool, []error) {
	n, ok, err := next()
	le := d.l.Context(d.context).Label("event", "review")
	if err != nil {
		le.Error(err)
	} else {
		msg := fmt.Sprintf("%d entries expired", n)
		d.log(le, ok, msg, "review fail")
	}
	return n, ok, err
}

// Scan logs a scan operation.
func (d *LoggerMiddleware) Scan(next func() ([]string, bool, error)) ([]string, bool, error) {
	ids, ok, err := next()
	le := d.l.Context(d.context).Label("event", "scan")
	if err != nil {
		le.Error(err)
	} else {
		msg := fmt.Sprintf("%d entries found", len(ids))
		d.log(le, ok, msg, "scan fail")
	}
	return ids, ok, err
}

// Search logs a search operation.
func (d *LoggerMiddleware) Search(q *databank.Query, next func(q *databank.Query) (map[string]*databank.Entry, bool, error)) (map[string]*databank.Entry, bool, error) {
	entries, ok, err := next(q)
	le := d.l.Context(d.context).Label("event", "search")
	if err != nil {
		le.Error(err)
	} else {
		msg := fmt.Sprintf("%d entries found", len(entries))
		d.log(le, ok, msg, "scan fail")
	}
	return entries, ok, err
}

// Write logs a write operation.
func (d *LoggerMiddleware) Write(e *databank.Entry, next func(e *databank.Entry) (bool, error)) (bool, error) {
	ok, err := next(e)
	le := d.l.Context(d.context).Label("event", "write").Label("id", e.ID())
	if err != nil {
		le.Error(err)
	} else {
		d.log(le, ok, "ok", "write fail")
	}
	return ok, err
}

// log an entry. Simple convenience method.
func (d *LoggerMiddleware) log(le *logger.Entry, ok bool, okText string, failText string) {
	le.Severity = d.severity
	if ok {
		le.Message = []interface{}{okText}
	} else {
		le.Message = []interface{}{failText}
	}
	d.l.Log(le)
}
