package databank

import (
	"crypto/sha1"
	"fmt"
	"time"
)

// Entry represents a single data entry and its metadata.
type Entry struct {
	Content []byte            `json:"content"`
	Key     string            `json:"key"`
	Size    uint64            `json:"size"`
	Tags    map[string]string `json:"tags"`

	Meta *EntryMetadata `json:"meta"`
}

// EntryMetadata TODO
type EntryMetadata struct {
	Created      time.Time `json:"created"`
	Expires      time.Time `json:"expires"`
	ExpiresNever bool      `json:"expiresNever"`
	Expired      bool      `json:"expired"`
}

// NewEntry returns an empty Entry with required key and metadata set.
// This can be used by Driver implementations to simplify reproduction of the Entry struct in read operations.
func NewEntry(key string, ttl time.Duration) *Entry {
	now := time.Now()
	expires := now.Add(ttl)
	meta := &EntryMetadata{
		Created:      now,
		Expires:      expires,
		ExpiresNever: ttl == 0,
		Expired:      false,
	}

	return &Entry{
		Content: []byte{},
		Key:     key,
		Size:    0,
		Tags:    map[string]string{},

		Meta: meta,
	}
}

// EntryID calculates an entry's ID.
//
// When tags are set in an entry, they are hashed into the entry's ID.
// If tags are not set, the entry's key is used as-is.
//
// This means that if your entry is not tagged, your calling code does not need to calculate anything i.e.
//
//   // Given var d databank.Databank
//   val, ok := d.ReadString("mykey")
//
// However, if you use any tags or just for safety, you can use EntryID to simplify things:
//
//   val, ok := d.ReadString(databank.EntryID("mykey", map[string]string{"abc": "def"}))
//
// If you have an Entry already, its ID() function produces the same result.
func EntryID(key string, tags map[string]string) string {
	if len(tags) > 0 {
		id := fmt.Sprintf("%s?", key)
		for k, v := range tags {
			id = fmt.Sprintf("%s&%s=%s", id, k, v)
		}
		return hash(id)
	}
	return key
}

// Expire marks the entry expired.
func (e *Entry) Expire() {
	e.Meta.Expired = true
}

// ID calculated for the entry.
func (e *Entry) ID() string {
	return EntryID(e.Key, e.Tags)
}

// Lifetime of entry.
func (e *Entry) Lifetime() time.Duration {
	return e.Meta.Expires.Sub(e.Meta.Created)
}

// MaybeExpire automatically expires the entry if it ShouldExpire().
func (e *Entry) MaybeExpire() bool {
	se := e.ShouldExpire()
	if se {
		e.Expire()
	}
	return se
}

// ShouldExpire quickly checks whether the entry should presently expire.
func (e *Entry) ShouldExpire() bool {
	if e.Meta.Expired || e.Meta.ExpiresNever {
		return false
	}
	return e.Meta.Expires.Before(time.Now())
}

// Touch the entry (renew its life). It will keep the same TTL that it had previously.
func (e *Entry) Touch() {
	e.Meta.Created = time.Now()
	e.Meta.Expires = e.Meta.Created.Add(e.Lifetime())
}

func hash(id string) string {
	sum := sha1.Sum([]byte(id))
	return string(sum[0:20])
}
