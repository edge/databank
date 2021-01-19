package atomic

import (
	"testing"
	"time"

	"github.com/edge/databank"
	"github.com/stretchr/testify/assert"
)

const (
	K = "test"
)

func Test_WriteAndRead(t *testing.T) {
	a := assert.New(t)
	c := New(databank.Lifetime{Infinite: true})

	a.Equal(true, c.WriteValue(K, "abc"))
	a.Equal(true, c.Exists(K))
	v, ok := c.ReadValue(K)
	if a.Equal(true, ok) {
		a.Equal("abc", v)
	}
}

func Test_Expiry(t *testing.T) {
	a := assert.New(t)
	c := New(databank.Lifetime{Infinite: true})

	a.Equal(true, c.WriteValue(K, "abc"))
	a.Equal(true, c.Expire(K))
	_, ok := c.ReadValue(K)
	a.Equal(false, ok)

	// secondary test: expiring again should be ignored
	a.Equal(true, c.Expire(K))
}

func Test_TimedExpiry(t *testing.T) {
	a := assert.New(t)
	c := New(databank.Lifetime{Expiry: 100 * time.Millisecond})

	a.Equal(true, c.WriteValue(K, "abc"))
	time.Sleep(200 * time.Millisecond)
	_, ok := c.ReadValue(K)
	a.Equal(false, ok)
}

func Test_Overwrite(t *testing.T) {
	a := assert.New(t)
	c := New(databank.Lifetime{Infinite: true})

	a.Equal(true, c.WriteValue(K, "abc"))
	v, ok := c.ReadValue(K)
	a.Equal(true, ok)
	a.Equal("abc", v)
}

func Test_Delete(t *testing.T) {
	a := assert.New(t)
	c := New(databank.Lifetime{Infinite: true})

	a.Equal(true, c.WriteValue(K, "abc"))
	a.Equal(true, c.Expire(K))
	a.Equal(true, c.Exists(K))
	_, ok := c.ReadValue(K)
	a.Equal(false, ok)
	a.Equal(true, c.Delete(K))
	a.Equal(false, c.Exists(K))
}

func Test_Flush(t *testing.T) {
	a := assert.New(t)
	c := New(databank.Lifetime{Infinite: true})

	a.Equal(true, c.WriteValue(K, "abc"))
	a.Equal(true, c.Flush())
	a.Equal(false, c.Exists(K))
	_, ok := c.ReadValue(K)
	a.Equal(false, ok)
}

func Test_FilterKeys(t *testing.T) {
	a := assert.New(t)
	c := New(databank.Lifetime{Infinite: true})

	a.Equal(true, c.WriteValue(K, "abc"))
	a.Equal(true, c.WriteValue("test2", "def"))
	a.Equal(true, c.WriteValue("test3", "ghi"))
	a.Equal(true, c.Expire(K))

	keys := c.FilterKeys(databank.Filter{
		UseBefore:      false,
		IncludeExpired: true,
		IncludeLive:    true,
	})
	a.Equal(3, len(keys))

	keys = c.FilterKeys(databank.Filter{
		UseBefore:      false,
		IncludeExpired: false,
		IncludeLive:    true,
	})
	a.Equal(2, len(keys))

	keys = c.FilterKeys(databank.Filter{
		UseBefore:      false,
		IncludeExpired: true,
		IncludeLive:    false,
	})
	a.Equal(1, len(keys))

	now := time.Now()
	time.Sleep(200 * time.Millisecond)

	a.Equal(true, c.WriteValue("test4", "jkl"))

	keys = c.FilterKeys(databank.Filter{
		UseBefore:      true,
		Before:         now,
		IncludeExpired: true,
		IncludeLive:    true,
	})
	a.Equal(3, len(keys))
}
