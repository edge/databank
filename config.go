package databank

import "time"

// Config object for a Databank.
type Config struct {
	// Hot Databanks do not automatically expire cache entries on-the-fly; you must set up your own routines to clean them.
	// Default is false, allowing the cache to self-clean and simplify development. In production, you may find that more control is better for performance.
	Hot bool
	// Lifetime of entries. If set to 0 (zero), entries never expire.
	// Default is 0.
	Lifetime time.Duration
}

// NewConfig creates a new config object with sensible defaults.
// ("Sensible" is defined by what little can be inferred without context; e.g. lifetime is assumed to be infinite.)
func NewConfig() *Config {
	return &Config{
		Hot:      false,
		Lifetime: 0,
	}
}
