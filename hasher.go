package databank

const (
	offset64 = 14695981039346656037
	prime64  = 1099511628211
)

// sum64 gets the string and returns its uint64 hash value.
func sum64(key string) uint64 {
	var hash uint64 = offset64
	for i := 0; i < len(key); i++ {
		hash ^= uint64(key[i])
		hash *= prime64
	}

	return hash
}
