package stats

// EphemeralMemoryLimit returns a sensible memory limit for ephemeral (CLI pipe/file)
// mode queries. It returns half of the detected system memory, giving the query
// engine room to work while leaving headroom for the OS and other processes.
//
// Returns 0 if system memory detection fails (caller should treat as "unlimited").
func EphemeralMemoryLimit() int64 {
	total := TotalSystemMemory()
	if total <= 0 {
		return 0
	}

	return total / 2
}
