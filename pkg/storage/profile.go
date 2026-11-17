package storage

// Profile determines the storage behavior based on configuration.
type Profile int

const (
	// Ephemeral: no persistence. DataDir="", segments in memory, cleanup on Close.
	Ephemeral Profile = iota

	// Persistent: local disk persistence. DataDir set, direct-to-part persistence.
	Persistent
)

// String returns the human-readable name of the storage profile.
func (p Profile) String() string {
	switch p {
	case Ephemeral:
		return "ephemeral"
	case Persistent:
		return "persistent"
	default:
		return "unknown"
	}
}

// ResolveProfile determines the storage profile from the configuration.
func ResolveProfile(dataDir string) Profile {
	if dataDir == "" {
		return Ephemeral
	}

	return Persistent
}

// ProfileFeatures describes which storage subsystems are enabled for a profile.
type ProfileFeatures struct {
	PartWriter bool // direct-to-part persistence
	Compaction bool // background compaction
	Cache      bool // query result cache
}

// Features returns the enabled subsystems for the given profile.
func Features(profile Profile) ProfileFeatures {
	switch profile {
	case Ephemeral:
		return ProfileFeatures{
			PartWriter: false,
			Compaction: false,
			Cache:      true, // cache still useful for repeated queries
		}
	case Persistent:
		return ProfileFeatures{
			PartWriter: true,
			Compaction: true,
			Cache:      true,
		}
	default:
		return ProfileFeatures{}
	}
}
