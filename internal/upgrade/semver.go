package upgrade

import (
	"fmt"
	"strconv"
	"strings"
)

// semver represents a parsed semantic version.
type semver struct {
	Major int
	Minor int
	Patch int
	Pre   string // pre-release suffix (e.g. "rc.1", "beta.2")
}

// parseSemver parses a version string like "v1.2.3" or "v1.2.3-rc.1".
// The leading "v" prefix is optional.
func parseSemver(s string) (semver, error) {
	s = strings.TrimPrefix(s, "v")
	if s == "" {
		return semver{}, fmt.Errorf("empty version string")
	}

	var pre string
	if idx := strings.IndexByte(s, '-'); idx >= 0 {
		pre = s[idx+1:]
		s = s[:idx]
	}

	parts := strings.Split(s, ".")
	if len(parts) != 3 {
		return semver{}, fmt.Errorf("invalid semver %q: expected 3 dot-separated numbers", s)
	}

	major, err := strconv.Atoi(parts[0])
	if err != nil {
		return semver{}, fmt.Errorf("invalid major version %q: %w", parts[0], err)
	}

	minor, err := strconv.Atoi(parts[1])
	if err != nil {
		return semver{}, fmt.Errorf("invalid minor version %q: %w", parts[1], err)
	}

	patch, err := strconv.Atoi(parts[2])
	if err != nil {
		return semver{}, fmt.Errorf("invalid patch version %q: %w", parts[2], err)
	}

	return semver{
		Major: major,
		Minor: minor,
		Patch: patch,
		Pre:   pre,
	}, nil
}

// CompareVersions compares two version strings and returns:
//
//	-1 if a < b
//	 0 if a == b
//	 1 if a > b
//
// Stable releases sort higher than pre-releases of the same version
// (e.g. v1.0.0 > v1.0.0-rc.1). Pre-release identifiers are compared
// lexicographically, with numeric segments compared numerically.
func CompareVersions(a, b string) int {
	va, errA := parseSemver(a)
	vb, errB := parseSemver(b)

	// If either parse fails, fall back to string comparison.
	if errA != nil || errB != nil {
		return strings.Compare(a, b)
	}

	if c := compareInts(va.Major, vb.Major); c != 0 {
		return c
	}
	if c := compareInts(va.Minor, vb.Minor); c != 0 {
		return c
	}
	if c := compareInts(va.Patch, vb.Patch); c != 0 {
		return c
	}

	return comparePre(va.Pre, vb.Pre)
}

// comparePre compares pre-release strings.
// A stable release (empty pre) is greater than any pre-release.
func comparePre(a, b string) int {
	switch {
	case a == "" && b == "":
		return 0
	case a == "":
		return 1 // stable > pre-release
	case b == "":
		return -1 // pre-release < stable
	}

	// Split by "." and compare each segment.
	aParts := strings.Split(a, ".")
	bParts := strings.Split(b, ".")

	minLen := len(aParts)
	if len(bParts) < minLen {
		minLen = len(bParts)
	}

	for i := 0; i < minLen; i++ {
		aNum, aIsNum := tryParseInt(aParts[i])
		bNum, bIsNum := tryParseInt(bParts[i])

		switch {
		case aIsNum && bIsNum:
			if c := compareInts(aNum, bNum); c != 0 {
				return c
			}
		case aIsNum:
			return -1 // numeric < string
		case bIsNum:
			return 1 // string > numeric
		default:
			if c := strings.Compare(aParts[i], bParts[i]); c != 0 {
				return c
			}
		}
	}

	// Longer pre-release has more segments = greater.
	return compareInts(len(aParts), len(bParts))
}

func compareInts(a, b int) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

func tryParseInt(s string) (int, bool) {
	n, err := strconv.Atoi(s)
	return n, err == nil
}
