//go:build !linux

package install

// configureUlimits is a no-op on non-Linux platforms.
// Linux-specific limits.d files are not applicable on macOS, FreeBSD, or Windows.
func configureUlimits(_ Options) (string, error) {
	return "(skipped, not Linux)", nil
}
