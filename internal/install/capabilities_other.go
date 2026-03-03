//go:build !linux

package install

// setCapabilities is a no-op on non-Linux platforms.
// Linux capabilities (setcap) are not available on macOS, FreeBSD, or Windows.
func setCapabilities(_ string) (string, error) {
	return "(skipped, not Linux)", nil
}
