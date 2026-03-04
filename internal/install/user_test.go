package install

import (
	"os/user"
	"testing"
)

func TestUserExists(t *testing.T) {
	// The current user should always exist.
	u, err := user.Current()
	if err != nil {
		t.Fatalf("test requires current user: %v", err)
	}

	if !userExists(u.Username) {
		t.Errorf("userExists(%q) = false, want true for current user", u.Username)
	}

	// A non-existent user should return false.
	if userExists("lynxdb_nonexistent_test_user_12345") {
		t.Error("userExists(nonexistent) = true, want false")
	}
}

func TestUserGroupExists(t *testing.T) {
	// Try to find a group that should exist on most systems.
	// On macOS "staff" is common; on Linux "root" or "wheel".
	knownGroups := []string{"staff", "root", "wheel", "users"}
	found := false

	for _, g := range knownGroups {
		if userGroupExists(g) {
			found = true

			break
		}
	}

	if !found {
		t.Fatal("test requires at least one known group (staff/root/wheel/users)")
	}

	if userGroupExists("lynxdb_nonexistent_test_group_12345") {
		t.Error("userGroupExists(nonexistent) = true, want false")
	}
}
