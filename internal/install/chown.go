package install

import (
	"os"
	"os/user"
	"strconv"
)

// chownByName changes the owner and group of a path by name.
// Best-effort: silently ignores errors (e.g. user/group not found,
// permission denied). This avoids failing the entire install when
// chown is not critical (e.g. macOS user installs).
func chownByName(path, userName, groupName string) {
	uid := -1
	gid := -1

	if u, err := user.Lookup(userName); err == nil {
		if id, err := strconv.Atoi(u.Uid); err == nil {
			uid = id
		}
	}

	if g, err := user.LookupGroup(groupName); err == nil {
		if id, err := strconv.Atoi(g.Gid); err == nil {
			gid = id
		}
	}

	if uid >= 0 || gid >= 0 {
		_ = os.Chown(path, uid, gid) //nolint:errcheck
	}
}
