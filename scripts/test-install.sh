#!/bin/sh
# Lint checks for scripts/install.sh
# Verifies POSIX sh compatibility (no bashisms).

set -e

echo "Checking install.sh syntax..."
sh -n scripts/install.sh && echo "OK: Syntax valid"

echo ""
echo "Checking for bashisms..."
fail=0

# Check for [[ ]] conditionals (exclude POSIX [[:class:]] in sed/grep patterns)
if grep -n '\[\[' scripts/install.sh | grep -v '\[\[:' | grep -v '^\s*#'; then
    echo "FAIL: bashism [[ found"
    fail=1
fi

if grep -n '^[^#]*function ' scripts/install.sh; then
    echo "FAIL: bashism 'function' keyword found"
    fail=1
fi

if grep -n '^[^#]*declare ' scripts/install.sh; then
    echo "FAIL: bashism 'declare' found"
    fail=1
fi

# Note: 'local' is technically a bashism but is supported by dash/ash,
# so we don't flag it. The install script avoids it anyway.

if [ "$fail" -ne 0 ]; then
    exit 1
fi

echo "OK: No bashisms detected"
