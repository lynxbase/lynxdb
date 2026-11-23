#!/bin/sh
# usage: dispatch.sh new|patch [--ref <ref>] [--lts] [--prestable] [--dry-run]
# Starts the release workflow. A new line is cut from main unless --ref says
# otherwise; a patch names the branch of its line with --ref.
set -eu

cd "$(dirname -- "$0")/../.."

kind=${1:-}
[ $# -eq 0 ] || shift
ref=
channel=default
dry_run=false
while [ $# -gt 0 ]; do
    case $1 in
    --ref)
        [ $# -ge 2 ] || {
            echo "dispatch.sh: --ref needs a value" >&2
            exit 2
        }
        ref=$2
        shift
        ;;
    --lts) channel=lts ;;
    --prestable) channel=prestable ;;
    --dry-run) dry_run=true ;;
    *)
        echo "dispatch.sh: unknown argument: $1" >&2
        exit 2
        ;;
    esac
    shift
done

case $kind in
new) ref=${ref:-main} ;;
patch)
    [ -n "$ref" ] || {
        echo "dispatch.sh: a patch needs --ref <line branch>, for example --ref 26.11" >&2
        exit 2
    }
    ;;
*)
    echo "usage: dispatch.sh new|patch [--ref <ref>] [--lts] [--prestable] [--dry-run]" >&2
    exit 2
    ;;
esac

# What GitHub has is what gets released, so the preview reads it too.
git fetch --quiet --tags origin
target=$ref
! git rev-parse --verify --quiet "origin/$ref^{commit}" >/dev/null || target=origin/$ref
set -- next "$kind" --ref "$target"
[ "$channel" = default ] || set -- "$@" --channel "$channel"
tools/release/version.sh "$@"

gh workflow run release.yml -f type="$kind" -f ref="$ref" -f channel="$channel" \
    -f dry_run="$dry_run"
echo "started; follow it with: gh run watch"
