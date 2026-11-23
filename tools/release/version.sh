#!/bin/sh
# usage: version.sh current [<ref>]
#        version.sh next new|patch [--ref <ref>] [--channel stable|lts|prestable]
#                                  [--date <YYYY-MM>]
#
# A release is tagged v<YY>.<M>.<patch>.<build>-<channel>. <YY>.<M> names the
# line, after the month it left main. <build> counts first-parent commits
# since the release it follows.
set -eu

tag_pattern='^v[0-9]{2}\.([1-9]|1[0-2])\.[1-9][0-9]*\.[1-9][0-9]*-(stable|lts|prestable)$'

die()
{
    printf 'version.sh: %s\n' "$*" >&2
    exit 1
}

# A prestable tag marks a candidate, so nothing is counted from it.
released_tags()
{
    git tag --list "${1:-v[0-9]*}" | grep -E "$tag_pattern" | grep -v -- '-prestable$' || true
}

sort_versions()
{
    sed 's/^v//' | sort -t. -k1,1n -k2,2n -k3,3n -k4,4n | sed 's/^/v/'
}

nearest_release()
{
    nearest=$(git describe --tags --first-parent --abbrev=0 \
        --match 'v[0-9]*-stable' --match 'v[0-9]*-lts' "$1" 2>/dev/null) || return 0
    printf '%s\n' "$nearest" | grep -E "$tag_pattern" || die "malformed release tag: $nearest"
}

numbers_of()
{
    numbers=${1#v}
    printf '%s\n' "${numbers%-*}"
}

resolve()
{
    git rev-parse --verify --quiet "$1^{commit}" || die "unknown revision: $1"
}

cmd_current()
{
    commit=$(resolve "${1:-HEAD}")
    short=$(git rev-parse --short=7 "$commit")
    dirty=
    if [ $# -eq 0 ] && [ -n "$(git status --porcelain --untracked-files=no)" ]; then
        dirty=dirty
    fi

    tags=$(git tag --points-at "$commit" | grep -E "$tag_pattern" || true)
    exact=$(printf '%s\n' "$tags" | grep -v -- '-prestable$' | head -n 1 || true)
    [ -n "$exact" ] || exact=$(printf '%s\n' "$tags" | head -n 1)

    if [ -n "$exact" ]; then
        version=$(numbers_of "$exact")
        meta=$dirty
    else
        base=$(nearest_release "$commit")
        if [ -n "$base" ]; then
            version=$(numbers_of "$base")
            meta="$(git rev-list --count --first-parent "$base..$commit").g$short"
        else
            version="0.0.0.$(git rev-list --count --first-parent "$commit")"
            meta="g$short"
        fi
        [ -z "$dirty" ] || meta="$meta.$dirty"
    fi

    if [ -n "$meta" ]; then
        printf '%s+%s\n' "$version" "$meta"
    else
        printf '%s\n' "$version"
    fi
}

cmd_next()
{
    kind=${1:-}
    [ $# -eq 0 ] || shift
    ref=HEAD
    channel=
    date=
    while [ $# -gt 0 ]; do
        [ $# -ge 2 ] || die "$1 needs a value"
        case $1 in
        --ref) ref=$2 ;;
        --channel) channel=$2 ;;
        --date) date=$2 ;;
        *) die "unknown argument: $1" ;;
        esac
        shift 2
    done
    case $channel in
    '' | stable | lts | prestable) ;;
    *) die "unknown channel: $channel" ;;
    esac

    commit=$(resolve "$ref")

    case $kind in
    new)
        if [ -n "$date" ]; then
            case $date in
            [0-9][0-9][0-9][0-9]-[0-9][0-9]) ;;
            *) die "--date takes YYYY-MM" ;;
            esac
            year=${date%-*}
            month=${date#*-}
        else
            year=$(date -u +%Y)
            month=$(date -u +%m)
        fi
        line="${year#??}.${month#0}"
        [ -z "$(released_tags "v$line.*")" ] ||
            die "line $line is already released; cut a patch from branch $line"
        channel=${channel:-stable}
        patch=1
        base=$(nearest_release "$commit")
        ;;
    patch)
        [ -z "$date" ] || die "--date applies to a new line only"
        reachable=$(nearest_release "$commit")
        [ -n "$reachable" ] || die "no release is reachable from $ref; cut a new line first"
        line=$(numbers_of "$reachable" | cut -d. -f1-2)
        base=$(released_tags "v$line.*" | sort_versions | tail -n 1)
        git merge-base --is-ancestor "$base" "$commit" ||
            die "$ref does not contain $base, the latest release of line $line"
        line_channel=${base##*-}
        channel=${channel:-$line_channel}
        [ "$channel" = prestable ] || [ "$channel" = "$line_channel" ] ||
            die "line $line is $line_channel; it cannot release as $channel"
        patch=$(($(numbers_of "$base" | cut -d. -f3) + 1))
        ;;
    *)
        die "usage: version.sh next new|patch [--ref <ref>] [--channel <channel>]"
        ;;
    esac

    if [ -n "$base" ]; then
        build=$(git rev-list --count --first-parent "$base..$commit")
    else
        build=$(git rev-list --count --first-parent "$commit")
    fi
    [ "$build" -gt 0 ] || die "nothing to release: $ref has no commits after $base"

    version="$line.$patch.$build"
    tag="v$version-$channel"
    ! git rev-parse --verify --quiet "refs/tags/$tag" >/dev/null || die "tag $tag already exists"

    latest=false
    prerelease=true
    if [ "$channel" != prestable ]; then
        prerelease=false
        newest=$({
            released_tags
            printf '%s\n' "$tag"
        } | sort_versions | tail -n 1)
        [ "$newest" != "$tag" ] || latest=true
    fi

    printf 'version=%s\n' "$version"
    printf 'tag=%s\n' "$tag"
    printf 'channel=%s\n' "$channel"
    printf 'line=%s\n' "$line"
    printf 'commit=%s\n' "$commit"
    printf 'previous=%s\n' "$base"
    printf 'latest=%s\n' "$latest"
    printf 'prerelease=%s\n' "$prerelease"
}

command=${1:-}
[ $# -eq 0 ] || shift
case $command in
current) cmd_current "$@" ;;
next) cmd_next "$@" ;;
*) die "usage: version.sh current [<ref>] | next new|patch [options]" ;;
esac
