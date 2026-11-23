#!/bin/sh
# usage: deploy.sh <site directory>
# Uploads the site that site.sh built to the R2 bucket that serves
# https://dl.lynxdb.org. The files of the releases, which never change under
# their name, go first, and the files that name a release after, so a client
# finds every release it is pointed at. Nothing is deleted.
# R2_ACCOUNT_ID, R2_ACCESS_KEY_ID and R2_SECRET_ACCESS_KEY name the Cloudflare
# account and an API token that may write the objects of the bucket, and
# R2_BUCKET_NAME the bucket, lynxdb-releases unless set.
# LYNXDB_SITE_REMOTE names another rclone destination to upload to instead.
set -eu

[ $# -eq 1 ] || {
    echo "usage: deploy.sh <site directory>" >&2
    exit 2
}
site=$(CDPATH='' cd -- "$1" && pwd)

remote=${LYNXDB_SITE_REMOTE:-}
if [ -z "$remote" ]; then
    : "${R2_ACCOUNT_ID:?names the Cloudflare account}"
    : "${R2_ACCESS_KEY_ID:?names the API token}"
    : "${R2_SECRET_ACCESS_KEY:?holds the secret of the API token}"
    export RCLONE_CONFIG=/dev/null
    export RCLONE_S3_PROVIDER=Cloudflare
    export RCLONE_S3_ENDPOINT="https://$R2_ACCOUNT_ID.r2.cloudflarestorage.com"
    export RCLONE_S3_ACCESS_KEY_ID="$R2_ACCESS_KEY_ID"
    export RCLONE_S3_SECRET_ACCESS_KEY="$R2_SECRET_ACCESS_KEY"
    # A token scoped to the objects of one bucket may not ask whether the bucket exists.
    export RCLONE_S3_NO_CHECK_BUCKET=true
    remote=:s3:${R2_BUCKET_NAME:-lynxdb-releases}
fi

# upload <cache-control> <rclone argument>...: copies what the filter on stdin admits.
upload()
{
    cache=$1
    shift
    rclone copy "$site" "$remote" --checksum --filter-from - --verbose --stats 0 \
        --header-upload "Cache-Control: $cache" "$@"
}

# A file of a published release that differs from the bucket's is an error, not an update.
printf '%s\n' '+ /dist/**' '- *' | upload 'public, max-age=31536000, immutable' --immutable
# A browser shows these instead of saving them.
printf '%s\n' '+ /install.sh' '+ /channels/*' '+ /versions/*' '- *' |
    upload no-cache --header-upload 'Content-Type: text/plain; charset=utf-8'
