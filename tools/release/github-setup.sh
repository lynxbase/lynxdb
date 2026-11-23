#!/bin/sh
# usage: github-setup.sh [--yes]
# Applies the repository settings the release process relies on; RELEASING.md
# lists them. Also gives the release workflow a deploy key for the Homebrew
# tap. Safe to run again.
# Needs gh, signed in as an administrator of the repository and of the tap.
set -eu

assume_yes=0
while [ $# -gt 0 ]; do
    case $1 in
    --yes) assume_yes=1 ;;
    *)
        echo "usage: github-setup.sh [--yes]" >&2
        exit 2
        ;;
    esac
    shift
done

repo=$(gh repo view --json nameWithOwner --jq .nameWithOwner)
org=${repo%/*}
tap=$org/homebrew-tap

echo "This changes the settings of $repo and adds a deploy key to $tap."
if [ "$assume_yes" -eq 0 ]; then
    printf 'Continue? [y/N] '
    read -r answer
    case $answer in
    y | Y | yes) ;;
    *) exit 1 ;;
    esac
fi

say()
{
    printf '%s\n' "- $*"
}

# ruleset <name>: creates the ruleset read from stdin, or replaces the one of that name.
ruleset()
{
    id=$(gh api "repos/$repo/rulesets" --jq ".[] | select(.name == \"$1\") | .id")
    if [ -n "$id" ]; then
        gh api --silent -X PUT "repos/$repo/rulesets/$id" --input -
    else
        gh api --silent -X POST "repos/$repo/rulesets" --input -
    fi
    say "rules: $1"
}

# A repository cannot allow what its organization forbids, and only an owner
# of the organization can change that.
if ! gh api --silent -X PUT "repos/$repo/actions/permissions/workflow" \
    -f default_workflow_permissions=read -F can_approve_pull_request_reviews=true; then
    cat >&2 <<EOF
Allow it for the organization first, then run this again:
  https://github.com/organizations/$org/settings/actions
  Workflow permissions: Allow GitHub Actions to create and approve pull requests
EOF
    exit 1
fi
say 'workflows may open pull requests'

gh api --silent -X PUT "repos/$repo/immutable-releases"
say 'published releases are immutable'

gh api --silent -X PUT "repos/$repo/private-vulnerability-reporting"
say 'vulnerabilities can be reported privately'

# The release workflow always runs from the default branch, whatever commit it releases.
gh api --silent -X PUT "repos/$repo/environments/release" --input - <<JSON
{
  "reviewers": [{ "type": "User", "id": $(gh api user --jq .id) }],
  "prevent_self_review": false,
  "deployment_branch_policy": { "protected_branches": false, "custom_branch_policies": true }
}
JSON
default_branch=$(gh repo view --json defaultBranchRef --jq .defaultBranchRef.name)
if ! gh api "repos/$repo/environments/release/deployment-branch-policies" \
    --jq '.branch_policies[].name' | grep -qx "$default_branch"; then
    gh api --silent -X POST "repos/$repo/environments/release/deployment-branch-policies" \
        -f name="$default_branch" -f type=branch
fi
say "the release environment waits for your approval and runs from $default_branch alone"

# The maintainers push to these branches directly, so what is protected is
# their history. A release checks its commit itself before publishing.
ruleset 'Branches' <<'JSON'
{
  "name": "Branches",
  "target": "branch",
  "enforcement": "active",
  "conditions": {
    "ref_name": { "include": ["~DEFAULT_BRANCH", "refs/heads/[0-9][0-9].*"], "exclude": [] }
  },
  "rules": [
    { "type": "deletion" },
    { "type": "non_fast_forward" }
  ]
}
JSON

# The tags of the releases numbered major.minor.patch stay as they are.
ruleset 'Release tags' <<'JSON'
{
  "name": "Release tags",
  "target": "tag",
  "enforcement": "active",
  "conditions": {
    "ref_name": {
      "include": ["refs/tags/v*-stable", "refs/tags/v*-lts", "refs/tags/v*-prestable"],
      "exclude": []
    }
  },
  "rules": [
    { "type": "deletion" },
    { "type": "non_fast_forward" },
    { "type": "update" }
  ]
}
JSON

# The key reaches the tap alone. Its private half is kept nowhere else: to
# replace it, delete the deploy key of this title from the tap and run again.
if ! gh repo deploy-key list --repo "$tap" | grep -q 'lynxdb release'; then
    keys=$(mktemp -d)
    trap 'rm -rf "$keys"' EXIT
    ssh-keygen -q -t ed25519 -N '' -C 'lynxdb release' -f "$keys/tap_key"
    if ! gh repo deploy-key add "$keys/tap_key.pub" --repo "$tap" --allow-write \
        --title 'lynxdb release' >/dev/null; then
        cat >&2 <<EOF
If deploy keys are disabled, enable them for the organization first, then run this again:
  https://github.com/organizations/$org/settings/member_privileges
  Deploy keys: Enabled
EOF
        exit 1
    fi
    gh secret set HOMEBREW_TAP_KEY --env release <"$keys/tap_key"
fi
say "the release workflow can push to $tap"
