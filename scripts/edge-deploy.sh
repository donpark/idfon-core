#!/usr/bin/env bash
# Update the production idfon-edge to a tagged image. Routine updates only:
# provisioning (droplet, DNS, certs, relay config) stays with `pnpm deploy:do`,
# which rewrites the droplet .env — this script never does.
#
#   pnpm edge deploy              # tag origin/main as the next edge-vX.Y.Z if the
#                                 # edge changed since the last tag, wait for CI to
#                                 # publish the image, then roll it out
#   pnpm edge deploy edge-v0.9.18 # roll out an existing tag (rollback), or create
#                                 # that tag on origin/main if it doesn't exist
#   pnpm edge deploy -y ...       # no confirmation prompt
#   pnpm edge deploy -n ...       # dry run: show what would be tagged and deployed
#
# Rollout touches only the idfon-edge service: pins IDFON_EDGE_IMAGE in the
# droplet .env to the tag (other lines kept; previous file saved as .env.prev),
# pulls, and recreates the container with --no-deps. Then it verifies the
# running image, the APNs key id (docs/edge-deploy-checklist.md §C.4), and
# /healthz.
#
# Env: IDFON_EDGE_HOST (default idfon.net), IDFON_DO_SSH_KEY (default
# ~/.ssh/idfon_edge_ed25519), IDFON_EDGE_KEYID_FILE (default
# ~/.idfon/edge.apns.keyid).
set -euo pipefail

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$root"

HOST="${IDFON_EDGE_HOST:-idfon.net}"
KEY="${IDFON_DO_SSH_KEY:-$HOME/.ssh/idfon_edge_ed25519}"
KEYID_FILE="${IDFON_EDGE_KEYID_FILE:-$HOME/.idfon/edge.apns.keyid}"
IMAGE_REPO="ghcr.io/donpark/idfon-edge"
REMOTE_DIR="/srv/idfon-edge"
SERVICE="idfon-edge"
CONTAINER="idfon-edge-idfon-edge-1"

say() { printf '\n\033[1m== %s\033[0m\n' "$*"; }
info() { printf '   %s\n' "$*"; }
die() { printf 'error: %s\n' "$*" >&2; exit 1; }

yes=0
dry=0
tag=""
for arg in "$@"; do
  case "$arg" in
    -y|--yes) yes=1 ;;
    -n|--dry-run) dry=1 ;;
    edge-v*) tag="$arg" ;;
    -h|--help) sed -n '2,25p' "$0"; exit 0 ;;
    *) die "unknown argument: $arg (usage: pnpm edge deploy [-y] [-n] [edge-vX.Y.Z])" ;;
  esac
done

confirm() {
  [ "$yes" = 1 ] && return 0
  printf '%s [y/N] ' "$1"
  read -r answer
  case "$answer" in y|Y|yes) ;; *) die "cancelled" ;; esac
}

command -v gh >/dev/null || die "gh not found"
[ -f "$KEY" ] || die "ssh key not found: $KEY"
[ -f "$KEYID_FILE" ] || die "APNs key id file not found: $KEYID_FILE"
SSH="ssh -i $KEY -o IdentitiesOnly=yes -o BatchMode=yes -o ConnectTimeout=10 -o StrictHostKeyChecking=accept-new root@$HOST"
$SSH true 2>/dev/null || die "cannot ssh to root@$HOST (the droplet firewall admits only the admin IP set at provisioning; re-run pnpm deploy:do or update the firewall if your IP changed)"

say "release"
# Tags come from origin, not local refs (local tags can be stale; fetching
# them with --tags would clobber).
git fetch -q origin main
main_sha=$(git rev-parse origin/main)
remote_tags=$(git ls-remote --tags origin 'refs/tags/edge-v*')
tag_names() { printf '%s\n' "$remote_tags" | sed -n 's|.*refs/tags/\(edge-v[^^]*\)$|\1|p'; }
tag_sha() { # commit a remote tag points at (peeled for annotated tags)
  local sha
  sha=$(printf '%s\n' "$remote_tags" | awk -v r="refs/tags/$1^{}" '$2 == r { print $1 }')
  [ -n "$sha" ] || sha=$(printf '%s\n' "$remote_tags" | awk -v r="refs/tags/$1" '$2 == r { print $1 }')
  printf '%s' "$sha"
}
last=$(tag_names | sed 's/^edge-v//' | sort -t. -k1,1n -k2,2n -k3,3n | awk 'END { print "edge-v" $0 }')
[ "$last" != "edge-v" ] || die "no edge-v* tags on origin"
last_sha=$(tag_sha "$last")
git cat-file -e "$last_sha^{commit}" 2>/dev/null || git fetch -q origin "$last_sha"

new_tag=0
if [ -z "$tag" ]; then
  # Local crates idfon-edge is built from (transitively), plus build inputs.
  paths=$(cargo tree -q -p idfon-edge -e normal,build --prefix none --format '{p}' \
    | sed -n "s|.*(\($root/[^)]*\)).*|\1|p" | sort -u | sed "s|^$root/||")
  paths="$paths deploy/idfon-edge Cargo.toml Cargo.lock"
  # shellcheck disable=SC2086
  if git diff --quiet "$last_sha" "$main_sha" -- $paths; then
    tag="$last"
    info "no edge changes on origin/main since $last; deploying $last"
  else
    IFS=. read -r major minor patch <<<"${last#edge-v}"
    tag="edge-v$major.$minor.$((patch + 1))"
    new_tag=1
    info "edge changes since $last:"
    # shellcheck disable=SC2086
    git log --oneline "$last_sha..$main_sha" -- $paths | sed 's/^/     /'
  fi
elif [ -z "$(tag_sha "$tag")" ]; then
  new_tag=1
fi

if [ "$dry" = 1 ]; then
  current=$($SSH "docker inspect $CONTAINER --format '{{.Config.Image}}'")
  info "droplet runs: $current"
  if [ "$new_tag" = 1 ]; then info "would tag origin/main ($(git log -1 --format=%h "$main_sha")) as $tag, then deploy $IMAGE_REPO:$tag"
  else info "would deploy $IMAGE_REPO:$tag"; fi
  exit 0
fi

if [ "$new_tag" = 1 ]; then
  confirm "Tag origin/main ($(git log -1 --format='%h %s' "$main_sha")) as $tag, publish, and deploy to $HOST?"
  git push -q origin "$main_sha:refs/tags/$tag"
  info "pushed $tag; waiting for the edge workflow to publish the image"
  run_id=""
  for _ in $(seq 1 30); do
    run_id=$(gh run list --workflow edge.yml --event push --limit 20 \
      --json databaseId,headBranch --jq "[.[] | select(.headBranch == \"$tag\")][0].databaseId // empty")
    [ -n "$run_id" ] && break
    sleep 4
  done
  [ -n "$run_id" ] || die "no edge workflow run found for $tag; check GitHub Actions"
  gh run watch "$run_id" --exit-status --interval 15 >/dev/null \
    || die "edge workflow failed for $tag: gh run view $run_id --log-failed"
  info "image published: $IMAGE_REPO:$tag"
else
  confirm "Deploy existing $tag to $HOST?"
fi

image="$IMAGE_REPO:$tag"

say "rollout ($image)"
# Compose files on the droplet should match the repo; changing them is a
# provisioning step (pnpm deploy:do), not an image update.
local_sum=$(shasum -a 256 deploy/digitalocean/compose.yaml | cut -d' ' -f1)
remote_sum=$($SSH "sha256sum $REMOTE_DIR/compose.yaml" | cut -d' ' -f1)
[ "$local_sum" = "$remote_sum" ] \
  || info "warning: droplet compose.yaml differs from deploy/digitalocean/compose.yaml (deploying the image only)"

$SSH "set -e
  cd $REMOTE_DIR
  files='-f compose.yaml'
  [ -f compose.relay.yaml ] && files=\"\$files -f compose.relay.yaml\"
  grep -q '^IDFON_EDGE_PUSH_KEY_ID=' .env || { echo 'error: .env has no IDFON_EDGE_PUSH_KEY_ID (C28); refusing' >&2; exit 1; }
  docker pull -q '$image' >/dev/null
  cp -p .env .env.prev
  if grep -q '^IDFON_EDGE_IMAGE=' .env; then
    sed -i 's|^IDFON_EDGE_IMAGE=.*|IDFON_EDGE_IMAGE=$image|' .env
  else
    echo 'IDFON_EDGE_IMAGE=$image' >> .env
  fi
  docker compose \$files up -d --no-deps $SERVICE"

say "verify"
running=$($SSH "docker inspect $CONTAINER --format '{{.Config.Image}} {{.State.Status}}'")
[ "$running" = "$image running" ] || die "container is '$running', expected '$image running'"
info "container: $running"

want_keyid=$(tr -d '[:space:]' <"$KEYID_FILE")
have_keyid=$($SSH "docker inspect $CONTAINER --format '{{json .Args}}'" \
  | grep -o '"--push-apns-key-id","[A-Z0-9]*"' | cut -d'"' -f4)
[ "$have_keyid" = "$want_keyid" ] \
  || die "APNs key id on the droplet does not match $KEYID_FILE (C26/C28); every push will fail. Fix the droplet .env."
info "APNs key id: matches $KEYID_FILE"

ok=0
for _ in $(seq 1 15); do
  if [ "$(curl -s -m 5 -o /dev/null -w '%{http_code}' "https://$HOST/healthz")" = 200 ]; then ok=1; break; fi
  sleep 2
done
[ "$ok" = 1 ] || die "https://$HOST/healthz is not answering 200; logs: $SSH 'cd $REMOTE_DIR && docker compose logs --since 5m $SERVICE'"
info "healthz: 200"

say "deployed $tag to $HOST"
info "rollback: pnpm edge deploy <previous edge-v tag>"
info "still manual (checklist §C.5): one person-call ring each way, then"
info "  $SSH 'cd $REMOTE_DIR && docker compose logs --since 10m $SERVICE' | grep -E 'apns send failed|InvalidProviderToken'"
