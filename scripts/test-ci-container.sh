#!/usr/bin/env bash
# Runs `just test-ci` inside a container shaped like the GitHub runner: ubuntu,
# 2 CPUs, 7GB. The CPU budget is the point — concurrency bugs in the sweep path
# surface there and stay hidden on a developer machine with more cores.
#
# The repo is cloned to a temp dir rather than mounted, so the host .venv is
# left alone and the container gets a Linux-native one.
set -euo pipefail

repo_root=$(git rev-parse --show-toplevel)
branch=$(git -C "$repo_root" rev-parse --abbrev-ref HEAD)
workdir=$(mktemp -d)
cache="${XDG_CACHE_HOME:-$HOME/.cache}/aidmi-test-ci-container"
mkdir -p "$cache/uv-home" "$cache/uv-cache"

cleanup() { rm -rf "$workdir"; }
trap cleanup EXIT

echo "cloning $branch into $workdir/repo"
git clone --quiet --local "$repo_root" "$workdir/repo" -b "$branch"

# The clone only carries committed state, but the point of running this locally
# is to test what you have right now, so replay uncommitted tracked changes.
if ! git -C "$repo_root" diff --quiet HEAD; then
  echo "applying uncommitted changes"
  git -C "$repo_root" diff --binary HEAD | git -C "$workdir/repo" apply --index
fi
untracked=$(git -C "$repo_root" ls-files --others --exclude-standard)
if [ -n "$untracked" ]; then
  echo "note: untracked files are NOT copied into the container:" >&2
  echo "$untracked" | sed 's/^/  /' >&2
fi

cat > "$workdir/run.sh" <<'INNER'
set -eu
export PATH="/root/.local/bin:$PATH"
if [ ! -x /root/.local/bin/uv ]; then
  export DEBIAN_FRONTEND=noninteractive
  apt-get update -qq
  apt-get install -y -qq curl ca-certificates >/dev/null
  curl -LsSf https://astral.sh/uv/install.sh | sh >/dev/null
fi
cd /w
uv sync --all-packages --extra plots
uv run --all-packages --extra plots pytest packages -m "not requires_llm" "$@"
INNER

# The Docker socket is shared so testcontainers starts Postgres as a sibling
# container; TESTCONTAINERS_HOST_OVERRIDE is how the test process reaches it.
exec docker run --rm --cpus 2 --memory 7g \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -v "$workdir/repo:/w" \
  -v "$workdir/run.sh:/run.sh" \
  -v "$cache/uv-home:/root/.local" \
  -v "$cache/uv-cache:/root/.cache/uv" \
  -e TESTCONTAINERS_HOST_OVERRIDE=host.docker.internal \
  -e TESTCONTAINERS_RYUK_DISABLED=true \
  -w /w ubuntu:24.04 bash /run.sh "$@"
