#!/usr/bin/env bash
set -euo pipefail

BUMP="${1:-patch}"             # patch|minor|major
WORKFLOW="${WORKFLOW:-release.yml}"
BRANCH="${BRANCH:-main}"
REMOTE="${REMOTE:-origin}"
GUARD_FILE="${GUARD_FILE:-scripts/.release_guard}"

if [[ "$BUMP" != "patch" && "$BUMP" != "minor" && "$BUMP" != "major" ]]; then
  echo "BLOCKED: bump must be one of patch|minor|major (got: $BUMP)"
  exit 1
fi

if [[ -z "${RELEASE_GUARD:-}" && -f "$GUARD_FILE" ]]; then
  RELEASE_GUARD="$(tr -d '\n' < "$GUARD_FILE")"
fi

if [[ -z "${RELEASE_GUARD:-}" ]]; then
  echo "BLOCKED: RELEASE_GUARD is not set."
  echo "Set RELEASE_GUARD or create $GUARD_FILE."
  exit 1
fi

if [[ "${RELEASE_SAFE_SKIP_CLEAN:-0}" != "1" ]]; then
  # Hard guard: never dispatch with local modifications.
  if [[ -n "$(git status --porcelain)" ]]; then
    echo "BLOCKED: working tree is not clean"
    git status --short
    exit 1
  fi
fi

current_branch="$(git rev-parse --abbrev-ref HEAD)"
if [[ "$current_branch" != "$BRANCH" ]]; then
  echo "BLOCKED: expected branch '$BRANCH', got '$current_branch'"
  exit 1
fi

git fetch "$REMOTE" "$BRANCH" --tags

if [[ "${RELEASE_SAFE_SKIP_SYNC:-0}" != "1" ]]; then
  read -r behind ahead < <(git rev-list --left-right --count "$REMOTE/$BRANCH"...HEAD)
  if [[ "$behind" != "0" || "$ahead" != "0" ]]; then
    echo "BLOCKED: branch not synced with $REMOTE/$BRANCH (behind=$behind ahead=$ahead)"
    exit 1
  fi
fi

echo "PASS: clean and synced. Dispatching $WORKFLOW with release_guard"
if grep -q '^[[:space:]]*bump:[[:space:]]*$' ".github/workflows/$WORKFLOW"; then
  gh workflow run "$WORKFLOW" -f bump="$BUMP" -f release_guard="$RELEASE_GUARD"
else
  gh workflow run "$WORKFLOW" -f release_guard="$RELEASE_GUARD"
fi
