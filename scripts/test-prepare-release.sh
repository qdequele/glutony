#!/usr/bin/env bash
# Tests for scripts/prepare-release.sh, run by CI's `version sync` job. Each
# case works on a throwaway copy of the files the script reads (Cargo.toml,
# Cargo.lock, docs/openapi.yaml, k8s/kustomization.yaml) in its own git repository, so the working tree
# is never touched. The CHANGELOG is the fixture's own: the repository's has an
# empty [Unreleased] on every release PR and on main right after a release, and
# the cases must not depend on what is in it.
#
#   scripts/test-prepare-release.sh
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
work="$(mktemp -d "${TMPDIR:-/tmp}/test-prepare-release.XXXXXX")"
trap 'rm -rf "$work"' EXIT

fails=0
pass() { echo "ok   - $1"; }
fail() { echo "FAIL - $1" >&2; fails=$((fails + 1)); }

crate="$(awk '/^\[workspace\.package\]/{p=1;next} /^\[/{p=0} p && /^version[ \t]*=/{gsub(/"/,"",$3); print $3; exit}' "$root/Cargo.toml")"

# fixture NAME [empty]: a fresh copy of the release files at the current crate
# version, committed in a new repository, with a CHANGELOG released at that
# version whose [Unreleased] has one entry (none with `empty`). Prints its path.
fixture() {
  local dir="$work/$1" entry="- A change worth releasing."
  [ "${2:-}" = empty ] && entry=""
  mkdir -p "$dir/scripts" "$dir/docs" "$dir/k8s"
  cp "$root/scripts/prepare-release.sh" "$dir/scripts/"
  cp "$root/Cargo.toml" "$root/Cargo.lock" "$dir/"
  cp "$root/docs/openapi.yaml" "$dir/docs/"
  cp "$root/k8s/kustomization.yaml" "$dir/k8s/"
  printf '# Changelog\n\n## [Unreleased]\n\n%s\n\n## [%s] - 2026-01-01\n\n- The last release.\n' \
    "$entry" "$crate" >"$dir/CHANGELOG.md"
  (
    cd "$dir"
    git init -q -b main
    git -c user.name=t -c user.email=t@example.invalid commit -q --allow-empty -m "chore: init"
    git add -A
    git -c user.name=t -c user.email=t@example.invalid commit -q -m "feat: fixture"
  )
  echo "$dir"
}

# expect_ok NAME DIR ARGS...: the script exits 0.
expect_ok() {
  local name="$1" dir="$2"
  shift 2
  if (cd "$dir" && scripts/prepare-release.sh "$@" >"$work/out" 2>&1); then pass "$name"; else
    fail "$name"
    sed 's/^/     /' "$work/out" >&2
  fi
}

# expect_fail NAME DIR PATTERN ARGS...: the script exits non-zero and says PATTERN.
expect_fail() {
  local name="$1" dir="$2" pattern="$3"
  shift 3
  if (cd "$dir" && scripts/prepare-release.sh "$@" >"$work/out" 2>&1); then
    fail "$name (exited 0)"
  elif grep -q -- "$pattern" "$work/out"; then
    pass "$name"
  else
    fail "$name (no '$pattern' in the output)"
    sed 's/^/     /' "$work/out" >&2
  fi
}

# --- --verify-tag: the gate release.yml runs before publishing ---------------

# The stray v0.2.0 case: a tag one minor ahead of an unbumped tree.
dir="$(fixture stray)"
stray="v$(awk -F. '{print $1 "." $2 + 1 ".0"}' <<<"$crate")"
expect_fail "--verify-tag refuses a tag ahead of the crate version" "$dir" "does not match the crate version" --verify-tag "$stray"
expect_fail "--verify-tag refuses a tag without the v prefix" "$dir" "needs vX.Y.Z" --verify-tag "$crate"
expect_fail "--verify-tag refuses no argument" "$dir" "needs vX.Y.Z" --verify-tag

# A prepared release passes, at the tag of the version it prepared.
dir="$(fixture prepared)"
(cd "$dir" && RELEASE_DATE=2026-01-02 scripts/prepare-release.sh 9.8.7 >/dev/null)
expect_ok "--verify-tag accepts a prepared release" "$dir" --verify-tag v9.8.7
expect_fail "--verify-tag refuses another version of a prepared release" "$dir" "does not match the crate version" --verify-tag v9.8.8

# Version copies that disagree fail even when Cargo.toml matches the tag.
dir="$(fixture drift)"
(cd "$dir" && RELEASE_DATE=2026-01-02 scripts/prepare-release.sh 9.8.7 >/dev/null)
sed -i.bak 's/^  version: 9\.8\.7$/  version: 9.8.6/' "$dir/docs/openapi.yaml"
expect_fail "--verify-tag refuses disagreeing version copies" "$dir" "disagree" --verify-tag v9.8.7

# A bumped tree whose CHANGELOG was not folded has no section for the tag.
dir="$(fixture nosection)"
(cd "$dir" && RELEASE_DATE=2026-01-02 scripts/prepare-release.sh 9.8.7 >/dev/null)
sed -i.bak 's/^## \[9\.8\.7\] - 2026-01-02$//' "$dir/CHANGELOG.md"
expect_fail "--verify-tag refuses a tag with no CHANGELOG section" "$dir" "no \[9.8.7\] section" --verify-tag v9.8.7

# --- k8s/kustomization.yaml: the deployed image tag is a version copy ---------

# The shipped kustomization pins the release, never the moving `latest` (nodes
# that cached `latest` never pull a newer one with imagePullPolicy IfNotPresent).
dir="$(fixture k8s)"
expect_ok "--check accepts the kustomization at the crate version" "$dir" --check
sed -i.bak 's/^\(    newTag:\).*$/\1 latest/' "$dir/k8s/kustomization.yaml"
expect_fail "--check refuses a kustomization newTag that is not the crate version" "$dir" "newTag" --check
expect_fail "--verify-tag refuses a kustomization newTag that is not the crate version" "$dir" "disagree" --verify-tag "v$crate"
expect_ok "--sync writes the crate version into the kustomization" "$dir" --sync
if grep -qx "    newTag: $crate" "$dir/k8s/kustomization.yaml"; then pass "--sync pins newTag to $crate"; else
  fail "--sync pins newTag to $crate"
  grep -n newTag "$dir/k8s/kustomization.yaml" | sed 's/^/     /' >&2
fi
expect_ok "--check accepts the synced kustomization" "$dir" --check
if (cd "$dir" && git diff --quiet -- Cargo.toml Cargo.lock docs/openapi.yaml CHANGELOG.md); then pass "--sync changes nothing else"; else
  fail "--sync changes nothing else"
fi

dir="$(fixture k8sprepare)"
(cd "$dir" && RELEASE_DATE=2026-01-02 scripts/prepare-release.sh 9.8.7 >/dev/null)
if grep -qx "    newTag: 9.8.7" "$dir/k8s/kustomization.yaml"; then pass "prepare pins the kustomization newTag to the release"; else
  fail "prepare pins the kustomization newTag to the release"
  grep -n newTag "$dir/k8s/kustomization.yaml" | sed 's/^/     /' >&2
fi
if (cd "$dir" && git diff -U0 -- k8s/kustomization.yaml | grep '^[-+] ' | grep -vq 'newTag:'); then
  fail "prepare changes only newTag in the kustomization"
else pass "prepare changes only newTag in the kustomization"; fi

# --- prepare: bumps count from the last tag, never reuse one -----------------

dir="$(fixture tagged)"
(cd "$dir" && git tag "$stray")
expect_fail "prepare refuses a version whose tag exists" "$dir" "already exists" "${stray#v}"
expect_ok "prepare bumps from the last tag, not the crate version" "$dir" patch
want="$(awk -F. '{print $1 "." $2 "." $3 + 1}' <<<"${stray#v}")"
expect_ok "the prepared tree verifies at the bumped tag" "$dir" --verify-tag "v$want"

# --- prepare: the [Unreleased] section ---------------------------------------

dir="$(fixture empty empty)"
expect_fail "prepare refuses an empty [Unreleased]" "$dir" "is empty" patch

# A freshly released tree ([Unreleased] folded, so empty again): re-running the
# same bump confirms it, and the next version waits for new entries.
dir="$(fixture released)"
(cd "$dir" && git tag "v$crate")
next="$(awk -F. '{print $1 "." $2 "." $3 + 1}' <<<"$crate")"
expect_ok "prepare folds [Unreleased]" "$dir" patch
expect_ok "prepare re-run on a freshly released tree" "$dir" patch
if grep -q "v$next is already prepared" "$work/out"; then pass "the re-run says the release is already prepared"; else
  fail "the re-run says the release is already prepared"
  sed 's/^/     /' "$work/out" >&2
fi
expect_fail "a freshly released tree refuses the next release until [Unreleased] has entries" "$dir" "is empty" minor

if [ "$fails" -gt 0 ]; then
  echo "$fails failure(s)" >&2
  exit 1
fi
echo "all prepare-release.sh tests passed"
