#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." >/dev/null && pwd)
cd "$REPO_ROOT"

VERSION="${1:-}"
if [ -z "$VERSION" ]; then
  echo "Usage: npm run release -- <version>" >&2
  exit 1
fi

npm run release:prepare

npm version "$VERSION" --no-git-tag-version
git add package.json package-lock.json
git commit --message "Release v$(node -p 'require("./package.json").version')"
git push
git tag --annotate "v$(node -p 'require("./package.json").version')" --message "v$(node -p 'require("./package.json").version')"
git push --tags
