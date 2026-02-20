#!/bin/sh
set -e

VERSION="$1"
if [ -z "$VERSION" ]; then
  echo "Usage: npm run release -- <version>" >&2
  exit 1
fi

npm version "$VERSION" --no-git-tag-version
git add --all
git commit --message "Release v$(node -p 'require("./package.json").version')"
git push
git tag --annotate "v$(node -p 'require("./package.json").version')" --message "v$(node -p 'require("./package.json").version')"
git push --tags
