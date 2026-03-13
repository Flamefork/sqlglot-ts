#!/usr/bin/env bash
set -euo pipefail

echo "=== Type smoke test: npm pack -> temp install -> tsc ==="

REPO_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." >/dev/null && pwd)
PACK_DIR=$(mktemp -d)
PROJ_DIR=$(mktemp -d)
trap 'rm -rf "$PACK_DIR" "$PROJ_DIR"' EXIT

cd "$REPO_ROOT"

npm run build
npm pack --pack-destination "$PACK_DIR"

TARBALLS=("$PACK_DIR"/sqlglot-ts-*.tgz)
if [[ ${#TARBALLS[@]} -ne 1 ]]; then
  echo "Expected exactly one tarball, found: ${TARBALLS[*]}" >&2
  exit 1
fi
TARBALL="${TARBALLS[0]}"
echo "Packed: $TARBALL"

cd "$PROJ_DIR"

cat > package.json <<'EOF'
{ "type": "module" }
EOF

npm install "$TARBALL" --save

cat > tsconfig.json <<'EOF'
{
  "compilerOptions": {
    "module": "nodenext",
    "moduleResolution": "nodenext",
    "target": "es2022",
    "strict": true,
    "skipLibCheck": false
  },
  "include": ["test.ts"]
}
EOF

cat > test.ts <<'EOF'
import { parseOne, select } from "sqlglot-ts"
import * as exp from "sqlglot-ts/expressions"

select("x").from_("tbl").where("x = 1")
parseOne("SELECT x").assertIs(exp.Select).select("y")
parseOne("SELECT x").assertIs(exp.Column).desc()

const ordered = parseOne("SELECT x FROM tbl ORDER BY x DESC")
  .find(exp.Ordered)
  ?.assertIs(exp.Ordered)

if (!ordered) {
  throw new Error("expected ordered expression")
}

ordered.desc()
// @ts-expect-error Ordered no longer exposes nullsFirst convenience getter
ordered.nullsFirst
EOF

node "$REPO_ROOT/node_modules/typescript/bin/tsc" --noEmit -p tsconfig.json
echo "=== Type smoke test passed ==="
