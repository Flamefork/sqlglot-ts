# API Surface Parity Metric

## Why

sqlglot-ts is a port of Python SQLGlot. Users expect the same API: the same functions (`select()`, `from_()`, `condition()`), the same methods on expressions (`.where()`, `.join()`, `.eq()`). Without automated checking it's easy to miss an unported method or break an existing one.

This script is a strict parity check: it counts how many Python functions/methods are missing in TS and fails unless the missing count is zero.

## Usage

```bash
npm run check:api-surface
```

## How it works

1. Python introspection: collects top-level functions and own methods per class from `sqlglot.expressions`
2. TS introspection: via `node --input-type=module` collects exports and prototype methods from `dist/`
3. Comparison: automatic snake_case → camelCase mapping, reserved word handling (`delete` → `delete_`)
4. Strict check: exits with code 1 if any Python API surface is still missing in TS

## Files

| File | Purpose |
|---|---|
| `api_surface.py` | Comparison script |
| `api_surface_excludes.json` | Exclusions with rationale (internal utils, Python-specific, etc.) |
## Workflow

- Implemented a new method/function -> run `npm run check:api-surface`
- The check stays green only when missing count is zero
- New exclusion — add to `api_surface_excludes.json` with rationale
