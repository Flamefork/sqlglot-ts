set shell := ["bash", "-euo", "pipefail", "-c"]
bin := justfile_directory() + "/node_modules/.bin"
tools_dir := justfile_directory() + "/tools"
codegen_dir := tools_dir + "/codegen"
checks_dir := tools_dir + "/checks"

[default]
default:
    @just --list --justfile {{justfile()}}

build:
    {{bin}}/tsdown

install-deps:
    npm install
    uv sync --directory {{tools_dir}}

update-deps:
    npm update
    uv lock --directory {{tools_dir}} --upgrade
    just install-deps

fix:
    {{bin}}/oxfmt src examples tools
    {{bin}}/oxlint --fix src examples tools
    uv run --directory {{tools_dir}} ruff format .
    uv run --directory {{tools_dir}} ruff check . --fix || true

generate:
    uv run --directory {{tools_dir}} python codegen/generate.py
    uv run --directory {{tools_dir}} python codegen/api_surface_snapshot.py

release version:
    npm version {{ version }} --no-git-tag-version
    git add --all
    git commit --message "Release v{{ version }}"
    git push
    git tag --annotate v{{ version }} --message v{{ version }}
    git push --tags

# Tests

test-format:
    {{bin}}/oxfmt --check src examples tools
    uv run --directory {{tools_dir}} ruff format --check .

test-lint:
    {{bin}}/oxlint src examples tools
    uv run --directory {{tools_dir}} ruff check .
    uv run --directory {{tools_dir}} basedpyright

test-typecheck:
    {{bin}}/tsc --noEmit
    {{bin}}/tsc -p examples --noEmit

test-architecture:
    node --test {{checks_dir}}/fluent_api_architecture_test.mjs

test-codegen:
    node --test {{codegen_dir}}/check_freshness.mjs

test-examples: build
    node --experimental-strip-types --test examples/*.test.ts

test-packaging: build
    node --test {{checks_dir}}/packaging.test.mjs

test-api-surface: build
    node --test {{checks_dir}}/api_surface.test.mjs

test-compat-strict: build
    just compat -k "DuckDB"

test-compat-ratchet: build
    (just compat 2>&1 || true) | python3 {{tools_dir}}/compat/ratchet.py --check

test-compat-ratchet-update: build
    (just compat 2>&1 || true) | python3 {{tools_dir}}/compat/ratchet.py --update

test: test-format test-lint test-typecheck test-examples test-architecture test-packaging test-codegen test-api-surface test-compat-strict

test-full: test test-compat-ratchet

# Raw pytest passthrough

[positional-arguments]
compat +args="--": build
    uv run --directory {{tools_dir}} python -m pytest -c pyproject.toml "$@"
