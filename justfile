set shell := ["bash", "-euo", "pipefail", "-c"]
bin := justfile_directory() + "/node_modules/.bin"

[default]
default:
    @just --list --justfile {{justfile()}}

build:
    {{bin}}/tsdown

install-deps:
    npm install
    uv sync --directory tools

update-deps:
    npm update
    uv lock --directory tools --upgrade
    just install-deps

format:
    {{bin}}/oxfmt --check src examples

lint:
    {{bin}}/oxlint src examples

typecheck:
    {{bin}}/tsc --noEmit
    {{bin}}/tsc -p examples --noEmit

architecture:
    node tools/fluent_api_architecture_test.mjs

codegen:
    node tools/check_codegen_freshness.mjs

examples: build
    node --experimental-strip-types --test examples/*.test.ts

packaging-runtime: build
    bash tools/packaging_test.sh

packaging-types: build
    bash tools/packaging_types_test.sh

packaging: packaging-runtime packaging-types

api-surface: build
    uv run --directory tools python api_surface.py

test: format lint typecheck examples architecture packaging codegen api-surface

[positional-arguments]
compat +args="":
    uv run --directory tools python -m pytest "$@"

full: test
    just compat

fix:
    {{bin}}/oxfmt src examples
    {{bin}}/oxlint --fix src examples

generate:
    uv run --directory tools generate.py

release version:
    npm version {{ version }} --no-git-tag-version
    git add --all
    git commit --message "Release v{{ version }}"
    git push
    git tag --annotate v{{ version }} --message v{{ version }}
    git push --tags
