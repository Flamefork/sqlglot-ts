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

format:
    {{bin}}/oxfmt --check src examples tools
    uv run --directory {{tools_dir}} ruff format --check .

lint:
    {{bin}}/oxlint src examples tools
    uv run --directory {{tools_dir}} ruff check .
    uv run --directory {{tools_dir}} basedpyright

typecheck:
    {{bin}}/tsc --noEmit
    {{bin}}/tsc -p examples --noEmit

architecture:
    node --test {{checks_dir}}/fluent_api_architecture_test.mjs

codegen:
    node --test {{codegen_dir}}/check_freshness.mjs

examples: build
    node --experimental-strip-types --test examples/*.test.ts

packaging: build
    node --test {{checks_dir}}/packaging.test.mjs

api-surface: build
    node --test {{checks_dir}}/api_surface.test.mjs

test: format lint typecheck examples architecture packaging codegen api-surface

[positional-arguments]
compat +args="":
    uv run --directory {{tools_dir}} python -m pytest "$@"

full: test
    just compat

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
