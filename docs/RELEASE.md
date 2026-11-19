# Releasing

Releases are tagged by the coordinator. This checklist is the release
gate; every step leaves evidence.

## 1. Gate green

`scripts/check.sh` — exactly what CI runs — passes on the release
commit: toolchain pin, `cargo fmt --check`, `clippy -D warnings`,
workspace tests, domain-purity + no-process-spawn checks, `cargo deny`,
npm audit/check/build, `deploy/test-format-guard.sh`, the pilot.

## 2. Evidence current

`perf/results/` carries the battery for the release commit (each
artifact records its machine + `commit`). A miss is kept as evidence,
never deleted or rerun to green — a regression must be explained in the
changelog entry that ships it.

## 3. Version

`[workspace.package] version` in `Cargo.toml` is the release version
(all five crates inherit it). Regenerate `Cargo.lock`:

```sh
export PATH="$(brew --prefix rustup)/bin:$PATH"
cargo check          # rewrites the lock's workspace versions; deps unchanged
```

Both binaries report it: `durable-server --version`, `durable --version`.

## 4. Changelog

`CHANGELOG.md` gains a dated `## [X.Y.Z] - YYYY-MM-DD` section covering
everything since the last release, following Keep a Changelog. Numbers
in it must match committed `perf/results/` artifacts.

## 5. Tag (coordinator)

After the branch merges to `main`:

```sh
git tag -a v1.0.0 <merge-commit> -m "v1.0.0"
git push origin v1.0.0
```

## Journal-format rule (D-056)

A new record format ships its **reader** one release before any writer
emits it, so rollback across one release is always readable.
`deploy/format-guard.sh` enforces it at upgrade/rollback and fails
closed on unreadable state.
