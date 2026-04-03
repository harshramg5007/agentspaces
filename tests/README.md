# Test Layout

This repository stores canonical test sources under `tests/`.

- `tests/go` contains the source-of-truth Go `*_test.go` files, mirrored by their original package paths.
- No Go `*_test.go` files are committed under `cmd/`, `internal/`, or `pkg/`.
- `scripts/run_go_tests.sh` temporarily materializes package-path symlinks, runs the Go suite, and removes those symlinks on exit.
- `make test` is the supported Go test entrypoint for a clean checkout.
- `tests/go/go.mod` is a storage-only nested module that prevents the root module from discovering the mirrored Go tests twice.
- `tests/python` contains the relocated Python SDK unit tests.
- This layout assumes a symlink-capable Unix/macOS checkout.
