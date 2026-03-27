# Release And Verification

The public release surface is intentionally narrower than the private development repo used to stage it.

## Release Gates

- `scripts/check_public_surface.sh`
- `go build ./...`
- `go test -race ./...`
- `python3.11 -m build sdk/python`
- Supported README commands succeed in CI
- `make doctor && make up && make smoke && make down`
- Python SDK unit coverage for lifecycle and error mapping

## Scope Boundary

Release gates cover:

- Go server
- Lifecycle HTTP API
- Manual Python SDK
- Supported examples in `examples/`

## Artifacts

- GitHub tag releases publish server binaries and Python build artifacts
- Python wheel and sdist artifacts must include the BUSL license file
