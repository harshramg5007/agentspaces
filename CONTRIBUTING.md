# Contributing

## Scope

Contributions should preserve the supported public boundary:

- `examples/` is supported and release-gated
- benchmark, experimental, and internal qualification material are not part of the public launch surface

## External Contributions

External pull requests are closed for the initial source-available launch. Contributor terms are not yet published, so outside contributions are not being accepted until that policy is in place.

## Workflow

1. Keep README and docs aligned with the actual supported surface.
2. Add or update tests for user-visible behavior changes.
3. Run the public smoke path before opening a PR:

```bash
make doctor
make up
make smoke
make down
```

## Pull Requests

- Prefer narrow, reviewable changes.
- Call out any path moves, public API changes, or support-boundary changes.
- Do not add generated local artifacts or machine-specific files to the repo.
