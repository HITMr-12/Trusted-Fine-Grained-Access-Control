# Source migration record

This branch imports the functional FGAC demo into a source-only repository. It
intentionally excludes offline container archives, release ZIP files, generated
plugin binaries, Python caches, local credentials, and large benchmark datasets.

## Polaris provenance

The Polaris integration was developed against:

```text
tag:    apache-polaris-1.7.0
commit: 4ac2f059d1cce149453d0a5f1ff1dff980ec97cc
```

Only project-specific catalog code is carried here:

- `catalog/polaris-minimal`: the executable compatibility catalog for the demo;
- `catalog/polaris-extension`: the resource and test intended for the full
  Polaris source tree.

The complete upstream Polaris repository is not vendored. This keeps upstream
history and project-specific changes separate and avoids duplicating unrelated
source files.

## Component boundaries

- Polaris remains the policy and authorization control plane.
- Remote remains an independently deployable governed-data execution service.
- Spark and PostgreSQL adapters remain independently built, hot-loaded plugins.
- Storage owns all persistent Parquet data; governed data is not mounted into the
  ordinary engine containers.
- `deploy/two-node` defines the native baseline and Remote FGAC benchmark phases.
