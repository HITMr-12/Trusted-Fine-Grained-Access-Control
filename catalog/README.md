# Catalog integration

The demo keeps the data plane and engine adapters independent from the catalog.

`polaris-minimal` is the small compatibility service used by the local Docker
Compose demo. `polaris-extension` contains the FGAC REST resource and its test in
the directory layout expected by the full Apache Polaris source tree.

The full integration is based on:

```text
tag:    apache-polaris-1.7.0
commit: 4ac2f059d1cce149453d0a5f1ff1dff980ec97cc
```

To validate the full integration, copy the extension tree over that exact source
revision and run the Polaris hard gates:

```text
./gradlew format compileAll
./gradlew :polaris-runtime-service:check
```

The minimal service is a demo fixture with fixed Alice/Bob identities. It is not
a replacement for Polaris authentication, persistence, or production policy
management.
