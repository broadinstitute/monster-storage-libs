# Monster Storage Libraries
Utilities used by the Monster team in DSP to interact with various storage systems (GCS, S3, FTP...)

## Motivation
Monster team's ingest work can be split into two "levels":
1. Low-level logic for interacting with specific storage back-ends
2. Higher-level logic orchestrating how information pulled from one back-end flows into another

When the two levels are intertwined, testing each in isolation becomes more difficult.
This repository aims to isolate the first level of work into a package which can be independently
tested, published, and used as needed.

## Why a separate repository?
Splitting code across repositories always introduces some operational overhead. We believe the
overhead is worth it in this case, vs. the monorepo alternative, because:
* Multiple Monster services will need low-level storage functionality. A monorepo approach would
  require going all-in and hosting every Monster service in one location.
* Our versioning schemes are driven by git tags. Since there's no way to tag a subset of a
  repository, a monorepo containing every Monster service would result in a new release for
  _every_ service even if only a subset had changed.
* After the initial wave of development we expect the logic in this repository to remain relatively
  stable, reducing the overhead of the separate library in practice.

## Using the library
Add the following to your `build.sbt` to pull in a specific module:
```sbt
resolvers in ThisBuild +=
  "Broad Artifactory Releases" at "https://broadinstitute.jfrog.io/broadinstitute/libs-release/"

libraryDependencies += "org.broadinstitute.monster" %% "<backend>-lib" % "<version>"
```

## Available libraries
| Backend | Module Name | Read | Write |
| ------- | ----------- | ---- | ----- |
| Google Cloud Storage | `gcs-lib` | yes | yes |
| FTP | `ftp-lib` | yes | no |
| SFTP | `sftp-lib` | yes | no |
