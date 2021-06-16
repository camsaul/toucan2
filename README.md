[![Circle CI](https://circleci.com/gh/camsaul/toucan2.svg?style=svg)](https://circleci.com/gh/camsaul/toucan2)
[![License](https://img.shields.io/badge/license-Eclipse%20Public%20License-blue.svg)](https://raw.githubusercontent.com/camsaul/toucan2/master/LICENSE)
[![cljdoc badge](https://cljdoc.org/badge/com.camsaul/toucan2)](https://cljdoc.org/d/com.camsaul/toucan2/CURRENT)
<!-- [![Downloads](https://versions.deps.co/camsaul/toucan2/downloads.svg)](https://versions.deps.co/camsaul/toucan2) -->
<!-- [![Dependencies Status](https://versions.deps.co/camsaul/toucan2/status.svg)](https://versions.deps.co/camsaul/toucan2) -->
<!-- [![codecov](https://codecov.io/gh/camsaul/toucan2/branch/master/graph/badge.svg)](https://codecov.io/gh/camsaul/toucan2) -->

[![Clojars Project](https://clojars.org/com.camsaul/toucan2/latest-version.svg)](https://clojars.org/com.camsaul/toucan2)

[![Get help on Slack](http://img.shields.io/badge/slack-clojurians%20%23toucan-4A154B?logo=slack&style=for-the-badge)](https://clojurians.slack.com/channels/toucan)

# Toucan 2

![Toucan 2](https://github.com/camsaul/toucan2/blob/master/assets/toucan2.png)

Toucan 2 is a successor library to [Toucan](https://github.com/metabase/toucan) with a modern and more-extensible API,
more consistent behavior, and support for different backends including non-JDBC databases and non-HoneySQL queries.

Documentation and examples will be added when things are a little further along.

## Subprojects

| Subproject | Description | Version |
| -- | -- | -- |
| [`toucan2-core`](https://github.com/camsaul/toucan2/tree/master/toucan2-core) | Core Toucan 2 API and utils | [![Clojars Project](https://clojars.org/com.camsaul/toucan2-core/latest-version.svg)](https://clojars.org/com.camsaul/toucan2-core) | 
| [`toucan2-honeysql`](https://github.com/camsaul/toucan2/tree/master/toucan2-honeysql) | Toucan 2 [HoneySQL](https://github.com/seancorfield/honeysql) Integration | [![Clojars Project](https://clojars.org/com.camsaul/toucan2-honeysql/latest-version.svg)](https://clojars.org/com.camsaul/toucan2-honeysql) |
| [`toucan2-jdbc`](https://github.com/camsaul/toucan2/tree/master/toucan2-jdbc) | Toucan 2 [`next.jdbc`](https://github.com/seancorfield/next-jdbc) Backend | [![Clojars Project](https://clojars.org/com.camsaul/toucan2-jdbc/latest-version.svg)](https://clojars.org/com.camsaul/toucan2-jdbc) |
| [`toucan2`](https://github.com/camsaul/toucan2/tree/master/toucan2) | `toucan2-core` + `toucan2-honeysql` + `toucan2-jdbc` | [![Clojars Project](https://clojars.org/com.camsaul/toucan2/latest-version.svg)](https://clojars.org/com.camsaul/toucan2) |
| [`toucan2-toucan1`](https://github.com/camsaul/toucan2/tree/master/toucan2-toucan1) | Legacy [Toucan 1](https://github.com/metabase/toucan) Compatibility | [![Clojars Project](https://clojars.org/com.camsaul/toucan2-toucan1/latest-version.svg)](https://clojars.org/com.camsaul/toucan2-toucan1) |
| [`toucan2-schema`](https://github.com/camsaul/toucan2/tree/master/toucan2-schema) | [Plumatic Schema](https://github.com/plumatic/schema) integration for Toucan 2 | [![Clojars Project](https://clojars.org/com.camsaul/toucan2-schema/latest-version.svg)](https://clojars.org/com.camsaul/toucan2-schema) |

## Development

Run tests for *all* subprojects with

```bash
clojure -X:dev:test
```

Run the Eastwood linter for *all* subprojects with

```bash
clojure -M:dev:eastwood
```

## License

Code, documentation, and artwork copyright © 2021 [Cam Saul](https://camsaul.com).

Distributed under the [Eclipse Public License](https://raw.githubusercontent.com/camsaul/toucan2/master/LICENSE),
same as Clojure.
