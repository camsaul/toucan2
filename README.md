[![License](https://img.shields.io/badge/license-Eclipse%20Public%20License-blue.svg?style=for-the-badge)](https://raw.githubusercontent.com/camsaul/toucan2/master/LICENSE)
[![GitHub last commit](https://img.shields.io/github/last-commit/camsaul/toucan2?style=for-the-badge)](https://github.com/camsaul/toucan2/commits/)
[![Codecov](https://img.shields.io/codecov/c/github/camsaul/toucan2?style=for-the-badge)](https://codecov.io/gh/camsaul/toucan2)
[![GitHub Sponsors](https://img.shields.io/github/sponsors/camsaul?style=for-the-badge)](https://github.com/sponsors/camsaul)
[![cljdoc badge](https://img.shields.io/badge/dynamic/json?color=informational&label=cljdoc&query=results%5B%3F%28%40%5B%22artifact-id%22%5D%20%3D%3D%20%22toucan2%22%29%5D.version&url=https%3A%2F%2Fcljdoc.org%2Fapi%2Fsearch%3Fq%3Dio.github.camsaul%2Ftoucan2&style=for-the-badge)](https://cljdoc.org/d/io.github.camsaul/toucan2/CURRENT)
[![Get help on Slack](http://img.shields.io/badge/slack-clojurians%20%23toucan-4A154B?logo=slack&style=for-the-badge)](https://clojurians.slack.com/channels/toucan)

<!-- [![Downloads](https://versions.deps.co/camsaul/toucan2/downloads.svg)](https://versions.deps.co/camsaul/toucan2) -->
<!-- [![Dependencies Status](https://versions.deps.co/camsaul/toucan2/status.svg)](https://versions.deps.co/camsaul/toucan2) -->

[![Clojars Project](https://clojars.org/io.github.camsaul/toucan2/latest-version.svg)](https://clojars.org/io.github.camsaul/toucan2)

# T2: Toucan 2

![Toucan 2](https://github.com/camsaul/toucan2/blob/master/assets/toucan2.png)

Toucan 2 is a successor library to [Toucan](https://github.com/metabase/toucan) with a modern and more-extensible API,
more consistent behavior (less gotchas), support for different backends including non-JDBC databases and non-HoneySQL
queries, and more useful utilities.

Toucan 2 uses [Honey SQL 2](https://github.com/seancorfield/honeysql),
[`next.jdbc`](https://github.com/seancorfield/next-jdbc) (a bit), and
[Methodical](https://github.com/camsaul/methodical) under the hood. Everything is super efficient and reducible under
the hood (even inserts, updates, and deletes!) and magical (in a good way).

The library is mostly usable now but I have a big pile of things to do before the
official announcement -- [take a look at the Trello board](https://trello.com/b/DFx8rVa8/toucan-2-todo) -- when things
get a little further along I'll move things over to GitHub issues. I will update this README when I publish the first
alpha release.

# `toucan2-toucan1`

[![Clojars Project](https://clojars.org/io.github.camsaul/toucan2-toucan1/latest-version.svg)](https://clojars.org/io.github.camsaul/toucan2-toucan1)

Compatibility layer for projects using [Toucan 1](https://github.com/metabase/toucan) to ease transition to Toucan 2.
Implements the same namespaces as Toucan 1, but they are implemented on top of Toucan 2. Projects using Toucan 1 can
remove their dependency on `toucan`, and include a dependency on `io.github.camsaul/toucan2-toucan1` in its place;
with a few changes your project should work without having to switch everything to Toucan 2 all at once. More details
on this coming soon.

See [`toucan2-toucan1` docs](toucan1/README.md) for more information.

## License

Code, documentation, and artwork copyright © 2017-2022 [Cam Saul](https://camsaul.com).

Distributed under the [Eclipse Public License](https://raw.githubusercontent.com/camsaul/toucan2/master/LICENSE),
same as Clojure.
