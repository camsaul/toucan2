[![GitHub Workflow Status (branch)](https://img.shields.io/github/workflow/status/camsaul/toucan2/Tests/master?style=for-the-badge)](https://github.com/camsaul/toucan2/actions/workflows/config.yml)
[![License](https://img.shields.io/badge/license-Eclipse%20Public%20License-blue.svg?style=for-the-badge)](https://raw.githubusercontent.com/camsaul/toucan2/master/LICENSE)
[![GitHub last commit](https://img.shields.io/github/last-commit/camsaul/toucan2?style=for-the-badge)](https://github.com/camsaul/toucan2/commits/)
[![codecov](https://codecov.io/gh/camsaul/toucan2/branch/master/graph/badge.svg?token=gpq2jMRVzI)](https://codecov.io/gh/camsaul/toucan2)
[![GitHub Sponsors](https://img.shields.io/github/sponsors/camsaul?style=for-the-badge)](https://github.com/sponsors/camsaul)
[![Get help on Slack](http://img.shields.io/badge/slack-clojurians%20%23toucan-4A154B?logo=slack&style=for-the-badge)](https://clojurians.slack.com/channels/toucan)
<!-- [![cljdoc badge](https://cljdoc.org/badge/com.camsaul/toucan2)](https://cljdoc.org/d/com.camsaul/toucan2/CURRENT) -->
<!-- [![Downloads](https://versions.deps.co/camsaul/toucan2/downloads.svg)](https://versions.deps.co/camsaul/toucan2) -->
<!-- [![Dependencies Status](https://versions.deps.co/camsaul/toucan2/status.svg)](https://versions.deps.co/camsaul/toucan2) -->
<!-- [![Clojars Project](https://clojars.org/com.camsaul/toucan2/latest-version.svg)](https://clojars.org/com.camsaul/toucan2) -->

# Toucan 2

![Toucan 2](https://github.com/camsaul/toucan2/blob/master/assets/toucan2.png)

Toucan 2 is a successor library to [Toucan](https://github.com/metabase/toucan) with a modern and more-extensible API,
more consistent behavior (less gotchas), support for different backends including non-JDBC databases and non-HoneySQL queries, and more useful utilities.

Toucan 2 uses [Honey SQL 2](https://github.com/seancorfield/honeysql), [`next.jdbc`](https://github.com/seancorfield/next-jdbc) (a bit), and [Methodical](https://github.com/camsaul/methodical) under the hood. Everything is super efficient and reducible under the hood (even inserts, updates, and deletes!) and magical (in a good way).

After working on this off and on for several years I decided that the library was overly complicated (at nearly 8000
lines of code) and decided to start over and re-implement things in a ~simpler fashion~ (EDIT: back up to 7000 LoC, but it's easier to use this time). The library is mostly usable now but I have a big pile of things to do before the official announcement -- [take a look at the Trello board](https://trello.com/b/DFx8rVa8/toucan-2-todo) -- when things get a little further along I'll move things over to GitHub issues. I will update this README when I publish the
first alpha release.

In the mean time, browse the [Documentation](docs/), which are a work in progress.

## License

Code, documentation, and artwork copyright © 2017-2022 [Cam Saul](https://camsaul.com).

Distributed under the [Eclipse Public License](https://raw.githubusercontent.com/camsaul/toucan2/master/LICENSE),
same as Clojure.
