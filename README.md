<!-- [![Circle CI](https://circleci.com/gh/camsaul/toucan2.svg?style=svg)](https://circleci.com/gh/camsaul/toucan2) -->
[![License](https://img.shields.io/badge/license-Eclipse%20Public%20License-blue.svg)](https://raw.githubusercontent.com/camsaul/toucan2/master/LICENSE)
<!-- [![cljdoc badge](https://cljdoc.org/badge/com.camsaul/toucan2)](https://cljdoc.org/d/com.camsaul/toucan2/CURRENT) -->
<!-- [![Downloads](https://versions.deps.co/camsaul/toucan2/downloads.svg)](https://versions.deps.co/camsaul/toucan2) -->
<!-- [![Dependencies Status](https://versions.deps.co/camsaul/toucan2/status.svg)](https://versions.deps.co/camsaul/toucan2) -->
<!-- [![codecov](https://codecov.io/gh/camsaul/toucan2/branch/master/graph/badge.svg)](https://codecov.io/gh/camsaul/toucan2) -->

<!-- [![Clojars Project](https://clojars.org/com.camsaul/toucan2/latest-version.svg)](https://clojars.org/com.camsaul/toucan2) -->

[![Get help on Slack](http://img.shields.io/badge/slack-clojurians%20%23toucan-4A154B?logo=slack&style=for-the-badge)](https://clojurians.slack.com/channels/toucan)

# Toucan 2

![Toucan 2](https://github.com/camsaul/toucan2/blob/master/assets/toucan2.png)

Toucan 2 is a successor library to [Toucan](https://github.com/metabase/toucan) with a modern and more-extensible API,
more consistent behavior, and support for different backends including non-JDBC databases and non-HoneySQL queries.

After working on this off and on for several years I decided that the library was overly complicated (at nearly 8000
lines of code) and decided to start over and re-implement things in a simpler fashion. The old library was mostly
usable and tested and several alpha releases are available on Clojars. It's available in the
[`old-implementation`](https://github.com/camsaul/toucan2/tree/old-implementation) branch.

The re-implementation is pretty close to being ready for alpha usage and I will update the README when I publish the
first alpha release.

## License

Code, documentation, and artwork copyright © 2017-2022 [Cam Saul](https://camsaul.com).

Distributed under the [Eclipse Public License](https://raw.githubusercontent.com/camsaul/toucan2/master/LICENSE),
same as Clojure.
