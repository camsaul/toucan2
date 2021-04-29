[![Downloads](https://versions.deps.co/camsaul/bluejdbc/downloads.svg)](https://versions.deps.co/camsaul/bluejdbc)
[![Dependencies Status](https://versions.deps.co/camsaul/bluejdbc/status.svg)](https://versions.deps.co/camsaul/bluejdbc)
[![Circle CI](https://circleci.com/gh/camsaul/bluejdbc.svg?style=svg)](https://circleci.com/gh/camsaul/bluejdbc)
[![codecov](https://codecov.io/gh/camsaul/bluejdbc/branch/master/graph/badge.svg)](https://codecov.io/gh/camsaul/bluejdbc)
[![License](https://img.shields.io/badge/license-Eclipse%20Public%20License-blue.svg)](https://raw.githubusercontent.com/camsaul/bluejdbc/master/LICENSE.txt)
[![cljdoc badge](https://cljdoc.org/badge/bluejdbc/bluejdbc)](https://cljdoc.org/d/bluejdbc/bluejdbc/CURRENT)

[![Clojars Project](https://clojars.org/bluejdbc/latest-version.svg)](http://clojars.org/bluejdbc)

# Blue JDBC

![Blue JDBC](https://github.com/camsaul/bluejdbc/blob/master/assets/bluejdbc.png)

Blue JDBC is a new Clojure interface to JDBC with an an emphasis on ease-of-use, performance, extensibility, and
complete documentation.

<!-- [Documentation is available here.](docs/). -->

### Key benefits!

* Blue JDBC automatically uses `java.time` everywhere!
* Blue JDBC supports [HoneySQL](https://github.com/jkk/honeysql) everywhere and its functions. Don't worry -- you can
  still use raw SQL everywhere as well!


#### Maps keep track of their changes!

```clj

(def m (db/select-one :people))
m
;; -> (bluejdbc.instance/instance :people {:first-name "Cam", :last-name "Era"})

(def m (assoc m :last-name "Era"))
m
;; -> (bluejdbc.instance/instance :people {:first-name "Cam", :last-name "Era"})

(original m)
;; -> {:first-name "Cam", :last-name "Saul"}

(changes m)
;; -> {:last-name "Era"}

(save! m)
```
## License

Code, documentation, and artwork copyright © 2021 [Cam Saul](https://camsaul.com).

Distributed under the [Eclipse Public License](https://raw.githubusercontent.com/camsaul/bluejdbc/master/LICENSE),
same as Clojure.
