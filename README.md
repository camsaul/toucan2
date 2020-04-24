[![Downloads](https://versions.deps.co/camsaul/bluejdbc/downloads.svg)](https://versions.deps.co/camsaul/bluejdbc)
[![Dependencies Status](https://versions.deps.co/camsaul/bluejdbc/status.svg)](https://versions.deps.co/camsaul/bluejdbc)
[![Circle CI](https://circleci.com/gh/camsaul/bluejdbc.svg?style=svg)](https://circleci.com/gh/camsaul/bluejdbc)
[![codecov](https://codecov.io/gh/camsaul/bluejdbc/branch/master/graph/badge.svg)](https://codecov.io/gh/camsaul/bluejdbc)
[![License](https://img.shields.io/badge/license-Eclipse%20Public%20License-blue.svg)](https://raw.githubusercontent.com/camsaul/bluejdbc/master/LICENSE.txt)
[![cljdoc badge](https://cljdoc.org/badge/bluejdbc/bluejdbc)](https://cljdoc.org/d/bluejdbc/bluejdbc/CURRENT)

[![Clojars Project](https://clojars.org/bluejdbc/latest-version.svg)](http://clojars.org/bluejdbc)

# Blue JDBC

![Blue JDBC](https://github.com/camsaul/bluejdbc/blob/master/assets/bluejdbc.png)

Blue JDBC is a new Clojure interface to JDBC with an an emphasis on ease-of-use, performance, extensibility.

### Key benefits!

* Blue JDBC automatically uses `java.time` everywhere!
* Blue JDBC supports [HoneySQL](https://github.com/jkk/honeysql) everywhere and its functions. Don't worry -- you can still use raw SQL everywhere as well!
* Blue JDBC's versions of `java.sql.PreparedStatement` and `java.sql.ResultSet` are reducible/transducible and sequable. Wow!
* Blue JDBC handles parameters and results in a database-aware fashion -- for example, you can have Blue JDBC convert a `UUID` to a String for MySQL and a byte array for Postgres!
* Has its own logo!

```clj
(jdbc/query "jdbc:postgresql://localhost:5432/test-data?user=cam&password=cam"
            "SELECT now() AS now;")
;; ->
[{:now #object[java.time.OffsetDateTime 0x75355ea0 "2020-04-22T05:36:46.257644Z"]}]

(jdbc/query-one a-datasource ; instance of java.sqlx.DataSource
                {:select    [:*]
                 :from      [:checkins]
                 :left-join [:venues [:= :checkins.venue_id :venues.id]]
                 :where     [:= :venues.id 25]}
                {:results/xform (jdbc/maps :namespaced)}
;; ->
{:checkins/date      #object[java.time.LocalDate 0x60156e1b "2014-11-16"]
 :checkins/id        12
 :checkins/user_id   5
 :checkins/venue_id  25
 :venues/category_id 50
 :venues/id          25
 :venues/latitude    37.7818
 :venues/longitude   -122.396
 :venues/name        "Garaje"
 :venues/price       2}
```

### Rationale

One might wonder whether the world needs another Clojure JDBC library. For many years,
[`clojure.java.jdbc`](https://github.com/clojure/java.jdbc) was the go-to Clojure JDBC library. I've personally spent
hundreds if not thousands of hours working with it as part of [Metabase](https://github.com/metabase/metabase).

Sean Corfield's successor library, [`next.jdbc`](https://github.com/seancorfield/next-jdbc), [fixes some of the
shortcomings of `clojure.java.jdbc` and makes some big improvements](https://corfield.org/blog/2019/07/04/next-jdbc/).

Blue JDBC shares a similar role as `next.jdbc` as a successor to `clojure.java.jdbc`, but with slightly different aims:

*  Blue JDBC aims to be REPL-friendly and "batteries included". Feature like being able to use HoneySQL anywhere you
   can use SQL and out-of-the-box support for `java.time`, currency types, Postgres JSON columns, and more mean the
   low-level boilerplate is part of the library and not your part of your project.
*  Blue JDBC aims to be more flexible and customizable and is fully database-aware

### Development & Tests

Blue JDBC currently runs tests against:

*  Postgres 9.6
*  Postgres latest
*  H2
*  MySQL 5.7
*  MySQL latest
*  MariaDB 10.2
*  MariaDB latest
*  SQL Server 2017

To run tests against a DB of your choice, set the environment variable `JDBC_URL` and run `lein test`:

```bash
JDBC_URL='jdbc:postgresql://localhost:5432/bluejdbc_test?user=cam&password=cam' lein test
```

Blue JDBC will automatically do the right thing based on the protocol.

## License

Code, documentation, and artwork copyright © 2020 [Cam Saul](https://camsaul.com).

Distributed under the [Eclipse Public License](https://raw.githubusercontent.com/camsaul/bluejdbc/master/LICENSE),
same as Clojure.
