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

![Toucan 2](https://github.com/camsaul/toucan2/raw/master/assets/toucan2.png)

Toucan 2 is a library for delightful database interaction.

At the end of the day, almost every non-trivial project needs to interact with a
database. Toucan 2 makes its easy to query the data in your database in a
consistent way and define behaviors that should happen every time a row is
retrieved, created, updated, or deleted.

Toucan 2 is a successor library to [Toucan](https://github.com/metabase/toucan)
with a modern and more-extensible API, more consistent behavior (less gotchas),
support for different backends including non-JDBC databases and non-HoneySQL
queries, support for namespaced keywords, and with more useful utilities.

Toucan 2 uses [Honey SQL 2](https://github.com/seancorfield/honeysql),
[`next.jdbc`](https://github.com/seancorfield/next-jdbc), and
[Methodical](https://github.com/camsaul/methodical) under the hood. Everything
is super efficient and reducible under the hood (even inserts, updates, and
deletes!) and magical (in a good way).

## You Can Toucan: 12 Cool Things You Can Do with Toucan 2

### REPL-friendly syntax

Toucan 2 is optimized to for *REPL-driven development*, It offers a high-level
interface that's quick and easy to use from the REPL.

Compare a simple query with `next.jdbc` vs the equivalent way to do it in Toucan:

```clj
(require '[next.jdbc :as jdbc])

(def db-spec
  {:dbtype   "postgresql"
   :dbname   "toucan2"
   :host     "localhost"
   :post     5432
   :user     "cam"
   :password "cam"})

;;; next.jdbc

(let [my-datasource (jdbc/get-datasource db-spec)]
  (with-open [connection (jdbc/get-connection my-datasource)]
    (jdbc/execute! connection ["SELECT * FROM people WHERE name = ?" "Cam"])))
;; =>
[{:people/id 1
  :people/name "Cam"
  :people/created-at #inst "2020-04-21T23:56:00.000000000-00:00"}]

;;; Toucan 2

(require '[toucan2.core :as t2])

(t2/select :conn db-spec "people" :name "Cam")
;; =>
[{:id 1
  :name "Cam"
  :created-at #object[java.time.OffsetDateTime 0x2c8ec7ed "2020-04-21T23:56Z"]}]

;;; next.jdbc + Honey SQL 2

(require '[honey.sql :as sql])

(let [my-datasource (jdbc/get-datasource db-spec)]
  (with-open [connection (jdbc/get-connection my-datasource)]
    (jdbc/execute! connection (sql/format {:select [:*]
                                           :from   [:people]
                                           :where  [:like :name "C%"]}))))
;; =>
[{:people/id 1
  :people/name "Cam"
  :people/created-at #inst "2020-04-21T23:56:00.000000000-00:00"}]

;;; Toucan 2

(t2/select :conn db-spec "people" :name [:like "C%"])
;; =>
[{:id 1
  :name "Cam"
  :created-at #object[java.time.OffsetDateTime 0x2c8ec7ed "2020-04-21T23:56Z"]}]
```

### Define a default connection

As you can see, Toucan 2 is quick and easy to use from the REPL. But it can be
even easier! Typing `dbspec` over and over can get tedious. Toucan 2 lets you
define a default connection:

```clj
(require '[methodical.core :as m])

(m/defmethod t2/do-with-connection :default
  [_connectable f]
  (t2/do-with-connection db-spec f))

(t2/select "people" :name [:like "C%"])
;; =>
[{:id 1
  :name "Cam"
  :created-at #object[java.time.OffsetDateTime 0x2bf3111d "2020-04-21T23:56Z"]}]
```

You can also define a default connection for specific tables (*models*). For
more information, see [Connections](/docs/connections.md).

### Define models

As we'll see below, you can define lots of arbitrary behaviors when selecting,
updating, inserting, and deleting rows from various tables in your database.
These behaviors are encapsulated in multimethods that are triggered for what
Toucan 2 calls *models*, which are usually just plain Clojure keywords.

By default, Toucan 2 will use the `name` of a model keyword as the table name to
use when querying it. Let's try using a model called `:model/people`:

```clj
(t2/table-name :model/people)
;; => :people

;; Select one row from :people with the primary key 1
(t2/select-one :model/people 1)
;; =>
{:id 1
 :name "Cam"
 :created-at #object[java.time.OffsetDateTime 0x2c8ec7ed "2020-04-21T23:56Z"]}
```

Going forward, we'll be deriving a lot of models from `:models/people`. To make
new models like `:models/people.cool` use the right table name, let's tell
Toucan 2 to always use `people` as the table name for anything deriving from
`:models/people`:

```clj
(m/defmethod t2/table-name :models/people
  [_model]
  :people)
```

To learn more about models, see [Models](/docs/models.md).

### Define transforms

Toucan 2 is much more than just a glorified collection of helper functions.
Suppose we have a column that we would like to automatically be converted to
keywords whenever it comes out of the database, and automatically be converted
back to strings when it goes back into the database. With Toucan 2, you can
easily define column transformations with
[`deftransforms`](https://cljdoc.org/d/io.github.camsaul/toucan2/CURRENT/api/toucan2.tools.transformed#deftransforms).

Let's define a new model, so we don't affect `:models/people` itself.

```clj
(derive :models/people.keyword-name :models/people)

(t2/deftransforms :models/people.keyword-name
  {:name {:in  name
          :out keyword}})

(t2/select :models/people.keyword-name :name :Cam)
;; =>
[{:id 1
  :name :Cam
  :created-at #object[java.time.OffsetDateTime 0x3af24cf "2020-04-21T23:56Z"]}]
```

Whenever a non-nil value goes in to the database, Toucan 2 calls `name` on it;
when a non-nil value comes out, Toucan 2 calls `keyword` on it. For more info,
see [Transforms](/docs/transforms.md).

### Define behavior after selecting something

Sometimes we want to do more general things than just transform a single column
to another value. You can use tools like
[`define-after-select`](https://cljdoc.org/d/io.github.camsaul/toucan2/CURRENT/api/toucan2.tools.after-select#define-after-select)
to define more general transformations for results, such as adding additional
columns, or to trigger some other behavior for side effects. Let's say we want
to give all our people cool names when they come out of the database.

```clj
(derive :models/people.cool :models/people)

(t2/define-after-select :models/people.cool
  [person]
  (assoc person :cool-name (str "Cool " (:name person))))

(t2/select-one :models/people.cool 1)
;; =>
{:name       "Cam"
 :cool-name  "Cool Cam"
 :id         1
 :created-at #object[java.time.OffsetDateTime 0x2691c719 "2020-04-21T23:56Z"]}
```

This method is not applied when you use `:models/people` or other models derived
from it, unless those models derive from `:models/people.cool`. You can define
`before-` and `after-` methods for `select`, `insert`, `update`, and `delete`.
For more information, see [Before & After Methods](/docs/before-and-after.md).

### Compose behaviors

Because Toucan 2 is implemented with multimethods, you can compose various
behaviors by simply deriving a model from one or more others. Built-in Toucan 2
tools like
[`deftransforms`](https://cljdoc.org/d/io.github.camsaul/toucan2/CURRENT/api/toucan2.tools.transformed#deftransforms)
and
[`define-after-select`](https://cljdoc.org/d/io.github.camsaul/toucan2/CURRENT/api/toucan2.tools.after-select#define-after-select)
automatically compose.

Let's define another after-select method, `::without-created-at`, to remove the
`:created-at` key from our results, then create a new model that combines
`:models/people.cool` with `::without-created-at` to get **both** behaviors:

```clj
(t2/define-after-select ::without-created-at
  [row]
  (dissoc row :created-at))

(derive :models/people.cool.without-created-at :models/people.cool)
(derive :models/people.cool.without-created-at ::without-created-at)

;;; Tell Methodical to call the :models.people.cool method before the
;;; ::without-created-at method
(m/prefer-method! #'toucan2.tools.after-select/after-select
                  :models/people.cool
                  ::without-created-at)

(t2/select-one :models/people.cool.without-created-at 1)
;; =>
{:name "Cam", :cool-name "Cool Cam", :id 1}
```

### Define named queries

Often you'll want to write a big complicated query:

```clj
(t2/select "venues" {:select    [:venues.id
                                 [:venues.name :venue-name]
                                 [:category.name :category-name]
                                 [:category.slug :category-slug]]
                     :from      [:venues]
                     :left-join [:category [:= :venues.category :category.name]]})
;; =>
[{:id 1, :venue-name "Tempest", :category-name "bar", :category-slug "bar_01"}
 {:id 2, :venue-name "Ho's Tavern", :category-name "bar", :category-slug "bar_01"}
 {:id 3, :venue-name "BevMo", :category-name "store", :category-slug "store_02"}
 ...]
```

This is not REPL-friendly! With Toucan 2, you can use
[`define-named-query`](https://cljdoc.org/d/io.github.camsaul/toucan2/CURRENT/api/toucan2.tools.named-query#define-named-query)
to define named queries to use again later:

```clj
(t2/define-named-query ::venues-with-categories
  {:select    [:venues.id
               [:venues.name :venue-name]
               [:category.name :category-name]
               [:category.slug :category-slug]]
   :from      [:venues]
   :left-join [:category [:= :venues.category :category.name]]})

(t2/select "venues" ::venues-with-categories)
;; =>
[{:id 1, :venue-name "Tempest", :category-name "bar", :category-slug "bar_01"}
 {:id 2, :venue-name "Ho's Tavern", :category-name "bar", :category-slug "bar_01"}
 {:id 3, :venue-name "BevMo", :category-name "store", :category-slug "store_02"}
 ...]
```

You can even combine those queries with additional constraints:

```clj
(t2/select "venues" :venues.name [:like "Temp%"] ::venues-with-categories)
;; =>
{:id 1, :venue-name "Tempest", :category-name "bar", :category-slug "bar_01"}
```

You can even define different versions of named queries to use for different
models. For example, you may want to have some sort of analytics query for
several different tables in your database. Define a different version of
`:analytics-query` for each of them!

To learn more about query resolution and named queries, see [Query
Resolution](/docs/query-resolution.md).

### Define default fields

TODO

### Disallow update, delete, or insert

TODO

### Get the model associated with an instance

TODO

### Record and `save!` changes made to an instance

TODO

### Define custom keyword-arg behavior

TODO

For more information, see [Query Compilation & Map Backends](query-compilation.md)

# `toucan2-toucan1`

[![Clojars Project](https://clojars.org/io.github.camsaul/toucan2-toucan1/latest-version.svg)](https://clojars.org/io.github.camsaul/toucan2-toucan1)

Compatibility layer for projects using [Toucan
1](https://github.com/metabase/toucan) to ease transition to Toucan 2.
Implements the same namespaces as Toucan 1, but they are implemented on top of
Toucan 2. Projects using Toucan 1 can remove their dependency on `toucan`, and
include a dependency on `io.github.camsaul/toucan2-toucan1` in its place; with a
few changes your project should work without having to switch everything to
Toucan 2 all at once. More details on this coming soon.

See [`toucan2-toucan1` docs](/toucan1/README.md) for more information.

## License

Code, documentation, and artwork copyright © 2017-2023 [Cam
Saul](https://camsaul.com).

Distributed under the [Eclipse Public
License](https://raw.githubusercontent.com/camsaul/toucan2/master/LICENSE), same
as Clojure.
