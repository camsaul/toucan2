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

### Demo

##### Define a connection.

```clj
(require '[bluejdbc.core :as blue])

(blue/defmethod blue/connection* ::pg-connection
  [_ options]
  (blue/connection* "jdbc:postgresql://localhost:5432/bluejdbc?user=cam&password=cam" options))
```

##### Use the built-in Postgres integrations, and enable java-time support.

```clj
(require 'bluejdbc.integrations.postgresql)
(derive ::pg-connection :bluejdbc.jdbc/postgresql)
```

##### Run a query.

```clj
(blue/select [::pg-connection :people])
;; =>
[(blue/instance :people {:id 1, :name "Cam", :created_at (t/offset-date-time "2020-04-21T23:56:00Z")})
 (blue/instance :people {:id 2, :name "Sam", :created_at (t/offset-date-time "2019-01-11T23:56:00Z")})
 (blue/instance :people {:id 3, :name "Pam", :created_at (t/offset-date-time "2020-01-01T21:56:00Z")})
 (blue/instance :people {:id 4, :name "Tam", :created_at (t/offset-date-time "2020-05-25T19:56:00Z")})]
```

##### Define the default connection.

```clj
(blue/defmethod blue/connection* :bluejdbc/default
  [_ options]
  (blue/connection* ::pg-connection options))
```

##### Some basic queries with the default connection.

```clj
(blue/select :people)
;; ->
[(blue/instance :people {:id 1, :name "Cam", :created_at (t/offset-date-time "2020-04-21T23:56:00Z")})
 (blue/instance :people {:id 2, :name "Sam", :created_at (t/offset-date-time "2019-01-11T23:56:00Z")})
 (blue/instance :people {:id 3, :name "Pam", :created_at (t/offset-date-time "2020-01-01T21:56:00Z")})
 (blue/instance :people {:id 4, :name "Tam", :created_at (t/offset-date-time "2020-05-25T19:56:00Z")})]
```

###### Fetch people with PK = 1

```clj
(blue/select :people 1)
;; ->
[(blue/instance :people {:id 1, :name "Cam", :created_at (t/offset-date-time "2020-04-21T23:56:00Z")})]
```

###### Fetch people with :name = Cam

```clj
(blue/select :people :name "Cam")
;; ->
[(blue/instance :people {:id 1, :name "Cam", :created_at (t/offset-date-time "2020-04-21T23:56:00Z")})]
```

###### Fetch :people with arbitrary HoneySQL

```clj
(blue/select :people {:order-by [[:id :desc]], :limit 1})
;; ->
[(blue/instance :people {:id 4, :name "Tam", :created_at (t/offset-date-time "2020-05-25T19:56:00Z")})]
```

##### "Pre-select"

```clj
;; create a new version of the "people" table that returns ID and name by default.
(blue/defmethod blue/select* :before [::pg-connection :people/default-fields clojure.lang.IPersistentMap]
  [_ _ query _]
  (merge {:select [:id :name]} query))

(blue/select [::pg-connection :people/default-fields])
;; ->
[(blue/instance :people/default-fields {:id 1, :name "Cam"})
 (blue/instance :people/default-fields {:id 2, :name "Sam"})
 (blue/instance :people/default-fields {:id 3, :name "Pam"})
 (blue/instance :people/default-fields {:id 4, :name "Tam"})]
```

##### "Post-select"

```clj
;; create a new version of "people" that converts :name to lowercase.
(blue/defmethod blue/select* :after [::pg-connection :people/lower-case-names :default]
  [_ _ reducible-query _]
  (eduction
   (map (fn [result]
          (update result :name str/lower-case)))
   reducible-query))

(blue/select [::pg-connection :people/lower-case-names])
;; ->
[(blue/instance :people/lower-case-names {:id 1, :name "cam", :created_at (t/offset-date-time "2020-04-21T23:56:00Z")})
 (blue/instance :people/lower-case-names {:id 2, :name "sam", :created_at (t/offset-date-time "2019-01-11T23:56:00Z")})
 (blue/instance :people/lower-case-names {:id 3, :name "pam", :created_at (t/offset-date-time "2020-01-01T21:56:00Z")})
 (blue/instance :people/lower-case-names {:id 4, :name "tam", :created_at (t/offset-date-time "2020-05-25T19:56:00Z")})]
```

##### Combine multiple actions together!

```clj
(derive :people/default :people/default-fields)
(derive :people/default :people/lower-case-names)

(blue/select [::pg-connection :people/default])
;; ->
[(blue/instance :people/default {:id 1, :name "cam"})
 (blue/instance :people/default {:id 2, :name "sam"})
 (blue/instance :people/default {:id 3, :name "pam"})
 (blue/instance :people/default {:id 4, :name "tam"})]
```

##### Use a non-integer or non-`:id` primary key

```clj
(blue/defmethod blue/primary-key* [::pg-connection :people/name-pk]
  [_ _]
  :name)

(blue/select [::pg-connection :people/name-pk] "Cam")
;; ->
[(blue/instance :people/name-pk {:id 1, :name "Cam", :created_at (t/offset-date-time "2020-04-21T23:56:00Z")})]
```

##### Use a composite PK (!)

```clj
(blue/defmethod blue/primary-key* [::pg-connection :people/composite-pk]
  [_ _]
  [:id :name])

(blue/select [::pg-connection :people/composite-pk] [1 "Cam"])
;; ->
[(blue/instance :people/composite-pk {:id 1, :name "Cam", :created_at (t/offset-date-time "2020-04-21T23:56:00Z")})]
```

##### maps keep track of their changes!

```clj
(def cam (blue/select-one :people 1))
(def cam-2 (assoc cam :name "Cam 2.0"))

(blue/original cam-2)
;; ->
{:id 1, :name "Cam", :created_at (t/offset-date-time "2020-04-21T23:56:00Z")}

(blue/changes cam-2)
;; ->
{:name "Cam 2.0"}
```

##### save something!

```clj
(def venue (blue/select-one :venues 1))
;; ->
(blue/instance :venues {:id         1
                        :name       "Tempest"
                        :category   "bar"
                        :created-at (t/local-date-time "2017-01-01T00:00")
                        :updated-at (t/local-date-time "2017-01-01T00:00")})


(def venue-2 (assoc venue :name "Hi-Dive"))

(blue/save! venue-2)

(blue/select-one :venues 1)
;; ->
(blue/instance :venues {:id         1
                        :name       "Hi-Dive"
                        :category   "bar"
                        :created-at (t/local-date-time "2017-01-01T00:00")
                        :updated-at (t/local-date-time "2017-01-01T00:00")})
```

## License

Code, documentation, and artwork copyright © 2021 [Cam Saul](https://camsaul.com).

Distributed under the [Eclipse Public License](https://raw.githubusercontent.com/camsaul/bluejdbc/master/LICENSE),
same as Clojure.
