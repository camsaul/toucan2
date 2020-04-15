(ns bluejdbc.driver
  (:require [clojure.string :as str]
            [potemkin.types :as p.types]))

(p.types/defprotocol+ CoerceToDriver
  (driver ^java.sql.Driver [this]
    "Coerce `this` to a `java.sql.Driver`."))

(extend-protocol CoerceToDriver
  java.sql.Driver
  (driver [this]
    this)

  Class
  (driver [this]
    (driver (.newInstance this)))

  String
  (driver [this]
    (if (str/starts-with? this "jdbc:")
      (java.sql.DriverManager/getDriver this)
      (driver (Class/forName this)))))
