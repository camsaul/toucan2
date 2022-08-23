(ns toucan2.protocols
  (:require [potemkin :as p]))

(p/defprotocol+ IModel
  :extend-via-metadata true
  "Protocol for something that is-a or has-a model."
  (model [this]
    "Get the Toucan model associated with `this`."))

(extend-protocol IModel
  nil
  (model [_this]
    nil)

  Object
  (model [_this]
    nil))

(p/defprotocol+ IWithModel
  :extend-via-metadata true
  "Protocol for something that has-a model that supports creating a copy with a different model."
  (with-model [this new-model]
    "Return a copy of `instance` with its model set to `new-model.`"))

;;; there are some default impls of [[with-model]] in [[toucan2.instance]]

(p/defprotocol+ IRecordChanges
  "Protocol for something that records the changes made to it, e.g. a Toucan instance."
  (original [instance]
            "Get the original version of `instance` as it appeared when it first came out of the DB.")

  (with-original [instance new-original]
    "Return a copy of `instance` with its `original` map set to `new-original`.")

  (current [instance]
           "Return the underlying map representing the current state of an `instance`.")

  (with-current [instance new-current]
    "Return a copy of `instance` with its underlying `current` map set to `new-current`.")

  (changes [instance]
           "Get a map with any changes made to `instance` since it came out of the DB. Only includes keys that have been
    added or given different values; keys that were removed are not counted. Returns `nil` if there are no changes."))

;;; `nil` and `IPersistentMap` can implement so of the methods that make sense for them -- `nil` or a plain map doesn't
;;; have any changes, so [[changes]] can return `nil`. I don't know what sort of implementation for stuff like
;;; [[with-original]] or [[with-current]] makes sense so I'm not implementing those for now.
(extend-protocol IRecordChanges
  nil
  (original [_this]
    nil)
  ;; (with-original [this])
  (current [_this]
    nil)
  ;; (with-current [this])
  (changes [_this]
    nil)

  ;; generally just treat a plain map like an instance with nil model/and original = nil,
  ;; and no-op for anything that would require "upgrading" the map to an actual instance in such a way that if
  ;;
  ;;    (= plain-map instance)
  ;;
  ;; then
  ;;
  ;;    (= (f plain-map) (f instance))
  clojure.lang.IPersistentMap
  (original [_this]
    nil)
  (with-original [this _m]
    this)
  (current [this]
    this)
  (with-current [_this new-current]
    new-current)
  (changes [_this]
    nil))

(p/defprotocol+ DispatchValue
  :extend-via-metadata true
  "Protocol to get the value to use for multimethod dispatch in Toucan from something."
  (dispatch-value
   [this]
   "Get the value that we should dispatch off of in multimethods for `this`. By default, the dispatch of a keyword is
    itself while the dispatch value of everything else is its [[type]]."))

(extend-protocol DispatchValue
  Object
  (dispatch-value [x]
    (type x))

  nil
  (dispatch-value [_nil]
    nil)

  clojure.lang.Keyword
  (dispatch-value [k]
    k))

;;; c3p0 integration: when we encounter a c3p0 connection dispatch off of the class of connection it wraps
(when-let [c3p0-connection-class (try
                                   (Class/forName "com.mchange.v2.c3p0.impl.NewProxyConnection")
                                   (catch Throwable _
                                     nil))]
  (extend c3p0-connection-class
    DispatchValue
    {:dispatch-value (fn [^java.sql.Wrapper conn]
                       (try
                         (dispatch-value (.unwrap conn java.sql.Connection))
                         (catch Throwable _
                           c3p0-connection-class)))}))
