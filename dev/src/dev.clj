(ns dev
  (:require [bluejdbc.util :as u]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [clojure.tools.logging.impl :as log.impl]))

(defn- logger [ns]
  (reify log.impl/Logger
    (enabled? [_ level]
      true)
    (write! [_ level e message]
      (let [e (when e
                (u/pprint-to-str (Throwable->map e)))
            s (format "%s [%s] %s%s"
                      (ns-name ns)
                      (str/upper-case (name level))
                      message
                      (if e
                        (str "\n" e)
                        ""))]
        (locking println
          (println s))))))

(def ^:private logger-factory
  (reify log.impl/LoggerFactory
    (name [_] "My Logger Factory")
    (get-logger [_ ns]
      (logger ns))))

(alter-var-root #'log/*logger-factory* (constantly logger-factory))

(defn ns-unmap-all
  "Unmap all interned vars in a namespace. Reset the namespace to a blank slate! Perfect for when you rename everything
  and want to make sure you didn't miss a reference or when you redefine a multimethod.

    (ns-unmap-all *ns*)"
  ([]
   (ns-unmap-all *ns*))

  ([a-namespace]
   (doseq [[symb] (ns-interns a-namespace)]
     (ns-unmap a-namespace symb))))
