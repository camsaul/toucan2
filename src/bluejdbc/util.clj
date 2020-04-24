(ns bluejdbc.util
  (:require [bluejdbc.util.macros.enum-map :as enum-map]
            [bluejdbc.util.macros.proxy-class :as proxy-class]
            [clojure.pprint :as pprint]
            [clojure.string :as str]
            [potemkin :as p]))

(comment enum-map/keep-me proxy-class/keep-me)

(p/import-vars [enum-map define-enums reverse-lookup]
               [proxy-class define-proxy-class])

(defn parse-currency
  "Parse a currency String to a BigDecimal. Handles a variety of different formats, such as:

    $1,000.00
    -£127.54
    -127,54 €
    kr-127,54
    € 127,54-
    ¥200"
  ^java.math.BigDecimal [^String s]
  (when-not (str/blank? s)
    (bigdec
     (reduce
      (partial apply str/replace)
      s
      [
       ;; strip out any current symbols
       [#"[^\d,.-]+"          ""]
       ;; now strip out any thousands separators
       [#"(?<=\d)[,.](\d{3})" "$1"]
       ;; now replace a comma decimal seperator with a period
       [#","                  "."]
       ;; move minus sign at end to front
       [#"(^[^-]+)-$"         "-$1"]]))))

(defn pprint-to-str
  "Pretty-print `x` to a string. Mostly used for log message purposes."
  ^String [x]
  (with-open [w (java.io.StringWriter.)]
    (binding [pprint/*print-right-margin* 120]
      (pprint/pprint x w))
    (str w)))
