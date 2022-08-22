(ns toucan2.jdbc.row
  (:require [potemkin :as p]
            [pretty.core :as pretty]
            [toucan2.realize :as realize]
            #_[toucan2.result-row :as result-row]
            [toucan2.util :as u]))

(defn- realize-column-with-thunk [k thunk]
  (try
    (thunk)
    (catch Throwable e
      (throw (ex-info (format "Error realizing column %s: %s" (u/safe-pr-str k) (ex-message e))
                      {:k k, :thunk thunk}
                      e)))))

;; TODO -- instead of thunks, should these just be DELAYS????
(p/def-map-type Row [col-name->thunk mta]
  (get [_ k default-value]
    (if-let [thunk (get col-name->thunk k)]
      (realize-column-with-thunk k thunk)
      default-value))

  (assoc [_ k v]
    (Row. (assoc col-name->thunk k (constantly v)) mta))

  (dissoc [_ k]
    (Row. (dissoc col-name->thunk k) mta))

  (keys [_]
    (keys col-name->thunk))

  (meta [_]
    mta)

  (with-meta [_ new-meta]
    (Row. col-name->thunk new-meta))

  ;; result-row/ResultRow
  ;; (thunks [_]
  ;;   col-name->thunk)
  ;; (with-thunks [_ new-thunks]
  ;;   (Row. new-thunks mta))

  realize/Realize
  (realize [_this]
    (u/with-debug-result "Realize entire row"
      (-> (into {} (for [[col-name thunk] col-name->thunk]
                     [col-name (realize-column-with-thunk col-name thunk)]))
          (with-meta mta))))

  pretty/PrettyPrintable
  (pretty [_]
    (list (pretty/qualify-symbol-for-*ns* `row) col-name->thunk)))

(defn row [col-name->thunk]
  ;; wrap the thunks in delays so we don't end up invoking them multiple times, since they are potentially expensive.
  (Row. (into {} (for [[k thunk] col-name->thunk
                       :let      [dlay (delay (thunk))]]
                   [k (fn cached-thunk [] @dlay)]))
        nil))
