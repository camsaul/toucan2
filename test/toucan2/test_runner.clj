(ns toucan2.test-runner
  "Simple wrapper to let us use eftest with the Clojure CLI. Pass `:only` to specify where to look for tests (see dox
  for [[find-tests]] for more info.)"
  (:require
   [clojure.java.classpath :as classpath]
   [clojure.java.io :as io]
   [clojure.pprint :as pprint]
   [clojure.string :as str]
   [clojure.tools.namespace.find :as ns.find]
   [eftest.report.pretty]
   [eftest.report.progress]
   [eftest.runner]
   [pjstadig.humane-test-output :as humane-test-output]
   [toucan2.test :as test]))

(set! *warn-on-reflection* true)

;; initialize Humane Test Output if it's not already initialized.
(humane-test-output/activate!)

(defmulti ^:private find-tests
  "Find test vars in `arg`, which can be a string directory name, symbol naming a specific namespace or test, or a
  collection of one or more of the above."
  {:arglists '([arg])}
  type)

;; collection of one of the things below
(defmethod find-tests clojure.lang.Sequential
  [coll]
  (mapcat find-tests coll))

;; directory name
(defmethod find-tests String
  [dir-name]
  (find-tests (io/file dir-name)))

(defmethod find-tests java.io.File
  [^java.io.File file]
  (when (.isDirectory file)
    (println "Looking for test namespaces in directory" (str file))
    (->> (ns.find/find-namespaces-in-dir file)
         (filter (fn [ns-symb]
                   (str/ends-with? ns-symb "-test")))
         (mapcat find-tests))))

;; a test namespace or individual test
(defmethod find-tests clojure.lang.Symbol
  [symb]
  (if-let [ns-symb (some-> (namespace symb) symbol)]
    ;; a actual test var e.g. `toucan2.whatever-test/my-test`
    (do
      (require ns-symb)
      [(resolve symb)])
    ;; a namespace e.g. `toucan2.whatever-test`
    (do
      (require symb)
      (eftest.runner/find-tests symb))))

;; default -- look in all dirs on the classpath
(defmethod find-tests nil
  [_]
  (find-tests (classpath/system-classpath)))

(defn tests [{:keys [only]}]
  (when only
    (println "Running tests in" (pr-str only)))
  (find-tests only))

;;;; Running tests & reporting the output

(alter-var-root #'eftest.runner/synchronized?
                (constantly (complement test/parallel?)))

(def ^:private ci? (some-> (System/getenv "CI") str/lower-case parse-boolean))

(defn- reporter
  "Create a new test reporter/event handler, a function with the signature `(handle-event event)` that gets called once
  for every [[clojure.test]] event, including stuff like `:begin-test-run`, `:end-test-var`, and `:fail`."
  []
  (let [stdout-reporter (if ci?
                          eftest.report.pretty/report
                          eftest.report.progress/report)]
    (fn handle-event [event]
      #_(test-runner.junit/handle-event! event) ; TODO
      (stdout-reporter event))))

(defn run
  "Run `test-vars` with `options`, which are passed directly to [[eftest.runner/run-tests]].

    ;; run tests in a single namespace
    (run (find-tests 'toucan2.bad-test))

    ;; run tests in a directory
    (run (find-tests \"test/toucan2/my_test\"))"
  ([test-vars]
   (run test-vars nil))

  ([test-vars options]
   ;; don't randomize test order for now please, thanks anyway
   (with-redefs [eftest.runner/deterministic-shuffle (fn [_ test-vars] test-vars)]
     (binding [test/*parallel-test-counter* (atom {})]
       (merge
        (eftest.runner/run-tests
         test-vars
         (merge
          {:capture-output? false
           :multithread?    :vars
           :report          (reporter)}
          options))
        @test/*parallel-test-counter*)))))

;;;; `clojure -X` entrypoint

(defn run-tests
  "`clojure -X` entrypoint for the test runner. `options` are passed directly to `eftest`; see
  https://github.com/weavejester/eftest for full list of options.

  To use our test runner from the REPL, use [[run]] instead."
  [options]
  (let [start-time-ms (System/currentTimeMillis)
        summary       (run (tests options) options)
        fail?         (pos? (+ (:error summary) (:fail summary)))]
    (println "Running tests with options" (pr-str options))
    (pprint/pprint summary)
    (printf "Ran %d tests in parallel, %d single-threaded.\n" (:parallel summary 0) (:single-threaded summary 0))
    (printf "Finding and running tests took %d ms.\n" (- (System/currentTimeMillis) start-time-ms))
    (println (if fail? "Tests failed." "All tests passed."))
    (System/exit (if fail? 1 0))))
