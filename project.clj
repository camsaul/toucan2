(defproject bluejdbc "0.1.0-alpha-SNAPSHOT"
  :url "https://github.com/camsaul/bluejdbc"
  :min-lein-version "2.5.0"

  :license {:name "Eclipse Public License"
            :url  "https://raw.githubusercontent.com/camsaul/bluejdbc/master/LICENSE"}

  :aliases
  {"deps"                      ["with-profile" "+deps" "deps"]
   "deploy"                    ["with-profile" "+deploy" "deploy"]
   "test"                      ["with-profile" "+test" "test"]
   "repl"                      ["with-profile" "+repl" "repl"]
   "bikeshed"                  ["with-profile" "+bikeshed" "bikeshed" "--max-line-length" "180"]
   "check-namespace-decls"     ["with-profile" "+check-namespace-decls" "check-namespace-decls"]
   "cloverage"                 ["with-profile" "+cloverage" "cloverage"]
   "eastwood"                  ["with-profile" "+eastwood" "eastwood"]
   "check-reflection-warnings" ["with-profile" "+reflection-warnings" "check"]
   "docstring-checker"         ["with-profile" "+docstring-checker" "docstring-checker"]
   ;; `lein lint` will run all linters
   "lint"                      ["do" ["eastwood"] ["bikeshed"] ["check-namespace-decls"] ["docstring-checker"] ["cloverage"]]}

  :dependencies
  [[clojure.java-time "0.3.2"]
   [honeysql "0.9.10" :exclusions [org.clojure/clojurescript]]
   [metabase/second-date "1.0.0"]
   [methodical "0.10.0-alpha"]
   [org.clojure/tools.logging "1.0.0"]
   [potemkin "0.4.5"]
   [pretty "1.0.4"]]

  :profiles
  {:h2
   {:dependencies
    [[com.h2database/h2 "1.4.200"]]}

   :postgres
   {:dependencies
    [[org.postgresql/postgresql "42.2.12"]]}

   :mysql
   {:dependencies
    [[org.mariadb.jdbc/mariadb-java-client "2.6.0"]]}

   :jdbc-drivers
   [:h2 :postgres :mysql]

   :dev
   [:jdbc-drivers
    {:dependencies
     [[org.clojure/clojure "1.10.1"]
      [environ "1.1.0"]
      [pjstadig/humane-test-output "0.10.0"]]

     :repl-options
     {:init-ns bluejdbc.core}

     :source-paths ["dev/src"]

     :injections
     [(require 'pjstadig.humane-test-output)
      (pjstadig.humane-test-output/activate!)
      (try (require 'dev) (catch Throwable _))]

     :global-vars
     {*warn-on-reflection* true}}]

   ;; for local dev anything in ./local/src is added to the classpath. Entire dir is git-ignored. So you can store
   ;; local JDBC URLs for testing there
   :repl
   {:source-paths ["local/src"]}

   ;; this is mostly for the benefit of fetching/caching deps on CI -- a single profile with *all* deps
   :deps
   [:dev :jdbc-drivers :cloverage]

   :test
   {}

   :cloverage
   {:dependencies
    ;; Required by both Potemkin and Cloverage, but Potemkin uses an older version that breaks Cloverage's ablity to
    ;; understand certain forms. Explicitly specify newer version here.
    [[riddley "0.2.0"]]

    :plugins
    [[lein-cloverage "1.1.2"]]

    ;; don't count ./dev stuff for code coverage calcualations.
    :source-paths ^:replace ["src"]

    :cloverage
    {:fail-threshold 45}}

   :eastwood
   {:plugins
    [[jonase/eastwood "0.3.11" :exclusions [org.clojure/clojure]]]

    :eastwood
    {:config-files [".eastwood-config.clj"]

     :add-linters
     [:unused-private-vars
      :unused-locals]

     :exclude-linters
     [:deprecations]}}

   :docstring-checker
   {:plugins
    [[docstring-checker "1.1.0"]]

    :docstring-checker
    {:exclude [#"test"]}}

   :bikeshed
   {:plugins  [[lein-bikeshed "0.5.2"]]
    :bikeshed {:max-line-length 160}}

   :check-namespace-decls
   {:plugins               [[lein-check-namespace-decls "1.0.2"]]
    :source-paths          ^:replace ["src" "test"]
    :check-namespace-decls {:prefix-rewriting false}}

   ;; run `lein check-reflection-warnings` to check for reflection warnings
   :reflection-warnings
   {:global-vars {*warn-on-reflection* true}}

   :deploy
   {:dependencies [[org.clojure/clojure "1.10.1"]]}}

  :deploy-repositories
  [["clojars"
    {:url           "https://clojars.org/repo"
     :username      :env/clojars_username
     :password      :env/clojars_password
     :sign-releases false}]])
