(ns build
  (:require [clojure.java.shell :as sh]
            [clojure.string :as str]
            [clojure.tools.build.api :as b]
            [deps-deploy.deps-deploy :as dd]))

(def scm-url "git@github.com:camsaul/toucan2.git")
(def github-url "https://github.com/camsaul/toucan2")
(def lib 'io.github.camsaul/toucan2-toucan1)

(def major-minor-version (str/trim (slurp "../VERSION.txt")))

(defn commit-number []
  (or (-> (sh/sh "git" "rev-list" "HEAD" "--count")
          :out
          str/trim
          parse-long)
      "9999-SNAPSHOT"))

(def version (str major-minor-version \. (commit-number)))

(def target    "target")
(def class-dir "target/classes")
(def jar-file  (format "target/%s-%s.jar" lib version))

(def sha
  (or (not-empty (System/getenv "GITHUB_SHA"))
      (not-empty (-> (sh/sh "git" "rev-parse" "HEAD")
                     :out
                     str/trim))))

(def pom-template
  [[:description "Toucan 1 reimplemented in terms of Toucan 2. Compatibility layer for existing projects using Toucan 1."]
   [:url github-url]
   [:licenses
    [:license
     [:name "Eclipse Public License"]
     [:url "http://www.eclipse.org/legal/epl-v10.html"]]]
   [:developers
    [:developer
     [:name "Cam Saul"]]]
   [:scm
    [:url github-url]
    [:connection (str "scm:git:" scm-url)]
    [:developerConnection (str "scm:git:" scm-url)]
    [:tag sha]]])

(def options
  {:lib       lib
   :version   version
   :jar-file  jar-file
   :basis     (b/create-basis {})
   :class-dir class-dir
   :target    target
   :src-dirs  ["src"]
   :pom-data  pom-template})

(println "Options:" (pr-str (dissoc options :basis)))

(defn build [& _]
  (b/delete {:path target})
  (println "\nWriting pom.xml...")
  (b/write-pom options)
  (println "\nCopying source...")
  (b/copy-dir {:src-dirs   ["src" "resources"]
               :target-dir class-dir})
  (printf "\nBuilding %s...\n" jar-file)
  (b/jar options)
  (println "Done."))

(defn deploy [& _]
  (dd/deploy {:installer :remote
              :artifact  (b/resolve-path jar-file)
              :pom-file  (b/pom-path (select-keys options [:lib :class-dir]))}))
