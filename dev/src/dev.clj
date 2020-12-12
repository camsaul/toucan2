(ns dev
  (:require [bluejdbc.core :as jdbc]
            [bluejdbc.test :as test]
            [environ.core :as env]))

(jdbc/defmethod jdbc/named-connectable :h2
  [_]
  "jdbc:h2:mem:bluejdbc_test;DB_CLOSE_DELAY=-1")

(jdbc/defmethod jdbc/named-connectable :postgres
  [_]
  "jdbc:postgresql://localhost:5432/bluejdbc?user=cam&password=cam")

(jdbc/defmethod jdbc/named-connectable :mysql
  [_]
  "jdbc:mysql://localhost:3306/metabase_test?user=root")

(defn use! [what]
  (test/set-jdbc-url! (jdbc/named-connectable what)))

(defn ns-unmap-all
  "Unmap all interned vars in a namespace. Reset the namespace to a blank slate! Perfect for when you rename everything
  and want to make sure you didn't miss a reference or when you redefine a multimethod.

    (ns-unmap-all *ns*)"
  ([]
   (ns-unmap-all *ns*))

  ([a-namespace]
   (doseq [[symb] (ns-interns a-namespace)]
     (ns-unmap a-namespace symb))))

(defn set-jdbc-url!
  "Set the JDBC URL used for testing."
  [url]
  (alter-var-root #'env/env assoc :jdbc-url url)
  nil)
