(defproject org.mobileink/migae.datastore "0.3.1-SNAPSHOT"
  :description "migae - MobileInk Google App Engine sdk for Clojure."
  :url "https://github.com/migae/datastore"
  :min-lein-version "2.0.0"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :aot [IPersistentEntityMap]
  :source-paths ["src/clojure"]
  :java-source-paths ["src/java"]
  :test-paths ["test/clojure"]
  :test-selectors {:ancestor :ancestor
                   :api :api
                   :assoc :assoc
                   :coll :coll
                   :ds :ds
                   :emap :emap
                   :elist :elist
                   :entity :entity
                   :entities :entities
                   :fetch :fetch
                   :fields :fields
                   :infix :infix
                   :into :into
                   :keys :keys
                   :keylink :keylink
                   :keychain :keychain
                   :map :map
                   :merge :merge
                   :meta :meta
                   :one :one
                   :pred :pred
                   :props :props
                   :query :query
                   }
  :dependencies [[org.clojure/clojure "1.7.0"]  ;;  "1.8.0-master-SNAPSHOT"]
                 [org.clojure/tools.reader "0.8.16"]
                 [org.clojure/tools.logging "0.3.1"]
                 [com.google.appengine/appengine-api-1.0-sdk "1.9.25"]
                 [log4j "1.2.17" :exclusions [javax.mail/mail
                                              javax.jms/jms
                                              com.sun.jdmk/jmxtools
                                              com.sun.jmx/jmxri]]]

                 ;; [org.slf4j/slf4j-log4j12 "1.6.6"]
;;  :repositories {"local" "file:maven_repository"}
  :repl-options {:welcome (println "migae datastore repl.  (ds-reset) to reinitialize test datastore.")}
  :profiles {:test {:dependencies [[com.google.appengine/appengine-api-stubs "1.9.25"]
                                   [com.google.appengine/appengine-testing "1.9.25"]]}
             :dev {:source-paths ["dev" "src/clojure"]
                   :dependencies [[com.google.appengine/appengine-api-stubs "1.9.25"]
                                   [com.google.appengine/appengine-testing "1.9.25"]]}})
  ;;                                  [ring-zombie "1.0.1"]]}})

