(defproject org.mobileink/migae.datastore "0.3.3-SNAPSHOT"
  :description "migae - MobileInk Google App Engine sdk for Clojure."
  :url "https://github.com/migae/datastore"
  :min-lein-version "2.0.0"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :scm {:name "git" :url "https://github.com/migae/database"}
  :aot [Interfaces Exceptions]
  :source-paths ["src/clj" "src/clj-compile"]
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
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.clojure/tools.reader "0.8.16"]
                 [org.clojure/tools.logging "0.3.1"]
                 [prismatic/schema "1.0.1"]
                 [com.google.appengine/appengine-api-1.0-sdk LATEST :scope "runtime"]
                 #_[com.google.appengine.tools/appengine-mapreduce LATEST ;; "0.8.5"
                  :exclusions [com.google.api-client/google-api-client
                               it.unimi.dsi/fastutil
                               com.google.appengine/appengine-api-1.0-sdk
                               com.google.guava/guava
                               com.google.http-client/google-http-client-jackson2
                               com.google.appengine.tools/appengine-pipeline
                               com.google.appengine.tools/appengine-gcs-client
                               com.google.api-client/google-api-client-appengine
                               com.fasterxml.jackson.core/jackson-databind
                               com.fasterxml.jackson.core/jackson-core]]
                 [log4j "1.2.17" :exclusions [javax.mail/mail
                                              javax.jms/jms
                                              com.sun.jdmk/jmxtools
                                              com.sun.jmx/jmxri]]]

  ;; :filespecs [;;{:type :path :path "config/base.clj"}
  ;;             ;; Directory paths are included recursively.
  ;;             ;;{:type :paths :paths ["config/web" "config/cli"]}
  ;;             ;; Programmatically-generated content can use :bytes.
  ;;             {:type :bytes :path "project.clj"
  ;;  :prep-tasks [["with-profile" "cc" "compile"]] ;; "javac" "compile"]
  :repl-options {:welcome (clojure.core/println "migae datastore repl.  (ds-reset) to reinitialize test datastore.")
                 :port 8081}
  :profiles {:test {:dependencies [[com.google.appengine/appengine-api-stubs "1.9.25"]
                                   [com.google.appengine/appengine-testing "1.9.25"]]}
             :dev {:source-paths ["src/clojure" "dev"]
                   :dependencies [[org.clojure/tools.namespace "0.2.3"]
                                  [org.clojure/java.classpath "0.2.0"]
                                  [com.google.appengine/appengine-api-stubs "1.9.25"]
                                  [com.google.appengine/appengine-testing "1.9.25"]]}
             :repl {:source-paths ["src/clojure" "dev"]
                    :dependencies [[org.clojure/tools.namespace "0.2.3"]
                                   [org.clojure/java.classpath "0.2.0"]
                                   [com.google.appengine/appengine-api-stubs "1.9.25"]
                                   [com.google.appengine/appengine-testing "1.9.25"]]}
             :compile {:source-paths ["src/clojure"]
                       :dependencies [[org.clojure/tools.namespace "0.2.3"]
                                      [org.clojure/java.classpath "0.2.0"]
                                      [com.google.appengine/appengine-api-stubs "1.9.25"]
                                      [com.google.appengine/appengine-testing "1.9.25"]]}
             })

