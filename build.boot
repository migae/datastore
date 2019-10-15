(def +project+ 'migae/datastore)
(def +version+ "0.4.0-SNAPSHOT")

(set-env!
 ;; :gae {:app-id "boot-test"
 ;;       :module "bulkops"
 ;;       :version +version+}
 :asset-paths #{"resources"}
 :resource-paths #{"src/clj"}
 ;; :source-paths #{"src/clj-aot"}
 ;; :repositories {"clojars" "https://clojars.org/repo"
 ;;                "central" "http://repo1.maven.org/maven2/"
 ;;                "maven-central" "http://mvnrepository.com"}
 :dependencies   '[[org.clojure/clojure "1.10.0" :scope "runtime"]

                   [javax.servlet/javax.servlet-api "3.1.0" :scope "provided"]

                   ;; the java sdk is not used by app code, it's just there for the devserver
                   ;; and test service stubs, so it should have :scope provided
                   ;; even though it will not in fact be provided by the prod env
                   ;; this is for the GAE runtime (NB: scope provided):
                   [com.google.appengine/appengine-java-sdk RELEASE :scope "provided" :extension "zip"]
                   ;; [com.google.appengine/appengine-java-sdk LATEST :scope "provided" :extension "zip"]

                   ;; this is for the GAE services like datastore (NB: scope runtime):
                   ;; NOTE: the api sdk is runtime scoped, since we do not compile our clojure files
                   [com.google.appengine/appengine-api-1.0-sdk RELEASE :scope "runtime"]
                   ;; [com.google.appengine/appengine-api-1.0-sdk LATEST :scope "runtime"]

                   ;; test service stubs
                   [com.google.appengine/appengine-api-labs RELEASE :scope "test"]
                   [com.google.appengine/appengine-api-stubs RELEASE :scope "test"]
                   [com.google.appengine/appengine-tools-sdk RELEASE :scope "test"]

                   ;; setup utils
                   [com.google.appengine/appengine-testing RELEASE :scope "test"]


                   ;; [org.mobileink/migae.datastore "0.3.3-SNAPSHOT" :scope "runtime"]
                   ;; [org.mobileink/migae.mail "0.1.0-SNAPSHOT" :scope "runtime"]

                   ;; GCM
                   ;; [com.ganyo/gcm-server "1.0.2"]
                   ;; [com.googlecode.json-simple/json-simple "1.1.1"]

                   ;; web
                   ;; [hiccup/hiccup "1.0.5" :scope "runtime"]
                   ;; [compojure/compojure "1.5.0" :scope "runtime"]
                   ;; [ring/ring-core "1.4.0" :scope "runtime"]
                   ;; [ring/ring-devel "1.4.0" :scope "runtime"]
                   ;; [ring/ring-servlet "1.4.0" :scope "runtime"]
                   ;; [ring/ring-defaults "0.1.5" :scope "runtime"]
                   ;; [metosin/compojure-api "0.23.1" :scope "runtime"]

                   ;; data
                   ;; [cheshire/cheshire "5.5.0" :scope "runtime"]
                   ;; [fogus/ring-edn "0.3.0" :scope "runtime"]
                   [org.clojure/tools.reader "1.3.2" :scope "runtime"]
                   ;; [commons-io/commons-io "2.4" :scope "runtime"]
                   ;; [commons-fileupload/commons-fileupload "1.3.1" :scope "runtime"]
                   [prismatic/schema "1.0.1"]

                   [potemkin "0.4.5"]
                   [org.clojure/tools.logging "0.4.1"]
                   [org.apache.logging.log4j/log4j-api "2.11.1"]
                   [org.apache.logging.log4j/log4j-core "2.11.1"]
                   [org.apache.logging.log4j/log4j-slf4j-impl "2.11.1"]
                   ;; [org.apache.logging.log4j/log4j-api "2.5"]
                   ;; [log4j "1.2.17" :exclusions [javax.mail/mail
                   ;;                              javax.jms/jms
                   ;;                              com.sun.jdmk/jmxtools
                   ;;                              com.sun.jmx/jmxri]]

                   ;; [org.clojure/tools.namespace "0.2.11"]
                   [ns-tracker/ns-tracker "0.3.1" :scope "test"]
                   [adzerk/boot-test "1.2.0" :scope "test"]
                   [migae/boot-gae "0.2.1-SNAPSHOT" :scope "test"]])

(require '[migae.boot-gae :as gae]
         '[boot.task.built-in :as boot]
         '[adzerk.boot-test :refer :all])

(def modules {:modules [{:name "bulkops"
                         :default true
                         :port 8085
                         :war "target/bulkops"}]})

(task-options!
 notify {:audible true :theme "woodblock"}
 gae/run modules
 ;; test {:startup (fn []
 ;;                  (println "XXXXXXXXXXXXXXXX")
 ;;                  ;; (set-env! ;; :source-paths #(conj % "test/clj" "target")
 ;;                  ;;  :asset-paths #{"resources"}
 ;;                  ;;  :resource-paths #{"src/clj" "test/clj"})
 ;;                  #_(require 'test.rig)
 ;;                  #_(test.rig/setup))
 ;;       ;;:shutdown test.rig/teardown
 ;;       }
 ;; run-tests {:startup test.rig/setup
 ;;            :shutdown test.rig/teardown}
 pom  {:project     +project+
       :version     +version+
       :description "Example code, boot, miraj, GAE"
       :license     {"EPL" "http://www.eclipse.org/legal/epl-v10.html"}})

(deftask dev
  "make a dev build - including reloader"
  [k keep bool "keep intermediate .clj and .edn files"
   v verbose bool "verbose"]
  (comp (boot/aot :namespace #{'Exceptions 'Interfaces})
        (boot/target)))

(deftask testing
  "Profile setup for running tests. E.g. $ boot testing test -n test.emap"
  []
  ;; (dev)
  (set-env! ;; :source-paths #(conj % "test/clj" "target")
            :asset-paths #{"resources"}
            :resource-paths #{"src/clj" "test/clj"})
  identity)

(deftask watchme
  "watch for gae project"
  []
  (comp (boot/aot :all true)
        (boot/target)
        (boot/watch)
        (boot/speak)
        (boot/pom)
        (boot/jar)
        (boot/install)))

        ;; (gae/logging :verbose verbose)
        ;; (boot/show :fileset true)
        ;; (boot/sift :to-asset #{#"(.*\.clj$)"}
        ;;               :move {#"(.*\.clj$)" "WEB-INF/classes/$1"})
        ;; (clj)
        ;; (appstats)
        ;; (filters :keep keep)
        ;; (servlets :keep keep)
        ;; (reloader :keep keep)
        ;; (webxml)
        ;;(appengine)
        ;; ))
