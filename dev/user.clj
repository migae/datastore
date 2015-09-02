;; datastore test environment setup
;; see https://cloud.google.com/appengine/docs/java/tools/localunittesting

;; (println "hello, user")

(import '(com.google.appengine.tools.development.testing
          LocalServiceTestHelper
          LocalServiceTestConfig
          LocalDatastoreServiceTestConfig))

(require '(migae [datastore :as ds]))

(def ds-test-env (LocalServiceTestHelper.
              (into-array LocalServiceTestConfig
                          [(LocalDatastoreServiceTestConfig.)])))

(defn ds-reset []
  ;;(.tearDown ds-test-env)
  (.setUp ds-test-env))

;; some abbrevs, to save typing in the repl.  season to taste.
(def k [:A/B])
(def m {:a 1})

(.setUp ds-test-env)
(def em (ds/entity-map k m))

;; (.tearDown ds-test-env))))

