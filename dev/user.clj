;; datastore test environment setup
;; see https://cloud.google.com/appengine/docs/java/tools/localunittesting

;; (println "hello, user")

(import '(com.google.appengine.tools.development.testing
          LocalServiceTestHelper
          LocalServiceTestConfig
          LocalMemcacheServiceTestConfig
          LocalMemcacheServiceTestConfig$SizeUnit
          LocalMailServiceTestConfig
          LocalDatastoreServiceTestConfig
          LocalUserServiceTestConfig))

(require '(migae [datastore :as ds]))

(def helper (LocalServiceTestHelper.
              (into-array LocalServiceTestConfig
                          [(LocalDatastoreServiceTestConfig.)])))

(defn ds-reset []
  (.tearDown helper) (.setUp helper))

(.setUp helper)
;; (.tearDown helper))))

;; some abbrevs, to save typing in the repl.  season to taste.
(def k [:A/B])
(def m {:a 1})
(def em (ds/entity-map k m))
