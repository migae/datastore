;; datastore test environment setup
;; see https://cloud.google.com/appengine/docs/java/tools/localunittesting

;; WARNING: run `lein with-profile compile compile` before starting repl

(clojure.core/println "loading user.clj")

(require '[clojure.test :refer [run-tests test-var]])
(require '[clojure.tools.namespace.repl :refer [refresh refresh-all set-refresh-dirs]])

;; (defn rt
;;   [t]
;;   (clojure.test/test-var t))

;;(require 'test.ctor-local :reload-all)
;;(run-tests 'test.ctor-local)

(import '(com.google.appengine.tools.development.testing
          LocalServiceTestHelper
          LocalServiceTestConfig
          LocalDatastoreServiceTestConfig))

(require '(migae [datastore :as ds]))

(def ds-test-env (.setEnforceApiDeadlines
                  (LocalServiceTestHelper.
                   (into-array LocalServiceTestConfig
                               [(LocalDatastoreServiceTestConfig.)]))
                  false))

(set-refresh-dirs "test/clojure" "src/clojure")

(defn ds-reset []
  ;;(.tearDown ds-test-env)
  (refresh-all)
  (.setUp ds-test-env))

;; some abbrevs, to save typing in the repl.  season to taste.
(def k [:A/B])
(def m {:a 1})

(.setUp ds-test-env)
(def em (ds/entity-map k m))

;; (.tearDown ds-test-env))))


;; LocalServiceTestHelper
;; static void	endRequest()
;; static ApiProxyLocal	getApiProxyLocal()
;; static LocalRpcService	getLocalService(java.lang.String serviceName)
;; protected com.google.apphosting.api.ApiProxy.Environment	newEnvironment()
;; protected LocalServerEnvironment	newLocalServerEnvironment()
;; LocalServiceTestHelper	setClock(Clock clock)
;; LocalServiceTestHelper	setEnforceApiDeadlines(boolean val)
;; LocalServiceTestHelper	setEnvAppId(java.lang.String envAppId)
;; LocalServiceTestHelper	setEnvAttributes(java.util.Map<java.lang.String,java.lang.Object> envAttributes)
;; LocalServiceTestHelper	setEnvAuthDomain(java.lang.String envAuthDomain)
;; LocalServiceTestHelper	setEnvEmail(java.lang.String envEmail)
;; LocalServiceTestHelper	setEnvInstance(java.lang.String envInstance)
;; LocalServiceTestHelper	setEnvIsAdmin(boolean envIsAdmin)
;; LocalServiceTestHelper	setEnvIsLoggedIn(boolean envIsLoggedIn)
;; LocalServiceTestHelper	setEnvModuleId(java.lang.String envModuleId)
;; LocalServiceTestHelper	setEnvRequestNamespace(java.lang.String envRequestNamespace)
;; LocalServiceTestHelper	setEnvVersionId(java.lang.String envVersionId)
;; cLocalServiceTestHelper	setRemainingMillisTimer(LocalServiceTestHelper.RequestMillisTimer timer)
;; LocalServiceTestHelper	setSimulateProdLatencies(boolean val)
;; LocalServiceTestHelper	setTimeZone(java.util.TimeZone timeZone)
;; LocalServiceTestHelper	setUp()
;; void	tearDown()
