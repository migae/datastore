;; datastore test environment setup
;; see https://cloud.google.com/appengine/docs/java/tools/localunittesting

;; (println "hello, user")

(import '(com.google.appengine.tools.development.testing
          LocalServiceTestHelper
          LocalServiceTestConfig
          LocalDatastoreServiceTestConfig))

(require '(migae [datastore :as ds]))
(require '(migae.datastore [keychain :as k]))

;; LocalServiceTestHelper
;; static void	endRequest()
;;     Indicate the request has ended so that local services can do any post request work.
;; static ApiProxyLocal	getApiProxyLocal()
;;     Convenience function for getting ahold of the currently registered ApiProxyLocal.
;; static LocalRpcService	getLocalService(java.lang.String serviceName)
;;     Convenience function for getting ahold of a specific local service.
;; protected com.google.apphosting.api.ApiProxy.Environment	newEnvironment()
;;     Constructs the ApiProxy.Environment that will be installed.
;; protected LocalServerEnvironment	newLocalServerEnvironment()
;;     Constructs the LocalServerEnvironment that will be installed.
;; LocalServiceTestHelper	setClock(Clock clock)
;;     Sets the clock with which all local services will be initialized.
;; LocalServiceTestHelper	setEnforceApiDeadlines(boolean val)
;;     Determines whether or not API calls should be subject to the same deadlines as in production.
;; LocalServiceTestHelper	setEnvAppId(java.lang.String envAppId)
;;     The value to be returned by ApiProxy.getCurrentEnvironment().getAppId()
;; LocalServiceTestHelper	setEnvAttributes(java.util.Map<java.lang.String,java.lang.Object> envAttributes)
;; The value to be returned by ApiProxy.getCurrentEnvironment().getAttributes()
;; LocalServiceTestHelper	setEnvAuthDomain(java.lang.String envAuthDomain)
;; The value to be returned by ApiProxy.getCurrentEnvironment().getAuthDomain()
;; LocalServiceTestHelper	setEnvEmail(java.lang.String envEmail)
;; The value to be returned by ApiProxy.getCurrentEnvironment().getEmail()
;; LocalServiceTestHelper	setEnvInstance(java.lang.String envInstance)
;; The current instance id held by ApiProxy.getCurrentEnvironment()
;; LocalServiceTestHelper	setEnvIsAdmin(boolean envIsAdmin)
;; The value to be returned by ApiProxy.getCurrentEnvironment().isAdmin()
;; LocalServiceTestHelper	setEnvIsLoggedIn(boolean envIsLoggedIn)
;; The value to be returned by ApiProxy.getCurrentEnvironment().isLoggedIn()
;; LocalServiceTestHelper	setEnvModuleId(java.lang.String envModuleId)
;; The value to be returned by ApiProxy.getCurrentEnvironment().getModuleId()
;; LocalServiceTestHelper	setEnvRequestNamespace(java.lang.String envRequestNamespace)
;; The value to be returned by ApiProxy.getCurrentEnvironment().getRequestNamespace()
;; LocalServiceTestHelper	setEnvVersionId(java.lang.String envVersionId)
;; The value to be returned by ApiProxy.getCurrentEnvironment().getVersionId()
;; cLocalServiceTestHelper	setRemainingMillisTimer(LocalServiceTestHelper.RequestMillisTimer timer)
;; Sets the object that will return the value to be returned by ApiProxy.getCurrentEnvironment().getRemainingMillis()
;; LocalServiceTestHelper	setSimulateProdLatencies(boolean val)
;; Determines whether or not local services should simulate production latencies.
;; LocalServiceTestHelper	setTimeZone(java.util.TimeZone timeZone)
;; Sets the time zone in which tests will execute.
;; LocalServiceTestHelper	setUp()
;; Set up an environment in which tests that use local services can execute.
;; void	tearDown()
;; Tear down the environment in which tests that use local services can execute.

(def ds-test-env (.setEnforceApiDeadlines
                  (LocalServiceTestHelper.
                   (into-array LocalServiceTestConfig
                               [(LocalDatastoreServiceTestConfig.)]))
                  false))

(defn ds-reset []
  ;;(.tearDown ds-test-env)
  (.setUp ds-test-env))

;; some abbrevs, to save typing in the repl.  season to taste.
(def k [:A/B])
(def m {:a 1})

(.setUp ds-test-env)
(def em (ds/entity-map k m))

;; (.tearDown ds-test-env))))

