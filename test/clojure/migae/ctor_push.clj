(ns migae.ctor-push
  (:refer-clojure :exclude [name hash])
  (:import [com.google.appengine.tools.development.testing
            LocalServiceTestHelper
            LocalServiceTestConfig
            LocalMemcacheServiceTestConfig
            LocalMemcacheServiceTestConfig$SizeUnit
            LocalMailServiceTestConfig
            LocalDatastoreServiceTestConfig
            LocalUserServiceTestConfig]
           [com.google.apphosting.api ApiProxy]
           [com.google.appengine.api.datastore EntityNotFoundException])
  ;; (:use [clj-logging-config.log4j])
  (:require [clojure.test :refer :all]
            ;;            [migae.infix :as infix]
            [migae.datastore :as ds]
            [clojure.tools.logging :as log :only [trace debug info]]))
                                        ;            [ring-zombie.core :as zombie]))

(defmacro should-fail [body]
  `(let [report-type# (atom nil)]
     (binding [clojure.test/report #(reset! report-type# (:type %))]
       ~body)
     (testing "should fail"
       (is (= @report-type# :fail )))))

;; (defn- make-local-services-fixture-fn [services hook-helper]
(defn- ds-fixture
  [test-fn]
  (let [;; environment (ApiProxy/getCurrentEnvironment)
        ;; delegate (ApiProxy/getDelegate)
        helper (LocalServiceTestHelper.
                (into-array LocalServiceTestConfig
                            [(LocalDatastoreServiceTestConfig.)]))]
    (do
      (.setUp helper)
      ;; (ds/init)
      (test-fn)
      (.tearDown helper))))
;; (ApiProxy/setEnvironmentForCurrentThread environment)
;; (ApiProxy/setDelegate delegate))))

                                        ;(use-fixtures :once (fn [test-fn] (dss/get-datastore-service) (test-fn)))
(use-fixtures :each ds-fixture)

(deftest ^:ctor-push ctor-push-notfound
  (testing "co-constructor notfound"
    (try (ds/entity-map* [:A/B])
         (catch EntityNotFoundException e
           (is (= "No entity was found matching the key: A(\"B\")"
                  (.getMessage e)))))))

(deftest ^:ctor-push ctor-push
  (testing "co-constructor"
    (let [em1 (ds/entity-map! [:A/B] {})
          em2 (ds/entity-map! [:A/B] {:a 1})
          em3 (ds/entity-map! [:A/B :C/D] {})
          em4 (ds/entity-map! [:A/B :C/D] {:a 1})
          em5 (ds/entity-map! [:A/B :C] {:a 1}) ; autogen id
          em6 (ds/entity-map! [:A/B :C] {})
          ]
      (log/trace "em1:" (ds/epr em1))
      (log/trace "em2:" (ds/epr em2))
      (log/trace "em3:" (ds/epr em3))
      (log/trace "em4:" (ds/epr em4))
      (log/trace "em5:" (ds/epr em5))
      (log/trace "em6:" (ds/epr em6))
      )))


