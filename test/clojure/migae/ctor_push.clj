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

(deftest ^:ctor-push ctor-push-fail
  (testing "ctor-push fail"
    (let [em1 (ds/entity-map! [:A/B] {:a 1})
          em2 (try (ds/entity-map! [:A/B] {:a 2})
                   (catch java.lang.RuntimeException ex ex))]
      (is (= (type em2) java.lang.RuntimeException))
      (is (= "Key already used" (.getMessage em2)))
      )))

(deftest ^:ctor-push ctor-push-proper
  (testing "ctor-push proper keychains"
    (let [em1 (ds/entity-map! [:A/A] {:x 9})
          em2 (ds/entity-map! [:A/B] {:a 1})
          em3 (ds/entity-map! [:X/Y :A/A] {})
          em4 (ds/entity-map! [:X/Y :B] {:a 1})
          ]
      (log/trace "em1:" (ds/epr em1))
      (log/trace "em2:" (ds/epr em2))
      (log/trace "em3:" (ds/epr em3))
      (log/trace "em4:" (ds/epr em4))
      )))

(deftest ^:ctor-push ctor-push-improper
  (testing "ctor-push improper keychains"
   (let [em1 (ds/entity-map! [:A/B :C] {:a 1})
         em2 (ds/entity-map! [:A/B :C] {})
          ]
      (log/trace "em1:" (ds/epr em1))
      (log/trace "em2:" (ds/epr em2))
      )))

(deftest ^:ctor-push ctor-push-force
  (testing "co-constructor"
    (let [em1  (ds/entity-map! [:A/B] {:a 1})
          em1a (ds/entity-map* [:A/B] {})
          em2  (ds/entity-map! :force [:A/B] {:a 2})
          em2a (ds/entity-map* [:A/B] {})
          ]
      (log/trace "em1:" (ds/epr em1))
      (log/trace "em1a:" (ds/epr em1a))
      (log/trace "em2:" (ds/epr em2))
      (log/trace "em2a:" (ds/epr em2a))
      )))

(deftest ^:ctor-push ctor-kinded-1
  (testing "entity-map kind ctor 1: keylink"
      (let [m {:a 1}
            em1 (ds/entity-map! [:Foo] m)
            em2 (ds/entity-map! [:Foo] m)]
        (log/trace "em1" (ds/epr em1))
        (log/trace "em2" (ds/epr em2))
        (is (ds/entity-map? em1))
        )))

(deftest ^:ctor-push ctor-kinded-2
  (testing "entity-map kind ctor 2: keychain"
      (let [m {:a 1}
            em1 (ds/entity-map! [:A/B :Foo] m)
            em2 (ds/entity-map! [:A/B :Foo] m)]
        (log/trace "em1" (ds/epr em1))
        (log/trace "em1 kind:" (ds/kind em1))
        (log/trace "em1 identifier:" (ds/identifier em1))
        (is (= (ds/kind em1) :Foo))
        (is (= (ds/kind em1) (ds/kind em2)))
        (is (= (val em1) (val em2)))
        (is (not= (ds/identifier em1) (ds/identifier em2)))
        )))




