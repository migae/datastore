(ns migae.ctor-pull
  "Unit tests for pull constructor entity-map*"
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
      ;; store some entities
      (ds/entity-map! [:A/B] {:a 1})
      (ds/entity-map! [:A/B :C/D] {:a 1})
      (ds/entity-map! [:A/B :C] {:a 1}) ; autogen id
      (test-fn)
      (.tearDown helper))))
;; (ApiProxy/setEnvironmentForCurrentThread environment)
;; (ApiProxy/setDelegate delegate))))

                                        ;(use-fixtures :once (fn [test-fn] (dss/get-datastore-service) (test-fn)))
(use-fixtures :each ds-fixture)

(deftest ^:ctor-pull ctor-pull-fail
  (testing "pull constructor: entity not found"
    (let [ex (try (ds/entity-map* [:Foo/BAR :NOT/FOUND])
                  (catch EntityNotFoundException e e))]
      (is (= (type ex) EntityNotFoundException)))))

(deftest ^:ctor-pull ctor-pull
  (testing "pull ctor"
    (let [em1 (ds/entity-map* [:A/B])        ; co-ctor
          em2 (ds/entity-map* [:A/B :C/D])
          ]
      (log/trace "em1:" (ds/epr em1))
      (log/trace "em2:" (ds/epr em2))
      (is (= (:a em1) 1))         ; previously fetched emap unaffected
      (is (= (:b em2) nil))
      )))

(deftest ^:ctor-pull ctor-pull-kinded
  (testing "pull ctor"
    (let [ems (ds/entity-map* [:C]) ; fetch emaps of kind :C
          ]
      (log/trace "ems:" ems)
      )))


