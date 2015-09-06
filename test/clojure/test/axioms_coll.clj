(ns test.axioms-coll
  "entity-map collection axioms"
  (:refer-clojure :exclude [name hash])
  (:import [com.google.appengine.api.datastore
            Email
            EntityNotFoundException
            Link]
           [com.google.appengine.tools.development.testing
            LocalServiceTestHelper
            LocalServiceTestConfig
            LocalMemcacheServiceTestConfig
            LocalMemcacheServiceTestConfig$SizeUnit
            LocalMailServiceTestConfig
            LocalDatastoreServiceTestConfig
            LocalUserServiceTestConfig]
           [com.google.apphosting.api ApiProxy])
  ;; (:use [clj-logging-config.log4j])
  (:require [clojure.test :refer :all]
            ;; [migae.infix :as infix]
            [migae.datastore :as ds]
            ;; [migae.datastore.api :as ds]
            ;; [migae.datastore.service :as dss]
            ;; [migae.datastore.entity :as dse]
            ;; [migae.datastore.query  :as dsqry]
            [clojure.tools.logging :as log :only [trace debug info]]))
;            [ring-zombie.core :as zombie]))

(defmacro should-fail [body]
  `(let [report-type# (atom nil)]
     (binding [clojure.test/report #(reset! report-type# (:type %))]
       ~body)
     (testing "should fail"
       (is (= @report-type# :fail )))))

;  (:require [migae.migae-datastore.PersistentEntityMap])
  ;; (:use clojure.test
  ;;       [migae.migae-datastore :as ds]))

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
;;        (datastore)
        (test-fn)
        (.tearDown helper))))
        ;; (ApiProxy/setEnvironmentForCurrentThread environment)
        ;; (ApiProxy/setDelegate delegate))))

;(use-fixtures :once (fn [test-fn] (ds/get-datastore-service) (test-fn)))
(use-fixtures :each ds-fixture)

(deftest ^:coll emap-axiom1
  (testing "emap axiom 1: an entity-map is a map"
    (let [e1 (ds/entity-map [:A/B] {:a 1 :b 2})]
      ;; (log/trace "e1 " (ds/print e1))
      (is (coll? e1))
      (is (map? e1))
      (is (ds/emap? e1))
      (is (= (keys e1) '(:a :b)))
      (is (= (vals e1) '(1 2)))
      )))

(deftest ^:coll emap-axiom2
  (testing "emap axiom 2: an entity-map is seqable"
    (let [e1 (ds/entity-map [:A/B] {:a 1 :b 2})]
      (is (not (seq? e1)))
      (is (seq? (seq e1)))
      )))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;   collections api axioms

(deftest ^:coll coll-axiom1
  (testing "coll axiom 1: an entity-map is countable"
    (let [e1 (ds/entity-map [:A/B] {:a 1 :b 2})]
      (is (= (count e1) 2))
      )))

(deftest ^:coll empty-axiom1
  (testing "empty axiom 1: empty works"
    (let [e1 (ds/entity-map [:A/B] {:a 1 :b 2})
          e2 (empty e1)]
      (is (ds/emap? e2))
      (is (empty? e2))
      (is (ds/key= e1 e2)) ; an entity-map must have a key
      )))

(deftest ^:coll empty-axiom2
  (testing "empty axiom 2: not-empty works on non-empty emaps"
    (let [e1 (ds/entity-map [:A/B] {:a 1 :b 2})
          e2 (not-empty e1)]
      (is (ds/emap? e2))
      (is (not (empty? e2)))
      (is (ds/key= e1 e2))
      (is (= e1 e2))
      )))

(deftest ^:coll empty-axiom3
  (testing "empty axiom 3: not-empty works on empty emaps"
    (let [e1 (ds/entity-map [:A/B] {})
          e2 (not-empty e1)]
      (is (nil? e2))
      (is (ds/emap? e2))
      )))

