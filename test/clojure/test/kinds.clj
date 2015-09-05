(ns test.kinds
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
            [migae.datastore :as ds]
            [migae.datastore.keychain :as k]
            ;; [migae.datastore.service :as dss]
            ;; [migae.datastore.entity :as dse]
            ;; [migae.datastore.query  :as dsqry]
            ;; [migae.datastore.key    :as dskey]
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
        ;; (ds/get-datastore-service)
        (test-fn)
        (.tearDown helper))))
        ;; (ApiProxy/setEnvironmentForCurrentThread environment)
        ;; (ApiProxy/setDelegate delegate))))

;(use-fixtures :once (fn [test-fn] (ds/get-datastore-service) (test-fn)))
(use-fixtures :each ds-fixture)

;; ################################################################

(deftest ^:kinds kind-ctor
  (testing "kinded construction"
    (let [e1 (ds/entity-map! [:A] {})
          e2 (ds/entity-map! [:A] {:a 1})]
      (log/trace "e1" (ds/print e1))
      (log/trace "e2" (ds/print e2))
      )))

(deftest ^:kinds kind-ctor-fail
  (testing "kinded construction: local ctor fails"
    (let [ex (try (ds/entity-map [:A] {})
                  (catch java.lang.IllegalArgumentException e e))]
      (is (= (type ex)  java.lang.IllegalArgumentException)))
    (let [ex (try (ds/entity-map [:A/B :C] {})
                  (catch java.lang.IllegalArgumentException e e))]
      (is (= (type ex)  java.lang.IllegalArgumentException)))
    (let [ex (try (ds/entity-map [:A/B :C :X/Y] {})
                  (catch java.lang.IllegalArgumentException e e))]
      (is (= (type ex)  java.lang.IllegalArgumentException)))
      ))

(deftest ^:kinds kind-ctor-chain
  (testing "kinded construction: chain"
    (let [e1 (ds/entity-map! [:A/B :C] {})
          e2 (ds/entity-map! [:A/B :C] {:a 1})
          e3 (ds/entity-map! [:A/B :C/D :X] {:a 1})
          e4 (ds/entity-map! [:A/B :C/D :X] {:a 1})]
      (log/trace "e1" (ds/print e1))
      (log/trace "e2" (ds/print e2))
      (log/trace "e3" (ds/print e3))
      (log/trace "e4" (ds/print e4))
      )))

;; TODO: support multi-level kinded construction, e.g. [:A :B :C]

(deftest ^:keychain improper
  (testing "keychain literals 20"
    (let [e1 (ds/entity-map! [:Foo])]
      (log/trace "e1" (type e1) (.getMessage e1)))
    ;; (let [e1 (ds/entity-map [:Foo/Bar :Baz] {})]
    ;;   (log/trace "e1" (ds/print e1)))
    ;; (let [e1 (ds/entity-map [:Foo/Bar :Baz/Buz :X] {:a 1 :b 2})]
    ;;   (log/trace "e1" (ds/print e1)))
      ))
