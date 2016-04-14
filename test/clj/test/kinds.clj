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
           [com.google.apphosting.api ApiProxy]
           [migae.datastore InvalidKeychainException])
  (:require [clojure.test :refer :all]
            [migae.datastore.model.entity-map :as ds]
            [clojure.tools.logging :as log :only [trace debug info]]))

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
      (log/trace "e1" (ds/dump e1))
      (log/trace "e2" (ds/dump e2))
      )))

(deftest ^:kinds kind-ctor-fail
  (testing "kinded construction: local ctor fails"
    (let [ex (try (ds/entity-map [:A] {})
                  (catch IllegalArgumentException e e))]
      (is (= (.getMessage ex) "Improper keychain '[:A]' not allowed for local ctor")))
    (let [ex (try (ds/entity-map [:A/B :C] {})
                  (catch IllegalArgumentException x x))]
      (is (= "Improper keychain '[:A/B :C]' not allowed for local ctor"
             (.getMessage ex))))
    (let [ex (try (ds/entity-map [:A/B :C :X/Y] {})
                  (catch InvalidKeychainException x x))]
      (is (= (.getMessage ex) "[:A/B :C :X/Y]")))
      ))

(deftest ^:kinds kind-ctor-chain
  (testing "kinded construction: chain"
    (let [e1 (ds/entity-map! [:A/B :C] {})
          e2 (ds/entity-map! [:A/B :C] {:a 1})
          e3 (ds/entity-map! [:A/B :C/D :X] {:a 1})
          e4 (ds/entity-map! [:A/B :C/D :X] {:a 1})]
      (log/trace "e1" (ds/dump e1))
      (log/trace "e2" (ds/dump e2))
      (log/trace "e3" (ds/dump e3))
      (log/trace "e4" (ds/dump e4))
      )))

;; TODO: support multi-level kinded construction, e.g. [:A :B :C]

(deftest ^:keychain improper
  (testing "keychain literals 20"
    (let [e1 (ds/entity-map! [:Foo] {})]
      ;; (log/trace "e1" (type e1) (.getMessage e1)))
    ;; (let [e1 (ds/entity-map [:Foo/Bar :Baz] {})]
    ;;   (log/trace "e1" (ds/dump e1)))
    ;; (let [e1 (ds/entity-map [:Foo/Bar :Baz/Buz :X] {:a 1 :b 2})]
    ;;   (log/trace "e1" (ds/dump e1)))
      )))
