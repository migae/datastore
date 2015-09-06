(ns test.mutation
  (:refer-clojure :exclude [name hash])
  (:import [com.google.appengine.tools.development.testing
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

(deftest ^:emap get-ds-fail-1
  (testing "get-ds fail 1"
    (try (ds/get-ds [:A/B])
         (catch com.google.appengine.api.datastore.EntityNotFoundException e
           (is (= "No entity was found matching the key: A(\"B\")"
                  (.getMessage e)))))))

;; FIXME:  keep into-ds! and into-ds internal?  only expose entity-map ctors?

;; (deftest ^:emap into-ds-1
;;   (testing "into-ds 1: non-destructive save"
;;     (let [em1 (ds/into-ds [:A/B] {:a 1})]
;;       (try (ds/into-ds [:A/B] {:b 2})
;;            (catch java.lang.RuntimeException e
;;              (is (= "key already exists: [:A/B]"
;;                     (.getMessage e))))))))

;; (deftest ^:emap into-ds!
;;   (testing "into-ds!: destructive save"
;;     (let [em1 (ds/into-ds! [:A/B] {:a 1 :b 2})
;;           em1a (ds/get-ds [:A/B])]
;;         (log/trace "em1:" (ds/print em1))
;;         (log/trace "em1a:" (ds/print em1a))
;;       (is (= (:a em1) 1))
;;       (let [em2 (ds/into-ds! [:A/B] {:c 3})
;;             em2b (ds/entity-map* [:A/B]) ;; co-constructor
;;             em2a (ds/get-ds [:A/B])]     ;; getter
;;         (log/trace "em2:" (ds/print em2))
;;         (log/trace "em2a:" (ds/print em2a))
;;         (is (= (:a em1) 1))             ; previously fetched emap unaffected
;;         (is (= (:a em1a) 1))
;;         (is (= (:b em2) nil))
;;         (is (= (:a em2a) nil))
;;         (is (= (:b em2a) nil))
;;         (is (= (:c em2a) 3))
;;       ))))
