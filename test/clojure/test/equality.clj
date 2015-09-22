(ns test.equality
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
           [com.google.appengine.api.datastore
            EntityNotFoundException])
  ;; (:use [clj-logging-config.log4j])
  (:require [clojure.test :refer :all]
            [migae.datastore.signature.entity-map :as ds]
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
        (test-fn)
        (.tearDown helper))))

(use-fixtures :each ds-fixture)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; DESIGN DECISION: should clojure.core/= mean full entity equality or
;; just map=?  To match clojure, just map=? I think - if the print
;; sytax is the same, then they are clojure.core/=.  The problem is
;; that such equal entity-maps may have different keys, which really
;; makes them unequal as entities.  So we have an unavoidable
;; infelicity.

;; One option is to outlaw clojure.core/= and require equality
;; predicates to be decorated to explicitly indicate which kind of
;; equality: ds/key=?, ds/map=?, and ds/entity-map=?

(deftest ^:equality eq-1
  (testing "Clojure.core/= is disallowed"
    (let [res1 (try (= (ds/entity-map [:A/B] {:a 1})
                       (ds/entity-map [:A/B] {:a 1}))
                    (catch java.lang.RuntimeException x x))
          res2 (try (not= (ds/entity-map [:A/B] {:a 1})
                          (ds/entity-map [:X/Y] {:z 2}))
                    (catch java.lang.RuntimeException x x))
          res3 (try (= (ds/entity-map [:A/B] {:a 1})
                       {:a 1})
                       (catch java.lang.RuntimeException x x))
          res4 (try (not= (ds/entity-map [:A/B] {:a 1})
                          {:a 1})
                    (catch java.lang.RuntimeException x x))
          res5 (try (= (ds/entity-map [:A/B] {:a 1})
                       (hash-map :a 1))
                       (catch java.lang.RuntimeException x x))
          res6 (try (= (ds/entity-map [:A/B] {:a 1})
                       (array-map :a 1))
                       (catch java.lang.RuntimeException x x))]
      ;; (is (= (class res1) java.lang.RuntimeException))
      ;; (is (= "clojure.core/= not supported; use one of key=?, map=? or entity-map?"
      ;;        (.getMessage res1)))
      ;; (is (= (class res2) java.lang.RuntimeException))
      ;; (is (= "clojure.core/= not supported; use one of key=?, map=? or entity-map?"
      ;;        (.getMessage res2)))
      ;; (is (= (class res3) java.lang.RuntimeException))
      ;; (is (= "clojure.core/= not supported; use one of key=?, map=? or entity-map?"
      ;;        (.getMessage res3)))
      ;; (is (= (class res4) java.lang.RuntimeException))
      ;; (is (= "clojure.core/= not supported; use one of key=?, map=? or entity-map?"
      ;;        (.getMessage res4)))
      ;; (is (= (class res5) java.lang.RuntimeException))
      ;; (is (= "clojure.core/= not supported; use one of key=?, map=? or entity-map?"
      ;;        (.getMessage res5)))
      ;; (is (= (class res6) java.lang.RuntimeException))
      ;; (is (= "clojure.core/= not supported; use one of key=?, map=? or entity-map?"
      ;;        (.getMessage res6)))
      )))

(deftest ^:equality eq-2
  (testing "Clojure.core/="
    (is (map? (ds/entity-map [:A/B] {:a 1})))
    (is (coll? (ds/entity-map [:A/B] {:a 1})))
    (is (= (hash-map :a 1) (ds/entity-map [:A/B] {:a 1})))
    (is (= (ds/entity-map [:A/B] {:a 1}) (hash-map :a 1)))
          ;;           (catch java.lang.RuntimeException x x))
          ;; res2 (try (= (array-map :a 1)
          ;;              (ds/entity-map [:A/B] {:a 1}))
          ;;           (catch java.lang.RuntimeException x x))
          ;; res3 (try (not= {:a 1}
          ;;                 (ds/entity-map [:A/B] {:a 1}))
          ;;           (catch java.lang.RuntimeException x x))
          ;; res4 (try (= {:a 1}
          ;;              (ds/entity-map [:A/B] {:a 1}))
          ;;           (catch java.lang.RuntimeException x x))]
      ;; (is (= (class res1) java.lang.RuntimeException))
      ;; ;; (is (= "clojure.core/= not supported; use one of key=?, map=? or entity-map?"
      ;; ;;        (.getMessage res9)))
      ;; (is (= (class res2) java.lang.RuntimeException))
      ;; (is (= "clojure.core/= not supported; use one of key=?, map=? or entity-map?"
      ;;        (.getMessage res2)))
      ;; (is (= (class res3) java.lang.RuntimeException))
      ;; (is (= "clojure.core/= not supported; use one of key=?, map=? or entity-map?"
      ;;        (.getMessage res3)))
      ;; (is (= (class res4) java.lang.RuntimeException))
      ;; (is (= "clojure.core/= not supported; use one of key=?, map=? or entity-map?"
      ;;        (.getMessage res4)))
      ))

(deftest ^:equality eq-3
  (testing "ds/entity-map=? means both ds/key=? and ds/map=?"
    (let [em1 (ds/entity-map [:A/B] {:a 1})
          em2 (ds/entity-map [:A/B] {:a 1})]
    (is (ds/entity-map=? em1 em2))
    (is (not (identical? em1 em2))))
    (is (ds/entity-map=? (ds/entity-map [:A/B] {:a 1})
                         (ds/entity-map [:A/B] {:a 1})))
    (is (not (identical? (ds/entity-map [:A/B] {:a 1})
                         (ds/entity-map [:A/B] {:a 1}))))
    (is (not (ds/entity-map=? (ds/entity-map [:A/B] {:a 1})
                   (ds/entity-map [:A/B] {:a 2}))))
    (is (not (ds/entity-map=? (ds/entity-map [:A/B] {:a 1})
                              (ds/entity-map [:A/X] {:a 1}))))
    (is (not (ds/entity-map=? (ds/entity-map [:A/B] {:a 1})
                              (ds/entity-map [:A/X] {:a 2}))))
    ))

(deftest ^:equality key-eq-1
  (testing "key equality 1"
    (is (ds/key=? (ds/entity-map [:A/B] {:a 1})
                 (ds/entity-map [:A/B] {:a 1})))
    (is (ds/key=? (ds/entity-map [:A/B] {:a 1})
                 (ds/entity-map [:A/B] {:a 2})))
    (is (not (ds/key=? (ds/entity-map [:A/B] {:a 1})
                      (ds/entity-map [:A/X] {:a 1}))))
    (is (not (ds/key=? (ds/entity-map [:A/B] {:a 1})
                      (ds/entity-map [:A/X] {:a 2}))))
    (log/trace "done")
    ))

(deftest ^:equality map-eq-1
  (testing "values map equality 1"
    (is (ds/map=? (ds/entity-map [:A/B] {:a 1})
               (ds/entity-map [:A/B] {:a 1})))
    (is (ds/map=? (ds/entity-map [:A/B] {:a 1})
               (ds/entity-map [:A/X] {:a 1})))
    (is (not (ds/map=? (ds/entity-map [:A/B] {:a 1})
                    (ds/entity-map [:A/B] {:a 2}))))
    (is (not (ds/map=? (ds/entity-map [:A/B] {:a 1})
                    (ds/entity-map [:A/X] {:a 2}))))
    ))

;;;;;;;;;;;;;;;;
;;; what about cross-type comparison?  e.g. (= (entity-map [:A/B] {:a 1}) {:a 1})
;; (deftest ^:equality eq-hybrid
;;   (testing "an emap and a clojure map are never = since clojure maps don't have a key"
;;     (is (not= (ds/entity-map [:A/B] {:a 1})
;;                   {:a 1}))
;;     (is (ds/map=? {:a 1}
;;                   (ds/entity-map [:A/B] {:a 1})))

;;     (is (ds/map=? (ds/entity-map [:A/B] {:a 1})
;;                   (hash-map :a 1)))
;;     (is (ds/map=? (hash-map :a 1)
;;                   (ds/entity-map [:A/B] {:a 1})))
;;     ))

;; (deftest ^:equality map-eq-hybrid
;;   (testing "an emap and a clojure map are map= if they have the same map"
;;     (is (ds/map=? (ds/entity-map [:A/B] {:a 1})
;;                   {:a 1}))
;;     (is (ds/map=? {:a 1}
;;                   (ds/entity-map [:A/B] {:a 1})))

;;     (is (ds/map=? (ds/entity-map [:A/B] {:a 1})
;;                   (hash-map :a 1)))
;;     (is (ds/map=? (hash-map :a 1)
;;                   (ds/entity-map [:A/B] {:a 1})))
;;     ))

;;; what about comparing an entity-map and a Map$Entry?  never equal, since type-incompatible
