(ns test.store-map
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
            EntityNotFoundException]
           [java.lang RuntimeException])
  ;; (:use [clj-logging-config.log4j])
  (:require [clojure.test :refer :all]
            [migae.datastore.model.gae :as ds]
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
;;        (ds/init)
        (test-fn)
        (.tearDown helper))))
        ;; (ApiProxy/setEnvironmentForCurrentThread environment)
        ;; (ApiProxy/setDelegate delegate))))

;(use-fixtures :once (fn [test-fn] (dss/get-datastore-service) (test-fn)))
(use-fixtures :each ds-fixture)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(deftest ^:sm store-map-0
  (testing "store-map 0"
    (log/debug "ds/store-map: " ds/store-map)
    (log/debug "meta ds/store-map: " (meta ds/store-map))
    (log/debug "type ds/store-map: " (type ds/store-map))
    (log/debug "class ds/store-map: " (class ds/store-map))
    (log/debug "map? ds/store-map: " (map? ds/store-map))
    (log/debug "empty? ds/store-map: " (empty? ds/store-map))
    (log/debug "(count ds/store-map): " (count ds/store-map))
    (log/debug "(= {} ds/store-map): " (= {} ds/store-map))
    (log/debug "(= ds/store-map {}): " (= ds/store-map {}))
    (is (= ds/store-map {}))
    (is (= (type ds/store-map) 'migae.datastore.PersistentStoreMap))
    ))

(deftest ^:sm store-map-1
  (testing "store-map 1"
    (let [em (ds/entity-map [:a/b] {:x 1})
          ds (into ds/store-map em)]
        (log/debug "ds: " ds)
        (log/debug "meta ds: " (meta ds))
        (log/debug "type ds: " (type ds))
        (log/debug "class ds: " (class ds))
        (log/debug "(= ds ds/store-map): " (= ds ds/store-map))
        ;; sadly, stats don't work on dev server
        (log/debug "(count ds): " (count ds))
        )))

(deftest ^:sm store-map-2
  (testing "store-map 2"
    (let [ds1 (into ds/store-map (ds/entity-map [:a/b] {:x 1} :em))
          ds2 (into ds1 (ds/entity-map [:a/b :c/d] {:x 1} :em))]
      (log/debug "ds1: " ds1)
      (let [em1 (get ds/store-map [:a/b])
            em2 (get ds/store-map [:a/b :c/d])]
        (log/debug "em1: " (ds/dump-str em1))
        (log/debug "em2: " (ds/dump-str em2))
      ))))

(deftest ^:sm store-map-3
  (testing "store-map 3"
    (into ds/store-map (ds/entity-map [:a/b] {:x 1} :em))
    (let [em1 (get ds/store-map [:a/b])
          em2 (ds/store-map [:a/b])]
        (log/debug "em1: " (ds/dump-str em1))
        (log/debug "em2: " (ds/dump-str em2))
      )))

(deftest ^:sm store-map-1.4
  (testing "store-map 1.4"
    (into ds/store-map (ds/entity-map [:a/b] {:x 1}))
    (let [em1 (try (get ds/store-map (ds/entity-map [:a/b] {:x 1}))
                   (catch IllegalArgumentException x x))]
      (= "PersistentStoreMap.valAt does not yet support map filters"
         (.getMessage em1))
      (log/debug (.getMessage em1))
      )))

(deftest ^:sm store-map-1.5
  (testing "store-map 1.5"
    (let [em (ds/entity-map [:a/b :c/d] {:x 1} :em)
          foo (log/debug "em:" (ds/dump-str em))
          ds (into ds/store-map em)]
      (log/debug "ds: " (ds/dump-str ds))
      (let [em1 (ds/entity-map [:a/b :c/d])
            em2 (get ds/store-map em1)]
        (log/debug "em1: " (ds/dump-str em1))
        (log/debug "em2: " (ds/dump-str em2))
        ))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;  assoc
(deftest ^:sm store-map-8
  (testing "store-map 8"
    (assoc ds/store-map [:a/x] {:x 1})
    (let [em (try (get ds/store-map [:a/x])
                   (catch Exception x x))]
      (log/debug "em: " (ds/dump-str em))
      )))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;  delete ops

(deftest ^:sm store-map-9
  (testing "store-map 9"
    (into ds/store-map (ds/entity-map [:a/x] {:x 1} :em))
    (into ds/store-map (ds/entity-map [:a/y] {:y 2} :em))
    (let [ds (dissoc ds/store-map [:a/x])
          emx (try (get ds/store-map [:a/x])
                   (catch Exception x x))
          emy (get ds/store-map [:a/y])]
      (= (type emx) com.google.appengine.api.datastore.EntityNotFoundException)
      (= "No entity was found matching the key: a(\"x\")" (.getMessage emx))
      (log/debug (.getMessage emx))
      (log/debug "emy: " (ds/dump-str emy))
      )))

(deftest ^:sm store-map-9.1
  (testing "store-map 9.1"
    (into ds/store-map (ds/entity-map [:a/x] {:x 1} :em))
    (into ds/store-map (ds/entity-map [:a/y] {:y 1} :em))
    (into ds/store-map (ds/entity-map [:a/z] {:z 1} :em))

    (let [ds (clojure.core/dissoc ds/store-map [:a/x] [:a/z])
          emx (try (get ds/store-map [:a/x])
                   (catch Exception x x))
          emy (get ds/store-map [:a/y])
          emz (try (get ds/store-map [:a/z])
                   (catch Exception x x))]
      (= (type emx) com.google.appengine.api.datastore.EntityNotFoundException)
      (= "No entity was found matching the key: a(\"x\")" (.getMessage emx))
      (log/debug (.getMessage emx))
      (log/debug "emy: " (ds/dump-str emy))
      (= (type emz) com.google.appengine.api.datastore.EntityNotFoundException)
      (= "No entity was found matching the key: a(\"z\")" (.getMessage emz))
      (log/debug (.getMessage emz))
      )))


;; (deftest ^:sm store-map-4
;;   (testing "store-map 3"
;;     (let [em (ds/entity-map! [:a] {:x 1})]
;;       (log/debug "em: " (ds/dump-str em))
;;       )))
