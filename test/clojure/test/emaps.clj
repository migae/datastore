(ns test.emaps
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
            [migae.infix :as infix]
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

(deftest ^:emap emap1
  (testing "emap key vector must not be empty"
    (try (ds/emap [] {})
         (catch IllegalArgumentException e
           (log/trace (.getMessage e))))))

(deftest ^:emap emap?
  (testing "entitymap deftype"
    (binding [*print-meta* true]
      (let [em1 (ds/emap [:Species/Felis_catus] {:name "Chibi"})
            em2 (ds/emap [:Genus/d99 :Species/Felis_catus] {:name "Chibi"})
            em2b (ds/emap [(keyword "Genus" "999") :Species/Felis_catus] {:name "Chibi"})
            em2c (ds/emap [:Genus/Felis :Species/Felis_catus] {:name "Chibi"})
            em3 (ds/emap [:Subfamily/Felinae :Genus/Felis :Species/Felis_catus] {:name "Chibi"})
            em4 (ds/emap [:Family/Felidae :Subfamily/Felinae :Genus/Felis :Species/Felis_catus] {:name "Chibi"})]
        (log/trace "em1" em1)
        (is (ds/emap? em1))
        (log/trace "em2" em2)
        (is (ds/emap? em2))
        (log/trace "em3" em3)
        (is (ds/emap? em3))
        (log/trace "em4" em4)
        (is (ds/emap? em4))
        ))))

(deftest ^:emap emap!
  (testing "entitymap deftype"
    (binding [*print-meta* true]
      (let [em1 (ds/entity-map! [:Species/Felis_catus] {})
            em2 (ds/emap [:Genus/Felis :Species/Felis_catus] {})
            em3 (ds/emap [:Subfamily/Felinae :Genus/Felis :Species/Felis_catus] {})
            em4 (ds/emap [:Family/Felidae :Subfamily/Felinae :Genus/Felis :Species/Felis_catus] {})]
        (is (ds/emap? em1))
        (is (ds/emap? em2))
        (is (ds/emap? em3))
        (is (ds/emap? em4))
        ))))

(deftest ^:emap emap-props-1
  (testing "entity-map! with properties"
    ;; (binding [*print-meta* true]
      (let [k [:Genus/Felis :Species/Felis_catus]
            e1 (ds/entity-map! k {:name "Chibi" :size "small" :eyes 1})
            e2 (ds/entity-map! k)]
        (log/trace "e1" e1)
        (log/trace "e1 entity" (.entity e1))
        (log/trace "e2" e2)
        (log/trace "e2 entity" (.entity e2))
        (is (= (e1 :name) "Chibi"))
        (is (= (e2 :name) "Chibi"))
        (is (= (:name (ds/entity-map! k)) "Chibi"))
        (should-fail (is (= e1 e2)))
        (is (ds/key= e1 e2))
        )))

(deftest ^:emap emap-fetch
  (testing "entity-map! new, update, replace"
    ;; ignore new if exists
    (let [em1 (ds/entity-map! [:Species/Felis_catus] {:name "Chibi"})
          em2 (ds/entity-map! [:Species/Felis_catus] {})]
        (is (ds/key= em1 em2))
        (is (= (get em1 :name) "Chibi"))
        (is (= (em1 :name) "Chibi"))
        (is (= (:name em1) "Chibi"))
        (is (= (get em2 :name) "Chibi")))

    ;; ! do not override existing
    (let [em2 (ds/entity-map! [:Species/Felis_catus] {:name "Booger"})]
      (log/trace "em2 " em2)
      (is (= (:name em2) "Chibi")))

    ;; !! - update existing
    (let [em3 (ds/emap!! [:Species/Felis_catus] {:name "Booger"})
          em3 (ds/emap!! [:Species/Felis_catus] {:name 4})]
      (log/trace "em3 " em3)
      (log/trace "em3: " (.entity em3))
      (log/trace "em3 key: " (:migae/key (meta em3)))
      (is (= (:name em3) ["Chibi", "Booger" 4]))
      (is (= (first (:name em3)) "Chibi")))

    ;; replace existing
    (let [em4 (ds/alter! [:Species/Felis_catus] {:name "Max"})]
      (log/trace "em4 " em4)
      (is (= (:name em4) "Max")))

    (let [em5 (ds/entity-map! [:Species/Felis_catus :Name/Chibi]
                       {:name "Chibi" :size "small" :eyes 1})
          em6 (ds/alter!  [:Species/Felis_catus :Name/Booger]
                       {:name "Booger" :size "lg" :eyes 2})]
      (log/trace "em5" em5)
      (log/trace "em6" em6))
    ))

(deftest ^:emap emap-fn
  (testing "emap fn"
    (let [em1 (ds/entity-map! [:Species/Felis_catus] {:name "Chibi"})]
      (log/trace em1))
      ))

;; ################################################################
;; NB:  the emaps! family does not (yet) create entities
;; (deftest ^:emap emaps
;;   (testing "emaps"
;;     (let [em1 (ds/emaps! :Species (>= :weight 5))
;;           ;; em2 (ds/emaps! :Species (and (>= :weight 5)
;;           ;;                              (<= :weight 7)))
;;           ]
;;       (log/trace em1)
;;       )))


(deftest ^:emaps emaps
  (testing "use emaps!! to create multiple PersistentEntityMaps of a kind in one stroke"
    (ds/emaps!! [:Foo] [{:a 1} {:a 2} {:a 3}])
    (ds/emaps!! [:Foo/Bar :Baz] [{:b 1} {:b 2} {:b 3}])
    ))
