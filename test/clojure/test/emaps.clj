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
            ;; [migae.datastore :as ds]
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
        ;; (ds/init)
        (test-fn)
        (.tearDown helper))))
        ;; (ApiProxy/setEnvironmentForCurrentThread environment)
        ;; (ApiProxy/setDelegate delegate))))

;(use-fixtures :once (fn [test-fn] (dss/get-datastore-service) (test-fn)))
(use-fixtures :each ds-fixture)

(deftest ^:coll entity-map-axiom1
  (testing "entity-map axiom 1: an entity-map is a map"
    (let [e1 (ds/entity-map [:A/B] {:a 1 :b 2})]
      ;; (log/trace "e1 " (ds/dump e1))
      (is (coll? e1))
      (is (map? e1))
      (is (ds/entity-map? e1))
      (is (= (keys e1) '(:a :b)))
      (is (= (vals e1) '(1 2)))
      )))

(deftest ^:coll entity-map-axiom2
  (testing "entity-map axiom 2: an entity-map is seqable"
    (let [e1 (ds/entity-map [:A/B] {:a 1 :b 2})]
      (is (not (seq? e1)))
      (is (seq? (seq e1)))
      )))

(deftest ^:entity-map entity-map1
  (testing "entity-map key vector must not be empty"
    (let [ex (try (ds/entity-map [] {})
                  (catch java.lang.IllegalArgumentException x x))]
      (is (= "Null keychain '[]' not allowed for local ctor"
             (.getMessage ex))))))

(deftest ^:entity-map entity-map?
  (testing "entitymap deftype"
    (binding [*print-meta* true]
      (let [em1 (ds/entity-map [:Species/Felis_catus] {:name "Chibi"})
            em2 (ds/entity-map [:Genus/d99 :Species/Felis_catus] {:name "Chibi"})
            em2b (ds/entity-map [(keyword "Genus" "999") :Species/Felis_catus] {:name "Chibi"})
            em2c (ds/entity-map [:Genus/Felis :Species/Felis_catus] {:name "Chibi"})
            em3 (ds/entity-map [:Subfamily/Felinae :Genus/Felis :Species/Felis_catus] {:name "Chibi"})
            em4 (ds/entity-map [:Family/Felidae :Subfamily/Felinae :Genus/Felis :Species/Felis_catus] {:name "Chibi"})]
        (log/trace "em1" em1)
        (is (ds/entity-map? em1))
        (log/trace "em2" em2)
        (is (ds/entity-map? em2))
        (log/trace "em3" em3)
        (is (ds/entity-map? em3))
        (log/trace "em4" em4)
        (is (ds/entity-map? em4))
        ))))

(deftest ^:entity-map entity-map!
  (testing "entitymap deftype"
    (binding [*print-meta* true]
      (let [em1 (ds/entity-map! [:Species/Felis_catus] {})
            em2 (ds/entity-map [:Genus/Felis :Species/Felis_catus] {})
            em3 (ds/entity-map [:Subfamily/Felinae :Genus/Felis :Species/Felis_catus] {})
            em4 (ds/entity-map [:Family/Felidae :Subfamily/Felinae :Genus/Felis :Species/Felis_catus] {})]
        (is (ds/entity-map? em1))
        (is (ds/entity-map? em2))
        (is (ds/entity-map? em3))
        (is (ds/entity-map? em4))
        ))))

(deftest ^:entity-map entity-map-props-1
  (testing "entity-map! with properties"
    ;; (binding [*print-meta* true]
      (let [k [:Genus/Felis :Species/Felis_catus :Cat]
            e1 (ds/entity-map! k {:name "Chibi" :size "small" :eyes 1})
            e2 (ds/entity-map! k {:name "Booger" :size "medium" :eyes 2})
            ]
        (log/trace "e1" e1)
        (log/trace "e1 entity" (.content e1))
        (log/trace "e2" e2)
        (log/trace "e2 entity" (.content e2))
        (is (= (e1 :name) "Chibi"))
        (is (= (e2 :name) "Booger"))
        (is (= (ds/kind (ds/entity-map! k {})) :Cat))
        ;; FIXME (should-fail (is (= e1 e2)))
        ;; FIXME (is (ds/key=? e1 e2))
        )))

(deftest ^:entity-map entity-map-fetch
  (testing "entity-map! new, update, replace"
    ;; ignore new if exists
    (let [em1 (ds/entity-map! [:Species/Felis_catus :Cat] {:name "Chibi"})
          em2 (ds/entity-map! [:Species/Felis_catus :Cat] {:name "Booger"})]
        ;; FIXME (is (not (ds/key=? em1 em2)))
        ;; FIXME: is (= pfx em1 pfx em2 (i.e. same ancestry)
        (is (= (get em1 :name) "Chibi"))
        (is (= (em1 :name) "Chibi"))
        (is (= (:name em1) "Chibi"))
        (is (= (get em2 :name) "Booger")))

    ;; ! do not override existing
    ;; FIXME
    ;; (let [em2 (ds/entity-map! [:Species/Felis_catus] {:name "Booger"})]
    ;;   (log/trace "em2 " em2)
    ;;   (is (= (:name em2) "Chibi")))

    ;; !! - update existing
    ;; FIXME
    ;; (let [em3 (ds/entity-map!! [:Species/Felis_catus] {:name "Booger"})
    ;;       em3 (ds/entity-map!! [:Species/Felis_catus] {:name 4})]
    ;;   (log/trace "em3 " em3)
    ;;   (log/trace "em3: " (.entity em3))
    ;;   (log/trace "em3 key: " (:migae/key (meta em3)))
    ;;   (is (= (:name em3) ["Chibi", "Booger" 4]))
    ;;   (is (= (first (:name em3)) "Chibi")))

    ;; replace existing
    ;; FIXME: implement alter
    ;; (let [em4 (ds/alter! [:Species/Felis_catus] {:name "Max"})]
    ;;   (log/trace "em4 " em4)
    ;;   (is (= (:name em4) "Max")))

    ;; (let [em5 (ds/entity-map! [:Species/Felis_catus :Name/Chibi]
    ;;                    {:name "Chibi" :size "small" :eyes 1})
    ;;       em6 (ds/alter!  [:Species/Felis_catus :Name/Booger]
    ;;                    {:name "Booger" :size "lg" :eyes 2})]
    ;;   (log/trace "em5" em5)
    ;;   (log/trace "em6" em6))
    ))

(deftest ^:entity-map entity-map-fn
  (testing "entity-map fn"
    (let [em1 (ds/entity-map! [:Species/Felis_catus] {:name "Chibi"})]
      (log/trace em1))
      ))

;; ################################################################
;; NB:  the entity-maps! family does not (yet) create entities
;; (deftest ^:entity-map entity-maps
;;   (testing "entity-maps"
;;     (let [em1 (ds/entity-maps! :Species (>= :weight 5))
;;           ;; em2 (ds/entity-maps! :Species (and (>= :weight 5)
;;           ;;                              (<= :weight 7)))
;;           ]
;;       (log/trace em1)
;;       )))


;; FIXME
;; (deftest ^:entity-maps entity-maps
;;   (testing "use entity-maps!! to create multiple PersistentEntityMaps of a kind in one stroke"
;;     (ds/entity-map! :multi [:Foo] [{:a 1} {:a 2} {:a 3}])
;;     (ds/entity-map! :multi [:Foo/Bar :Baz] [{:b 1} {:b 2} {:b 3}])
;;     ))
