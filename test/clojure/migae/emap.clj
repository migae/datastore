(ns migae.emap
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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;        emap create funcs
;;  !   throw exception if already exists
;;  ?-!  ignore em arg if already exists
;;  ?+!  merge em arg if already exists
;;  !!  replace if existant (i.e. discard existing)

(deftest ^:emap emap!
  (testing "create or fetch"
    (ds/emap! [:Foo/Bar])
    (try (ds/emap! [:Foo/Bar])
         (catch RuntimeException e (do (log/trace "entity already exists"))))
         ;; (catch EntityNotFoundException e
         ;;   (throw e)))
    ;; (is (= (try (ds/emap?? [:A/B 'C/D])
    ;;                   (catch IllegalArgumentException e
    ;;                     (log/trace "Exception:" (.getMessage e))
    ;;                     (.getClass e)))
    ;;        IllegalArgumentException))
    ))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(deftest ^:emap emap??
  (testing "emap?!"
    (try (ds/emap?? [:Foo/Baz])
         (catch EntityNotFoundException ex))
    ))

(deftest ^:emaps emap-def
  (testing "emap!! definite"
    (let [e1 (ds/emap!! [:Foo/Bar] {:a 1})
          e2 (ds/emap!! [:Foo/Bar :Baz/Buz] {:b 1})
          e3 (ds/emap!! [:Foo/Bar :Baz/Buz :X/Y] {:b 1})]
      (log/trace "e1" (ds/epr e1))
      (log/trace "e2" (ds/epr e2))
      (log/trace "e3" (ds/epr e3))
    )))

(deftest ^:emaps emap-indef
  (testing "emap!! indefinite"
    (let [e1 (ds/emap!! [:Foo] {:a 1 :b "Foobar"})
          e2 (ds/emap!! [:Foo/Bar :Baz] {:b 1})
          e3 (ds/emap!! [:Foo/Bar :Baz/Buz :X] {:a "Foo/Bar Baz/Buz Z"})]
      (log/trace "e1" (ds/epr e1))
      (log/trace "e2" (ds/epr e2))
      (log/trace "e3" (ds/epr e3))
    )))

(deftest ^:emaps emaps-multi
  (testing "use emaps!! to create multiple PersistentEntityMaps of a kind in one stroke"
    (ds/emap!! [:Foo] [{:a 1} {:a 2} {:a 3}])
    (ds/emap!! [:Foo/Bar :Baz] [{:b 1} {:b 2} {:b 3}])
    ))

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

(deftest ^:emap emap?!
  (testing "entitymap deftype"
    (binding [*print-meta* true]
      (let [em1 (ds/emap!! [:Species/Felis_catus])
            em2 (ds/emap [:Genus/Felis :Species/Felis_catus] {})
            em3 (ds/emap [:Subfamily/Felinae :Genus/Felis :Species/Felis_catus] {})
            em4 (ds/emap [:Family/Felidae :Subfamily/Felinae :Genus/Felis :Species/Felis_catus] {})]
        (is (ds/emap? em1))
        (is (ds/emap? em2))
        (is (ds/emap? em3))
        (is (ds/emap? em4)))

      (let [k [:Genus/Felis :Species/Felis_catus]]
        (log/trace "?!"   (ds/emap?!  k {:name "Chibi" :size "small" :eyes 1}))
        ;; (try              (ds/emap?!  k {:name "Chibi" :size "small" :eyes 1})
        ;;      (catch Exception e (log/trace "Exception:" (.getMessage e))))

        ;; =? override maybe
        (log/trace "=?" (ds/emap=? k  {:name "Chibi" :size "large"}))

        ;; +? - extend maybe
        (log/trace "+?" (ds/emap+? k  {:name "Chibi" :size "large" :foo "bar"}))

        ;; =! - override necessarily
        (log/trace "=!" (ds/emap=! k  {:name "Chibi" :size "large"}))

        ;; !!  - replace necessarily i.e. discard old and create new
        (log/trace "!!" (ds/emap!! k  {:name "Booger" :size "medium"}))

        ;; ?? - find maybe
        (log/trace "??" (ds/emap?? k))
        )
      )))

(deftest ^:emap emap-props-1
  (testing "emap! with properties"
    ;; (binding [*print-meta* true]
      (let [k [:Genus/Felis :Species/Felis_catus]
            e1 (ds/emap! k {:name "Chibi" :size "small" :eyes 1})
            e2 (ds/emap?? k)]
        (log/trace "e1" e1)
        (log/trace "e1 entity" (.entity e1))
        (log/trace "e2" e2)
        (log/trace "e2 entity" (.entity e2))
        (is (= (e1 :name) "Chibi"))
        (is (= (e2 :name) "Chibi"))
        (is (= (:name (ds/emap! k)) "Chibi"))
        (should-fail (is (= e1 e2)))
        (is (ds/key= e1 e2))
        )))

(deftest ^:emap emap-fetch
  (testing "emap! new, update, replace"
    ;; ignore new if exists
    (let [em1 (ds/emap! [:Species/Felis_catus] {:name "Chibi"})
          em2 (ds/emap! [:Species/Felis_catus] {})]
        (is (ds/key= em1 em2))
        (is (= (get em1 :name) "Chibi"))
        (is (= (get em2 :name) "Chibi")))

    ;; ! do not override existing
    (let [em2 (ds/emap! [:Species/Felis_catus] {:name "Booger"})]
      (log/trace "em2 " em2)
      (is (= (:name em2) "Chibi")))

    ;; !! - update existing
    (let [em3 (ds/emap!! [:Species/Felis_catus] {:name "Booger"})
          em3 (ds/emap!! [:Species/Felis_catus] {:name 4})]
      (log/trace "em3 " em3)
      (is (= (:name em3) ["Chibi", "Booger" 4]))
      (is (= (first (:name em3)) "Chibi")))

    ;; replace existing
    (let [em4 (ds/alter! [:Species/Felis_catus] {:name "Max"})]
      (log/trace "em4 " em4)
      (is (= (:name em4) "Max")))

    (let [em5 (ds/emap! [:Species/Felis_catus :Name/Chibi]
                       {:name "Chibi" :size "small" :eyes 1})
          em6 (ds/alter!  [:Species/Felis_catus :Name/Booger]
                       {:name "Booger" :size "lg" :eyes 2})]
      (log/trace "em5" em5)
      (log/trace "em6" em6))
    ))

(deftest ^:emap emap-fn
  (testing "emap fn"
    (let [em1 (ds/emap! [:Species/Felis_catus] {:name "Chibi"})]
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

