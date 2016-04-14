(ns test.emap
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
           [migae.datastore DuplicateKeyException])
  ;; (:use [clj-logging-config.log4j])
  (:require [clojure.test :refer :all]
            [migae.datastore.model.entity-map :as em]
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
        ;; (em/init)
        (test-fn)
        (.tearDown helper))))
        ;; (ApiProxy/setEnvironmentForCurrentThread environment)
        ;; (ApiProxy/setDelegate delegate))))

;(use-fixtures :once (fn [test-fn] (dss/get-datastore-service) (test-fn)))
(use-fixtures :each ds-fixture)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;        entity-map create funcs
;;  !   throw exception if already exists
;;  ?-!  ignore em arg if already exists
;;  ?+!  merge em arg if already exists
;;  !!  replace if existant (i.e. discard existing)

(deftest ^:entity-map entity-map!
  (testing "create or fetch"
    (em/entity-map! [:Foo/Bar] {})
    (try (em/entity-map! [:Foo/Bar] {})
         ;; (catch RuntimeException e (do (log/trace "entity already exists"))))
         (catch DuplicateKeyException e
           (is (= (.getMessage e) "[:Foo/Bar]"))))
         ;; (catch EntityNotFoundException e
         ;;   (throw e)))
    ;; (is (= (try (em/entity-map?? [:A/B 'C/D])
    ;;                   (catch IllegalArgumentException e
    ;;                     (log/trace "Exception:" (.getMessage e))
    ;;                     (.getClass e)))
    ;;        IllegalArgumentException))
    ))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; FIXME
;; (deftest ^:entity-map entity-map??
;;   (testing "entity-map?!"
;;     (try (em/entity-map?? [:Foo/Baz])
;;          (catch EntityNotFoundException ex))
;;     ))

(deftest ^:entity-map entity-map1
  (testing "entity-map key vector must not be empty"
    (let [e (try (em/entity-map [] {})
                 (catch java.lang.IllegalArgumentException ex ex))]
      (is (= "Null keychain '[]' not allowed for local ctor"
             (.getMessage e))))))

(deftest ^:entity-map entity-map?
  (testing "entitymap deftype"
    (binding [*print-meta* true]
      (let [em1 (em/entity-map [:Species/Felis_catus] {:name "Chibi"})
            em2 (em/entity-map [:Genus/d99 :Species/Felis_catus] {:name "Chibi"})
            em2b (em/entity-map [(keyword "Genus" "999") :Species/Felis_catus] {:name "Chibi"})
            em2c (em/entity-map [:Genus/Felis :Species/Felis_catus] {:name "Chibi"})
            em3 (em/entity-map [:Subfamily/Felinae :Genus/Felis :Species/Felis_catus] {:name "Chibi"})
            em4 (em/entity-map [:Family/Felidae :Subfamily/Felinae :Genus/Felis :Species/Felis_catus] {:name "Chibi"})]
        (log/trace "em1" em1)
        (is (em/entity-map? em1))
        (log/trace "em2" em2)
        (is (em/entity-map? em2))
        (log/trace "em3" em3)
        (is (em/entity-map? em3))
        (log/trace "em4" em4)
        (is (em/entity-map? em4))
        ))))

(deftest ^:entity-map entity-map?!
  (testing "entitymap deftype"
    (binding [*print-meta* true]
      (let [em1 (em/entity-map! [:Species/Felis_catus] {})
            em2 (em/entity-map [:Genus/Felis :Species/Felis_catus] {})
            em3 (em/entity-map [:Subfamily/Felinae :Genus/Felis :Species/Felis_catus] {})
            em4 (em/entity-map [:Family/Felidae :Subfamily/Felinae :Genus/Felis :Species/Felis_catus] {})]
        (is (em/entity-map? em1))
        (is (em/entity-map? em2))
        (is (em/entity-map? em3))
        (is (em/entity-map? em4)))

      (let [k [:Genus/Felis :Species/Felis_catus]]
;; FIXME: support this stuff?
        ;; (log/trace "?!"   (em/entity-map?!  k {:name "Chibi" :size "small" :eyes 1}))
        ;; ;; (try              (em/entity-map?!  k {:name "Chibi" :size "small" :eyes 1})
        ;; ;;      (catch Exception e (log/trace "Exception:" (.getMessage e))))

        ;; ;; =? override maybe
        ;; (log/trace "=?" (em/entity-map=? k  {:name "Chibi" :size "large"}))

        ;; ;; +? - extend maybe
        ;; (log/trace "+?" (em/entity-map+? k  {:name "Chibi" :size "large" :foo "bar"}))

        ;; ;; =! - override necessarily
        ;; (log/trace "=!" (em/entity-map=! k  {:name "Chibi" :size "large"}))

        ;; ;; !!  - replace necessarily i.e. discard old and create new
        ;; (log/trace "!!" (em/entity-map! k  {:name "Booger" :size "medium"}))

        ;; ;; ?? - find maybe
        ;; (log/trace "??" (em/entity-map?? k))
        )
      )))

(deftest ^:emap emap-axioms
  (testing "entity-map axioms"
    (let [em1 (em/entity-map [:Species/Felis_catus] {:name "Chibi"})
          ]
      (log/info "em1" (em/dump em1))
      (log/info "em1 type:" (type em1))
      (log/info "em1 kind:" (em/kind em1))
      (log/info "em1 ident:" (em/identifier em1) " (type: " (type (em/identifier em1)) ")")
      (log/info "em1 keychain:" (em/keychain em1))
      (log/info "em1 keychain type:" (type (em/keychain em1)))
;; FIXME      (log/info "em1 key:" (key em1))
;; FIXME      (log/info "em1 key type:" (type (key em1)))
;; FIXME      (log/info "em1 content:" (val em1))
;; FIXME      (log/info "em1 content type:" (type (val em1)))
      (is (em/entity-map? em1))
      )))

(deftest ^:entity-map entity-map-props-1
  (testing "entity-map! with properties"
    ;; (binding [*print-meta* true]
      (let [k [:Genus/Felis :Species/Felis_catus]
            e1 (em/entity-map! k {:name "Chibi" :size "small" :eyes 1})
            e2 (em/entity-map* k)]
        (log/trace "e1" e1)
        (log/trace "e1 entity" (.content e1))
        (log/trace "e2" e2)
        (log/trace "e2 entity" (.content e2))
        (is (= (e1 :name) "Chibi"))
        (is (= (e2 :name) "Chibi"))
        ;; (should-fail (is (= e1 e2)))
        (is (em/keychains=? e1 e2))
        )))

(deftest ^:entity-map entity-map-fetch
  (testing "entity-map! new, update, replace"
    ;; ignore new if exists
    (let [em1 (em/entity-map! [:Species/Felis_catus] {:name "Chibi"})]
;; FIXME: implement support for :or, meaning: fetch if exists, otherwise create and save
          ;; em2 (em/entity-map! :or [:Species/Felis_catus] {:name "Felix"})]
        ;; (is (em/keys=? em1 em2))
        ;; (is (= (get em1 :name) "Chibi"))
        ;; (is (= (get em2 :name) "Chibi"))
        )

    ;; ! do not override existing
    ;; (let [em2 (em/entity-map! [:Species/Felis_catus] {:name "Booger"})]
    ;;   (log/trace "em2 " em2)
    ;;   (is (= (:name em2) "Chibi")))

    ;; ;; !! - update existing
    ;; (let [em3 (em/entity-map! [:Species/Felis_catus] {:name "Booger"})
    ;;       em3 (em/entity-map! [:Species/Felis_catus] {:name 4})]
    ;;   (log/trace "em3 " em3)
    ;;   (is (= (:name em3) ["Chibi", "Booger" 4]))
    ;;   (is (= (first (:name em3)) "Chibi")))

    ;; ;; replace existing
    ;; (let [em4 (em/alter! [:Species/Felis_catus] {:name "Max"})]
    ;;   (log/trace "em4 " em4)
    ;;   (is (= (:name em4) "Max")))

    ;; (let [em5 (em/entity-map! [:Species/Felis_catus :Name/Chibi]
    ;;                    {:name "Chibi" :size "small" :eyes 1})
    ;;       em6 (em/alter!  [:Species/Felis_catus :Name/Booger]
    ;;                    {:name "Booger" :size "lg" :eyes 2})]
    ;;   (log/trace "em5" em5)
    ;;   (log/trace "em6" em6))
    ))

(deftest ^:entity-map entity-map-fn
  (testing "entity-map fn"
    (let [em1 (em/entity-map! [:Species/Felis_catus] {:name "Chibi"})]
      (log/trace em1))
      ))

;; ################################################################
;; NB:  the entity-maps! family does not (yet) create entities
;; (deftest ^:entity-map entity-maps
;;   (testing "entity-maps"
;;     (let [em1 (em/entity-maps! :Species (>= :weight 5))
;;           ;; em2 (em/entity-maps! :Species (and (>= :weight 5)
;;           ;;                              (<= :weight 7)))
;;           ]
;;       (log/trace em1)
;;       )))

