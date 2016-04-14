(ns test.ctor-local
  "unit tests for local constructor: entity-map"
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
           [migae.datastore InvalidKeychainException])
  ;; (:use [clj-logging-config.log4j])
  (:require [clojure.test :refer :all]
            [migae.datastore.model.entity-map :as ds]
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

(deftest ^:ctor ctor-fail-1
  (testing "keychain must not be empty"
    (let [e (try (ds/entity-map [] {})
                 (catch java.lang.IllegalArgumentException x x))]
      (log/info "e: " (.getMessage e))
      (is (= "Null keychain '[]' not allowed for local ctor"
             (.getMessage e)))
      )))

(deftest ^:ctor ctor-fail-1.1
  (testing "keychain must be vector of keywords"
    (let [e (try (ds/entity-map [1 2] {})
                 (catch InvalidKeychainException x x))]
      (is (= (.getMessage e) "[1 2]")))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;  binary representation:  [:a/b] {:x 1}

(deftest ^:ctor ctor-bin
  (testing "for vector keychain, property map arg may be null or empty"
    (let [em1 (ds/entity-map [:A/B] {})
          em2 (try (ds/entity-map [:A/B] )
                   (catch clojure.lang.ArityException x x))]
      (log/info "em2: " (meta em2) em2))))

(deftest ^:ctor ctor-fail-3
  (testing "local ctor requires proper keychain"
    (let [em1 (ds/entity-map [:A/B] {}) ;; ok
          em2 (try (ds/entity-map [:A] {})
                   (catch java.lang.IllegalArgumentException x x))
          ;; AssertionError x x))
          em3 (try (ds/entity-map [:A/B :C] {})
                   (catch java.lang.IllegalArgumentException x x))]
      (log/info "em2: " (.getMessage em2))
      (is (= "Improper keychain '[:A]' not allowed for local ctor"
           (.getMessage em2)))
      (log/info "em3: " (.getMessage em3))
      (is (= "Improper keychain '[:A/B :C]' not allowed for local ctor"
           (.getMessage em3)))
      )))


(deftest ^:ctor ctor-fail-4
  (testing "local ctor arg must be map")
    (let [em1 (try (ds/entity-map [:A/B] [1 2])
                   (catch java.lang.IllegalArgumentException x x))]
      (is (= "Invalid map arg [1 2]"
             (.getMessage em1)))))

(deftest ^:ctor bin
  (testing "entity-map ctor"
    (let [em1 (ds/entity-map [:Species/Felis_catus] {:name "Chibi"})
          em2 (ds/entity-map [:Genus/Felis :Species/Felis_catus] {:name "Chibi"})
          em3 (ds/entity-map [:Subfamily/Felinae :Genus/Felis :Species/Felis_catus] {:name "Chibi"})
          em4 (ds/entity-map [:Family/Felidae :Subfamily/Felinae :Genus/Felis :Species/Felis_catus] {:name "Chibi"})]
      (log/info "em1" (ds/dump em1))
      ;; (log/info "em1 kind:" (ds/kind em1))
      ;; (log/info "em1 ident:" (ds/identifier em1) " (type: " (type (ds/identifier em1)) ")")
      ;; (log/info "em1" (.entity em3))
      (is (ds/entity-map? em1))
      (is (ds/entity-map? em2))
      (is (ds/entity-map? em3))
      (is (ds/entity-map? em4))
      )))

;; (deftest ^:emap hashmap-axioms
;;   (testing "entity-hashmap axioms"
;;     (let [em1 (ds/entity-hashmap [:Species/Felis_catus] {:name "Chibi"})
;;           ]
;;       (log/info "em1" (ds/dump em1))
;;       (log/info "em1 type:" (type em1))
;;       (log/info "em1 kind:" (ds/kind em1))
;;       (log/info "em1 ident:" (ds/identifier em1) " (type: " (type (ds/identifier em1)) ")")
;;       (log/info "em1 keychain:" (.k em1))
;;       (log/info "em1 keychain type:" (type (.k em1)))
;;       (log/info "em1 content:" (.content em1))
;;       (log/info "em1 content type:" (type (.content em1)))
;;       (is (ds/entity-map? em1))
;;       )))

;; (deftest ^:emap hashmap-ctor-2
;;   (testing "entity-hashmap ctor"
;;     (let [em1 (ds/entity-hashmap [:Species/Felis_catus] {:name "Chibi"})
;;           em2 (ds/entity-hashmap [:Genus/Felis :Species/Felis_catus] {:name "Chibi"})
;;           em3 (ds/entity-hashmap [:Subfamily/Felinae :Genus/Felis :Species/Felis_catus] {:name "Chibi"})
;;           em4 (ds/entity-hashmap [:Family/Felidae :Subfamily/Felinae :Genus/Felis :Species/Felis_catus] {:name "Chibi"})
;;           ]
;;       (log/info "em1" (ds/dump em1))
;;       (log/info "em1 type:" (type em1))
;;       (log/info "em1 kind:" (ds/kind em1))
;; ;; FIXME      (log/info "em1 ident:" (ds/identifier em1) " (type: " (type (ds/identifier em1)) ")")
;;       (log/info "em1 keychain:" (.k em1))
;;       (log/info "em1 keychain type:" (type (.k em1)))
;;       (log/info "em1 content:" (.content em1))
;;       (log/info "em1 content type:" (type (.content em1)))
;;       (is (ds/entity-map? em1))
;;       ;; (is (ds/entity-map? em2))
;;       ;; (is (ds/entity-map? em3))
;;       ;; (is (ds/entity-map? em4))
;;       )))

(deftest ^:emap emap-ctor2
  (testing "entity-map ctor2"
    (let [em1 (ds/entity-map [:A/B :C/D] {:a 1 :b 2})]
      (is (ds/entity-map? em1))
;; FIXME      (is (= (key em1) [:A/B :C/D]))
;; FIXME      (is (= (val em1) {:a 1 :b 2}))
;; FIXME:  keys and vals should not include :migae/keychain?
      (is (= (keys em1) [:a :b]))
      (is (= (vals em1) [1 2]))
      )))

(deftest ^:emap emap-empty-ctor
  (testing "entity-map empty ctor"
    (let [em1 (ds/entity-map [:A/B] {})
          em2 (ds/entity-map [:A/B] {})]
      ;; (log/info "em1" (ds/dump em1))
      ;; (log/info "em1 kind:" (ds/kind em1))
      ;; (log/info "em1 ident:" (ds/identifier em1) " (type: " (type (ds/identifier em1)) ")")
      ;; (log/info "em1" (.entity em3))
      (is (ds/entity-map? em1))
      (is (ds/entity-map? em2))
      )))

(deftest ^:emap emap-id-ctor
  (testing "entity-map ctor: ids"
    (let [em1 (ds/entity-map [:Genus/#10] {:a 1})
          em2 (ds/entity-map [(keyword "Genus" "10")] {:a 1})
          em3 (ds/entity-map [:Genus/#0x10] {:a 1})
          em4 (ds/entity-map [:Genus/#0x0A] {:a 1})]
      (log/info "START EM1")
      (log/info "id em1" (ds/identifier em1))
      (log/info "START EM2")
      (log/info "id em2" (ds/identifier em2))
      (log/info "id em3" (ds/identifier em3))
      (log/info "id em4" (ds/identifier em4))
      ;; (log/info "em3 kind:" (ds/kind em3))
      ;; (log/info "em3 ident:" (ds/identifier em3) " (type: " (type (ds/identifier em3)) ")")
      (is (= (ds/identifier em1)
             (ds/identifier em2)
             ;; (ds/identifier em3)
             (ds/identifier em4)))
      (is (= (type (ds/identifier em1))
             (type (ds/identifier em2))
             (type (ds/identifier em3))
             (type (ds/identifier em4))
             java.lang.Long))
      (is (= (ds/kind em1)
             (ds/kind em2)
             (ds/kind em3)
             (ds/kind em4)))
      (is (= (type (ds/kind em1))
             (type (ds/kind em2))
             (type (ds/kind em3))
             (type (ds/kind em4))
             java.lang.String))
      )))

;; ################################################################
;; NB:  the emaps! family does not (yet) create entities
;; (deftest ^:emap emaps
;;   (testing "emaps"
;;     (let [em1 (ds/emaps! :Species (>= :weight 5))
;;           ;; em2 (ds/emaps! :Species (and (>= :weight 5)
;;           ;;                              (<= :weight 7)))
;;           ]
;;       (log/info em1)
;;       )))


;; FIXME: re-enable this using push ctor, maybe entity-map! :multi
;; (deftest ^:emaps emaps
;;   (testing "use emaps!! to create multiple PersistentEntityMaps of a kind in one stroke"
;;     (ds/emaps!! [:Foo] [{:a 1} {:a 2} {:a 3}])
;;     (ds/emaps!! [:Foo/Bar :Baz] [{:b 1} {:b 2} {:b 3}])
;;     ))


;;;;  FIXME: move these tests to emaps.clj, they are about emap props, not ctors

(deftest ^:em-preds em-predicate-axiom1
  (testing "entity-map predicate axiom 1: what entity-maps are."
    (let [k [:A/B]
          em (ds/entity-map k {:a 1 :b 2})]
      (is (associative? em))
      (is (counted? em))
      (is (coll? em))
      (is (map? em))
    )))

(deftest ^:em-preds em-predicate-axiom2
  (testing "entity-map predicate axiom 2: what entity-maps are not."
    (let [em (ds/entity-map  [:A/B] {:a 1 :b 2})]
      (is (not (sequential? em)))
      (is (not (sorted? em)))
      (is (not (reversible? em)))
      (is (not (list? em)))
      (is (not (vector? em)))
      (is (not (set? em)))
      (is (not (seq? em)))
      (is (not (record? em)))
    )))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(deftest ^:em-preds quant-axiom1
  (testing "entity-map quantification axiom 1: every?"
    (let [em (ds/entity-map  [:A/B] {:a 1 :b 2})]
      ;; use entity-map collection as predicate
      (is (every? em [:a :b]))
      (is (every? em (keys em))) ;; em qua predicate satisfied by every one of its keys
      )))

(deftest ^:em-preds quant-axiom2
  (testing "entity-map quantification axiom 2: not-every?"
    (let [em (ds/entity-map  [:A/B] {:a 1 :b 2})]
      (is (not-every? em [:a :b :c]))
      )))

(deftest ^:em-preds quant-axiom3
  (testing "entity-map quantification axiom 3: some"
    (let [em (ds/entity-map  [:A/B] {:a 1 :b 2})]
      (is (some? em))
      (is (some em [:a]))
      (is (not (some em [:x])))
      (is (some em [:a :x :y]))
      )))

(deftest ^:em-preds quant-axiom4
  (testing "entity-map quantification axiom 4: not-any?"
    (let [em (ds/entity-map  [:A/B] {:a 1 :b 2})]
      (is (not-any? em [:x :y]))
      (is (not (not-any? em [:x :a :y])))
      )))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;  vector representation, e.g.  [[:a/b]{:x 1}]
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(deftest ^:ctor ctor-vec
  (testing "entity-map ctor for vector form"
    (let [em (ds/entity-map [[:A/B] {:a 1 :b 2}])]
      (is (ds/entity-map? em))
      (is (not-any? em [:x :y]))
      (is (not (not-any? em [:x :a :y])))
      )))
