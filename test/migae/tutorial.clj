(ns migae.tutorial
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
            [migae.infix :as infix]
            [migae.datastore :as ds]
            [migae.datastore.service :as dss]
            [migae.datastore.entity :as dse]
            [migae.datastore.query  :as dsqry]
            [migae.datastore.key    :as dskey]
            [clojure.tools.logging :as log :only [trace debug info]]))
;            [ring-zombie.core :as zombie]))

(defmacro should-fail [body]
  `(let [report-type# (atom nil)]
     (binding [clojure.test/report #(reset! report-type# (:type %))]
       ~body)
     (testing "should fail"
       (is (= @report-type# :fail )))))

;  (:require [migae.migae-datastore.EntityMap])
  ;; (:use clojure.test
  ;;       [migae.migae-datastore :as ds]))

;; (defn datastore [& {:keys [storage? store-delay-ms
;;                            max-txn-lifetime-ms max-query-lifetime-ms
;;                            backing-store-location]
;;                     :or {storage? false}}]
;;   (let [ldstc (LocalDatastoreServiceTestConfig.)]
;;     (.setNoStorage ldstc (not storage?))
;;     (when-not (nil? store-delay-ms)
;;       (.setStoreDelayMs ldstc store-delay-ms))
;;     (when-not (nil? max-txn-lifetime-ms)
;;       (.setMaxTxnLifetimeMs ldstc max-txn-lifetime-ms))
;;     (when-not (nil? max-query-lifetime-ms)
;;       (.setMaxQueryLifetimeMs ldstc max-query-lifetime-ms))
;;     (if-not (nil? backing-store-location)
;;         (.setBackingStoreLocation ldstc backing-store-location)
;;         (.setBackingStoreLocation ldstc "/dev/null"))
;;         ;; (.setBackingStoreLocation ldstc (if (= :windows (os-type))
;;         ;;                                     "NUL"
;;         ;;                                     "/dev/null")))
;;     ldstc))

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
        (dss/get-datastore-service)
        (test-fn)
        (.tearDown helper))))
        ;; (ApiProxy/setEnvironmentForCurrentThread environment)
        ;; (ApiProxy/setDelegate delegate))))

;(use-fixtures :once (fn [test-fn] (dss/get-datastore-service) (test-fn)))
(use-fixtures :each ds-fixture)

(deftest ^:init ds-init
  (testing "DS init"
    (is (= com.google.appengine.api.datastore.DatastoreServiceImpl
           (class (dss/get-datastore-service))))
    (is (= com.google.appengine.api.datastore.DatastoreServiceImpl
           (class @dss/*datastore-service*)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(deftest ^:api emap-predicates
  (testing "emap predicates"
    (let [e (ds/emap!! [:Foo/bar])]
      (log/trace "test: emap-predicates")
      (log/trace "entity: " (.entity e))
      (log/trace "(ds/emap? e): " (ds/emap? e))
      (log/trace "(ds/entity? (.entity e)): " (ds/entity? (.entity e)))
      (log/trace "(ds/empty? e): " (ds/emap? e))
      (log/trace "(ds/key? (ds/key e)): " (ds/key? (ds/key e)))
      (is (ds/emap? e))
      (is (ds/entity? (.entity e)))
      (is (ds/key? (ds/key e)))
      (is (ds/empty? e))
      )))

(deftest ^:api clojure-predicates
  (testing "clojure predicates, entity with 1 property"
    (let [e (ds/emap!! [:Dept/IT] {:a 1})]
      (log/trace "test: api-predicates")
      (log/trace "e " e)
      (log/trace "(coll? e) " (coll? e))
      (log/trace "(associative? e) " (associative? e))
      (log/trace "(list? e) " (list? e))
      (log/trace "(vector? e) " (vector? e))
      (log/trace "(map? e) " (map? e))
      (log/trace "(ifn? e) " (ifn? e))
      (log/trace "(fn? e) " (fn? e))
      (log/trace "(seq? e) " (seq? e))
      (is (ds/emap? e))
      (is (coll? e))
      (is (associative? e))
      (should-fail (is (list? e)))
      (should-fail (is (vector? e)))
      (is (map? e))
      (is (ifn? e))
      (should-fail (is (fn? e)))
      (should-fail (is (seq? e)))
      )))

(deftest ^:api emap-1
  (testing "clojure map api: entity with 1 property"
    (let [e (ds/emap!! [:Dept/IT] {:a 1})]
      (log/trace "test: map-entity-1")
      (log/trace "(map? e) " (map? e))
      (log/trace "(e :a) " (e :a))
      (log/trace "(:a e) " (:a e))
      (log/trace "(get e :a) " (get e :a))
      (log/trace "(contains? e :a) " (contains? e :a))
      (log/trace "(keys e) " (keys e))
      (log/trace "(vals e) " (vals e))
      (log/trace "(find e :a) " (find e :a))
      (log/trace "(count e) " (count e))
      (is (= (e :a) 1))
      (is (= (:a e) 1))
      (is (= (count e) 1))
      (is (= (seq e) '([:a 1])))
      (log/trace "(seq e) " (seq e))
      (log/trace "(assoc e :b 2) " (assoc e :b 2))
      (is (= (count e) 2))
      (is (= (seq e) '([:b 2] [:a 1])))
      (log/trace "(seq e) " (seq e))
      (let [x (assoc e :c 3)]
        (log/trace "(assoc e :c 3) -> " x)
        (log/trace "(type (assoc e :c 3)): " (type x)))
      (is (= (seq e) '([:b 2] [:c 3] [:a 1])))
      )))

(deftest ^:api map-change
  (testing "clojure map change api: assoc, dissoc, etc"
    ;; our assoc, unlike clojure's assoc, updates in place.  clojure's
    ;; assoc returns a new datum (immutability)
    ;; we could follow clojure and spawn a new entity instead of
    ;; updating the original, but I'm not sure what the benefit would
    ;; be; we make Entity look like a map to get conceptual
    ;; integration, not in order to actually use it just like a
    ;; clojure map.
    (log/trace "test: clojure map api: assoc")
    (let [e1 (ds/emap!! [:Foo/bar] {:a 1 :b 2})]
      (log/trace "(seq e1) " (seq e1))
      (let [e2 (assoc e1 :c "Hi!")]
        (log/trace "(seq e1) " (seq e1))
        (log/trace "(seq e2) " (seq e2))
        (is (ds/key= e1 e1))
        (let [e3 (dissoc! e2 :a)]
          (log/trace "(seq e1) " (seq e1))
          (log/trace "(seq e2) " (seq e2))
          (log/trace "(seq e3) " (seq e3))))
      ;; now (keys e1) = :a, :c
      (let [e4 (conj! e1 {:x 9})]
        (log/trace "(conj! e1 {:x 9}) " (seq e4)))
      ;; (let [e5 (merge e1 {:foo "bar"})]
      ;;   (log/trace "(merge e1 {:foo 'bar'}) " (seq e5)))
      (let [e4 (merge e1 {:y 1})]
        (log/trace "(merge e1 {:x 9}) " (seq e4)))
      (let [e5 (ds/emap!! [:Foo/baz] {:a 1})
            e6 (ds/emap!! [:Foo/bux] {:b 2})]
        (log/trace "(merge e5 e6) " (merge e5 e6)))
      ;; (let [e5 (merge e1 {:foo "bar"})]
      ;;   (log/trace "(merge e1 {:foo 'bar'}) " (seq e5)))
    )))

(deftest ^:api emap-into
  (testing "clojure map api: into"
    (log/trace "test: clojure map api: into")
    (let [em1 (ds/emap!! [:A/B] {:a 1})
          em2 (ds/emap!! [:X/Y] {:b 2})
          foo (do
                (log/trace "em1" em1)
                (log/trace "em2" em2))
          em3 (into em1 em2)
          ]
      (log/trace "em3" em3)
      (is (= em1 em3))
      (is (ds/key= em1 em3))

      (let [em4 (into em3 {:c 3})]
        (log/trace "em4" em4)
        ;; emap into mutates in place
        (is (= em3 em4)))

      ;; what if we want into to generate a new Entity?
      ;; (let [em4a (into! em3 {:c 3})]
      ;;   (log/trace "em4a" em4a)
      ;;   ;; emap into mutates in place
      ;;   (is (= em3 em4a)))

      ;; convert emap to clj persistent map
      (let [em5 (into {:x 9} em3)]
        (log/trace "em5" em5 (type em5))
        (is (map? em5))
        (should-fail (is (ds/emap? em5))))

      )))

(deftest ^:api emap-seq-into
  (testing "clojure map api: put an emap-seq into a map"
    (do ;; construct elements of kind :A
      (ds/emap!! [:A] {:a 1})
      (ds/emap!! [:A] {:b 2})
      (ds/emap!! [:A] {:c 3})
      (ds/emap!! [:A] {:d 4})
      (ds/emap!! [:A] {:d 4})           ; a dup
      ;; now do a Kind query, yielding an emap-seq
      (let [ems (ds/emaps?? [:A])
            foo (do (log/trace "ems" ems)
                    (log/trace "(type ems)" (type ems))
                    (log/trace "(class ems)" (class ems))
                    (is (seq? ems))
                    (is (= (count ems) 5))
                    )
            em (into {} ems)]
        (log/trace "em" em))

      (let [ems (ds/emaps?? [:A])
            foo (do ;; (log/trace "ems" ems)
                    (is (= (count ems) 5))
                    )
            em1 {:as (into {} ems)}
            em2 {:bs ems}
            em3 {:cs (into #{} ems)}
            ]
        (log/trace "em1" em1 (type em1))
        (log/trace "em2" em2 (type em2))
        (log/trace "em3" em3 (type em3))
        (log/trace "(:cs em3)" (:cs em3) (type (:cs em3))))
        )))

(deftest ^:api emap-seq-merge
  (testing "clojure map api: merge map with an emap-seq"
    (do ;; construct elements of kind :A
      (ds/emap!! [:A] {:a 1})
      (ds/emap!! [:A] {:b 2})
      (ds/emap!! [:A] {:c 3})
      (ds/emap!! [:A] {:d 4})
      (ds/emap!! [:A] {:d 4})           ; a dup
      ;; now do a Kind query, yielding an emap-seq
      (let [ems (ds/emaps?? [:A])
            foo (do (log/trace "ems" ems)
                    (log/trace "(type ems)" (type ems))
                    (log/trace "(class ems)" (class ems))
                    (is (seq? ems))
                    (is (= (count ems) 5))
                    )
            ;; em (merge {} ems)]
            em (into {} ems)]
        (log/trace "em" em))

      (let [ems (ds/emaps?? [:A])
            foo (do ;; (log/trace "ems" ems)
                    (is (= (count ems) 5))
                    )
            ;;em1 {:as (merge {} ems)}
            em2 {:bs ems}
            em3 {:cs (merge (into #{} ems) {:x 9})}
            ]
        ;;(log/trace "em1" em1 (type em1))
        (log/trace "em2" em2 (type em2))
        (log/trace "em3" em3 (type em3))
        (log/trace "(:cs em3)" (:cs em3) (type (:cs em3))))
        )))

(deftest ^:api emap-merge
  (testing "clojure map api: mergge"
    (log/trace "test: clojure map api: merge")
    (let [em1 (ds/emap!! [:A/B] {:a 1})
          em2 (ds/emap!! [:X/Y] {:b 2})
          foo (do
                (log/trace "em1" em1)
                (log/trace "em2" em2))
          em3 (merge em1 {:foo "bar"} em2)
          ]
      (log/trace "em1" em1)
      (log/trace "em3" em3)
      (is (= em1 em3))
      (is (ds/key= em1 em3))

      (let [em4 (merge em3 {:c 3})]
        (log/trace "em4" em4)
        ;; emap into mutates in place
        (is (= em3 em4)))

      (let [em4 (merge em3 {:c #{{:d 3}}})]
        (log/trace "em4" em4)
        ;; emap into mutates in place
        (is (= em3 em4)))

      ;; what if we want into to generate a new Entity?
      ;; (let [em4a (into! em3 {:c 3})]
      ;;   (log/trace "em4a" em4a)
      ;;   ;; emap into mutates in place
      ;;   (is (= em3 em4a)))

      ;; convert emap to clj persistent map
      (let [em5 (merge {:x 9} em3)]
        (log/trace "em5" em5 (type em5))
        (is (map? em5))
        (should-fail (is (ds/emap? em5))))
      )))

(deftest ^:query kind-query
  (testing "kind query"
    (let [e1 (ds/emaps?? :Foo)]
      (log/trace e1))))

(deftest ^:api emap-ancestors
  (testing "clojure map api: ancestors"
    (log/trace "test: clojure map api: merge")
    (let [e1 (ds/emap!! [:Dept/IT] {:name "IT Dept"})
          k  (ds/key e1)
          emp1 (ds/emap!! [k :Employee] {:lname "Bits" :fname "Joey" :email "bits@foo.org"})
          emp2 (ds/emap!! [:Dept/IT :Employee] {:lname "Abend" :fname "Janey" :email "abend@foo.org"})
          emp3 (ds/emap!! [:Dept/IT :Employee] {:lname "Bates" :fname "Bill" :email "bates@foo.org"})
          employees  (try (ds/emaps?? [k :Employee]) ; ancestory query
                        (catch EntityNotFoundException e (log/trace (.getMessage e))))]
      (is (map? e1))
      (is (ds/emap? e1))
      (is (map? emp1))
      (is (ds/emap? emp1))
      (is (map? emp2))
      (is (ds/emap? emp2))
      (is (map? emp3))
      (is (ds/emap? emp3))
      (is (seq? employees))
      (is (ds/emap-seq? employees))
      (is (= (count employees) 3))
      )))

(deftest ^:api clojure-map-2
  (testing "clojure map api: entity with 2 properties"
    (let [e (ds/emap!! [:Foo/bar] {:a 1 :b 2})]
      (log/trace "test: clojure map api")
      (log/trace "entity: " (.entity e))
      (log/trace "e: " e)
      (log/trace "e type: " (type e))
      (log/trace "(e :a) " (e :a))
      (log/trace "(:a e) " (:a e))
      (log/trace "(get e :a) " (get e :a))
      ;; (log/trace "(get-in e ... see below, embedded-maps test
      (log/trace "(count e) " (count e))
      (log/trace "(seq e) " (seq e))
      (log/trace "(contains? e :a) " (contains? e :a))
      (log/trace "(contains? e :b) " (contains? e :b))
      (log/trace "(keys e) " (keys e))
      (log/trace "(vals e) " (vals e))
      (log/trace "(find e :a) " (find e :a))
      (log/trace "(find e :a) " (find e :a))
      ;; assoc, assoc-in, dissoc, merge, merge-with, select-keys, update-in, rename-keys, map-invert
      (log/trace "(assoc e :a) " (find e :a))
      (is (= (e :a) 1))
      (is (= (:a e) 1))
      (is (= (count e) 2))
      (is (= (seq e) '([:b 2] [:a 1])))
      (is (contains? e :a))
      (is (contains? e :b))
      (is (= (keys e) '(:b :a)))
      (is (= (vals e) '(2 1)))
      )))

(deftest ^:api emap-props
  (testing "Entity property types"
    (let [e1 (ds/emap!! [:Foo/bar]
                        {:int 1 ;; BigInt and BigDecimal not supported
                         :float 1.1
                         :bool true :string "I'm a string"
                         :today (java.util.Date.)
                         :email (Email. "foo@example.org")
                         :dskey (ds/key :Foo/bar)
                         :link (Link. "http://example.org")
                         ;; :embedded (EmbeddedEntity. ...
                         ;; TODO: Blob, ShortBlob, Text, GeoPt, PostalAddress, PhoneNumber, etc.
                         })]
      (log/trace "test: emap-props")
      (log/trace "e1: " e1)
      (log/trace "(e1 :notfound) " (e1 :notfound))
      (is (= (type (:int e1)) java.lang.Long))
      (is (integer? (:int e1)))
      (is (= (type (:float e1)) java.lang.Double))
      (is (float? (:float e1)))
      (is (= (type (:bool e1)) java.lang.Boolean))
      (is (= (type (:string e1)) java.lang.String))
      (is (= (type (:today e1)) java.util.Date))
      )))

(deftest ^:api emap-embedded-map-1
  (testing "using a map as a property value"
    (let [e (ds/emap!! [:Foo/bar] {:a 1, :b {:c 3, :d 4}})]
      (log/trace "test: emap-embedded 1")
      (log/trace "e: " e)
      (log/trace "(e :b): " (e :b) (type (e :b)))
      (log/trace "(seq e): " (seq e)))))


;; FIXME:  recursive embedding doesn't work yet
(deftest ^:api emap-embedded-map-2
  (testing "using a map as a property value - dbl embed"
    (let [e (ds/emap!! [:Foo/bar] {:a 1, :b {:c 3, :d {:e 4}}})]
      (log/trace "test: emap-embedded 2")
      (log/trace "e: " e)
      (log/trace "(e :b): " (e :b))
      (log/trace "(:c (:b e)): " (:c (:b e)))
      (log/trace "(seq e): " (seq e)))))

(deftest ^:api emap-embedded-vec
  (testing "using a vector as a property value"
    (let [e (ds/emap!! [:Foo/bar] {:a 1, :b [1 2 3]})]
      (log/trace "test: emap-embedded-vec")
      (log/trace "e: " e)
      (log/trace "entity: " (.entity e))
      (log/trace "(e :b): " (e :b))
      (log/trace "(e :b): " (e :b))
      (log/trace "(seq e): " (seq e)))))

(deftest ^:api emap-embedded-set
  (testing "using a set as a property value"
    (let [e (ds/emap!! [:Foo/bar] {:a 1, :b #{1 2 3}})]
      (log/trace "test: emap-embedded-set")
      (log/trace "e: " e)
      (log/trace "entity: " (.entity e))
      (log/trace "(e :b) dump: " (e :b) (type (e :b)))
      (log/trace "(seq e): " (seq e)))))

(defn seqtest
  [e indent]
  (doseq [[k v] e]
    (if (not (map? v))
      (log/trace (format "%s%s -> %s" (apply str (repeat indent " ")) k v))
      (do
        (log/trace (format "%s%s -\\" (apply str (repeat indent " ")) k))
        (seqtest v (+ indent 4))))))

(deftest ^:api emap-seq
  (testing "emaps are seqable just like clojure maps"
  (let [e (ds/emap!! [:Foo/bar] {:a 1, :b {:c 3, :d {:e 4, :f 5}}})
        indent 4]
    (seqtest e indent))))

(deftest ^:api apix
  (testing "clojure api: query"
    (let [e (ds/emap!! [:Foo/d3] {:a 1 :b 2})]
      (log/trace e)
      (log/trace (.entity e))
      (log/trace (type e))
      (log/trace "associative? " (associative? e))
      (log/trace "map? " (map? e))
      (log/trace "lookup1 " (:a e))
      )))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(deftest ^:meta emap-meta
  (testing "emap meta"
    (let [e (ds/emap!! [:Foo/d3] {:a 1 :b 2})]
      (is (ds/emap? e))
      (is (= (type e) migae.datastore.EntityMap))
      (is (= (type (:key (meta e))) com.google.appengine.api.datastore.Key))
    )))

(deftest ^:meta metadata-kind
  (testing "get entity kind"
    (let [theEntity (ds/Entities
                     ^{:kind :Employee, :name "asalieri"}
                     {:fname "Antonio", :lname "Salieri"})]
      (is (= ((meta theEntity) :kind)
             :Employee)))))

(deftest ^:meta metadata-id
  (testing "get entity id"
    (let [theEntity (ds/Entities
                        ^{:kind :Employee, :id 123}
                        {:fname "Antonio", :lname "Salieri"})]
      (is (= ((meta theEntity) :id)
             123)))))

;; (deftest ^:meta metadata-id-fail
;;   (testing "get entity id - expected failure"
;;     (let [theEntity (ds/Entities
;;                         ^{:kind :Employee, :name "asalieri"}
;;                         {:fname "Antonio", :lname "Salieri"})]
;;       (is (= ((ds/meta? theEntity) :id)
;;              "asalieri")))))

(deftest ^:meta metadata-name
  (testing "get entity name"
    (let [theEntity (ds/Entities
                        ^{:kind :Employee, :name "asalieri"}
                        {:fname "Antonio", :lname "Salieri"})]
      (is (= ((meta theEntity) :name)
             "asalieri")))))

(deftest ^:meta metadata-keynamespace
  (testing "get entity kind"
    (let [theEntity (ds/Entities
                        ^{:kind :Employee, :name "asalieri"}
                        {:fname "Antonio", :lname "Salieri"})]
      (is (nil? ((meta theEntity) :keynamespace))))))

(deftest ^:meta metadata-keystring
  (testing "get entity kind"
    (let [theEntity (ds/Entities
                        ^{:kind :Employee, :name "asalieri"}
                        {:fname "Antonio", :lname "Salieri"})]
      (is (not (nil? (:keystring (meta theEntity))))))))

;; ################################################################
;; (defprotocol PAssociative
;;   (containsKey [this k])
;;   (entryAt [this k]))

;; (extend com.google.appengine.api.datastore.Entity
;;   PAssociative
;;   {:containsKey (fn [this k] (log/trace "containsKey") (.hasProperty this (str k)))}
;;   {:entryAt (fn [this k] (log/trace "entryAt") (.getProperty this k))})

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

(deftest ^:emap emap??
  (testing "entitymap get by key")
  (let [putit (ds/emap!! [:Species/Felis_catus] {})
        getit (ds/emap?? [:Species/Felis_catus])
        getit2 (ds/emap?? :Species/Felis_catus)
        getit3 (ds/emap?? (keyword "Species" "Felis_catus"))]
    (log/trace "putit " putit)
    (log/trace "key putit " (ds/key putit))
    (log/trace "type putit " (type putit))
    (log/trace "getit " getit)
    (log/trace "getit2 " getit2)
    (log/trace "getit3 " getit3)
    (is (= (ds/key putit) (ds/key getit) (ds/key getit2) (ds/key getit3)))
    (is (= (type putit) migae.datastore.EntityMap))
    (is (= (try (ds/emap?? (keyword "Group" "foo"))
                      (catch EntityNotFoundException e EntityNotFoundException))
            EntityNotFoundException))
    ))

(deftest ^:emap emap!
  (testing "entitymap deftype"
    (binding [*print-meta* true]
      (let [em1 (ds/emap! [:Species/Felis_catus] {})
            em2 (ds/emap [:Genus/Felis :Species/Felis_catus] {})
            em3 (ds/emap [:Subfamily/Felinae :Genus/Felis :Species/Felis_catus] {})
            em4 (ds/emap [:Family/Felidae :Subfamily/Felinae :Genus/Felis :Species/Felis_catus] {})]
        (is (ds/emap? em1))
        (is (ds/emap? em2))
        (is (ds/emap? em3))
        (is (ds/emap? em4))
        ))))

(deftest ^:emap emap-props-1
  (testing "emap! with properties"
    ;; (binding [*print-meta* true]
      (let [k [:Genus/Felis :Species/Felis_catus]
            e1 (ds/emap! k {:name "Chibi" :size "small" :eyes 1})
            e2 (ds/emap! k)]
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

(deftest ^:emap emap-assoc
  (testing "emap assoc"
    (let [em1 (ds/emap!! [:Species/Felis_catus :Cat/Booger]
                        (fn [e]
                          (assoc e :sex "F")))
          em2 (ds/emap!! [:Species/Felis_catus :Cat/Booger]
                        (fn [e]
                          (assoc e :age 5)))
          ]
      (log/trace em1)
      (log/trace (ds/assoc!! em1 :weight 7))
      (log/trace (ds/assoc! (ds/emap! [:Species/Felis_catus :Cat/Booger])
                           :name "Niki" :weight 7))
      (log/trace  (ds/emap! [:Species/Felis_catus :Cat/Booger]))
      ;; (log/trace em2)
      )))

          ;; em3 (ds/emap!! [:Species/Felis_catus]
          ;;                (fn [e]
          ;;                  ;; an emap is conceptually just a map
          ;;                  (ds/into e {:name "Booger" :weight 5})))
          ;;                ]


;; NB:  the emaps! family does not (yet) create entities
;; (deftest ^:emap emaps
;;   (testing "emaps"
;;     (let [em1 (ds/emaps! :Species (>= :weight 5))
;;           ;; em2 (ds/emaps! :Species (and (>= :weight 5)
;;           ;;                              (<= :weight 7)))
;;           ]
;;       (log/trace em1)
;;       )))


;; ################################################################
;;  entity lists
(deftest ^:props props1
  (testing "properties 1"
    (let [int_ (ds/emap!! [:Foo] {:a 1})
          float_ (ds/emap!! [:Foo] {:a 1.0})
          string_ (ds/emap!! [:Foo] {:a "Hello"})
          bool_ (ds/emap!! [:Foo] {:a true})

          ;; keywords
          map_ (ds/emap!! [:Foo] {:a {:b :c}})
          vec_ (ds/emap!! [:Foo] {:a [:b :c]})
          lst_ (ds/emap!! [:Foo] {:a '(:b :c)})
          set_ (ds/emap!! [:Foo] {:a #{:b :c}})
          ;; symbols
          map2 (ds/emap!! [:Foo] {:a {'b 'c}})
          vec2 (ds/emap!! [:Foo] {:a ['b 'c]})
          lst2 (ds/emap!! [:Foo] {:a '(b c)})
          set2 (ds/emap!! [:Foo] {:a #{'b 'c}})
          ;; FIXME: make nested quote work
          ;; lst2 (ds/emap!! [:Foo] {:x '('b 'c)})

          mix1 (ds/emap!! [:Foo] {:a {:b :c}
                                  :b [1 2]
                                  :c '(foo bar)
                                  :d #{1 'x :y "z"}})

          ;; nested
          ]
      ;; (log/trace "int_" int_)
      ;; (log/trace "bool_" bool_)

      ;; (log/trace "map_" map_)
      ;; (log/trace "vec_" vec_)
      ;; (log/trace "lst_" lst_)
      (log/trace "set_" set_)
      (log/trace "set2" set2)

      (log/trace "mix1" mix1)

      ;; (is (map? (:a map_)))
      ;; (is (vector? (:a vec_)))
      ;; (is (list? (:a lst_)))
      (is (set? (:a set_)))
      )))


;; ################################################################
(deftest ^:query emaps-q
  (testing "emaps?? 1"
    (let [em1 (ds/emap!! [:Group] {:name "Acme"})
          em2 (ds/emap!! [:Group] (fn [e] (assoc e :name "Tekstra")))
          ems (ds/emaps?? :Group)]
      (log/trace "ems " ems)
      (log/trace "ems type " (type ems))
      (log/trace (format "(seq? ems) %s\n" (seq? ems)))
      [(doseq [e ems]
         (do
           (log/trace (format "e: %s" e))
           (log/trace "(meta e): " (meta e))
           (log/trace "(type e): " (type e))
           (log/trace "(.entity e): " (.entity e))
           {:id (ds/id e) :name (e :name)}))]
      )))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;  ANCESTRY - i.e. Namespacing
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(deftest ^:query ancestor-path
  (testing "emaps?? ancestor query"
    (let [e1 (ds/emap!! [:Family/Felidae :Subfamily/Felinae :Genus/Felis :Species/Felis_catus]{})
          f1 (ds/emap?? [:Family/Felidae :Subfamily/Felinae :Genus/Felis :Species/Felis_catus])
          e2 (ds/emap!! [:Species/Felis_catus]{})
          f2 (ds/emap?? [:Species/Felis_catus])]
      (log/trace "e1 " e1)
      (log/trace "f1 " f1)
      (log/trace "e2 " e2)
      (log/trace "f2 " f2))))

(deftest ^:query ancestor-query
  (testing "emaps?? ancestor query"
    (let [acme (ds/emap!! [:Group] {:name "Acme"})
          k (ds/key acme)
          foo (log/trace "key: " k)
          id (ds/id k)
          joe (ds/emap!! [k :Member/Joe] {:fname "Joe" :lname "Cool"})
          ;; joeq  (ds/emaps?? [:Group/Acme :Member/Joe])
          ;; joev  (ds/emaps?? :Member/Joe)
          jane (ds/emap!! [k :Member/Jane] {:fname "Jane" :lname "Hip"})
          frank (ds/emap!! [:Member/Frank] {:fname "Frank" :lname "Funk"})
          root (ds/emaps?? k)
          members (ds/emaps?? :Member)
          membersV (ds/emaps?? [:Member])
          members-acme (ds/emaps?? [k :Member])
          ] ; ancestor query
      ;; (log/trace "root: " root)
      ;; (log/trace "all members: " members)
      ;; (log/trace "all membersV: " membersV)
      (log/trace "acme members: " members-acme)
      (log/trace "joe " joe)
      ;; (log/trace "joeq " joeq)
      ;; (log/trace "joev " joev)
      ;; (is (=  (ds/emaps?? :Group/Acme)  (ds/emaps?? [:Group/Acme])))
      ;; (is (ds/key=  (first (ds/emaps?? [:Group/Acme :Member/Joe])) joe))
      (is (=  (count (ds/emaps?? :Member)) 3))
      (is (=  (count (ds/emaps?? [:Member])) 3))
      (is (=  (count (ds/emaps?? [k :Member])) 2))
      )))

;; ################################################################

(deftest ^:query query-entities-1
  (testing "query 1"
    (let [q (dsqry/entities)]
      (log/trace "query entities 1" q)
      )))

(deftest ^:query query-entities-2
  (testing "query 2"
    (let [q (dsqry/entities :kind :Employee)]
      (log/trace "query entities 2" q)
      )))


;; (deftest ^:query query-ancestors-1
;;   (testing "query ancestors-1"
;;     (let [k (dskey/make "foo" "bar")
;;           q (dsqry/ancestors :key k)]
;;       (log/trace "query ancestors-1:" q)
;;       )))

(deftest ^:query query-ancestors-2
  (testing "query ancestors-2"
    (let [q (dsqry/ancestors :kind "foo" :name "bar")]
      (log/trace "query ancestors-2:" q)
      )))

(deftest ^:query query-ancestors-3
  (testing "query ancestors-3"
    (let [q (dsqry/ancestors :kind "foo" :id 99)]
      (log/trace "query ancestors-3:" q)
      )))

(deftest ^:query query-ancestors-3
  (testing "query ancestors-3"
    (let [q (dsqry/ancestors :kind "foo" :id 99)]
      (log/trace "query ancestors-3:" q)
      )))

(deftest ^:query query-ancestors-4
  (testing "query ancestors-4"
    (let [q (dsqry/ancestors :kind :Person :id 99)]
      (log/trace "query ancestors-3:" q)
      )))

;; ################################################################
(deftest ^:emap entity-map-1
  (testing "entitymap deftype"
    ;;(ds/new-entitymap ...
    (let [em ^{:_kind :Employee,
               :_name "asalieri"},
          {:fname "Antonio",
           :lname "Salieri"}]
 ;      (println em)
      ;; (is (= (type em)
      ;;        migae.migae_datastore.EntityMap))
      (is (= (:fname em)
             "Antonio"))
      (is (= (:fname (merge em {:fname "Wolfie"})
             "Wolfie")))
      )))

(deftest ^:emap entity-map-save
  (testing "entitymap deftype saving"
    (let [em ^{:_kind :Employee,
               :_name "asalieri"} {:fname "Antonio",
                                   :lname "Salieri"}]
      (let [e (ds/persist em)]
        ;; (is (= (type e)
        ;;        com.google.appengine.api.datastore.Key))
        (is (= (dskey/id (:_key (meta e)))
               0))
        (is (= (dskey/name (:_key (meta e)))
               "asalieri")))
      )))

(deftest ^:emap entity-map-update
  (testing "entitymap deftype update"
    (let [em ^{:_kind :Employee,
               :_name "asalieri"},
          {:fname "Antonio",
           :lname "Salieri"}]
      (let [e (ds/persist em)
            f (ds/fetch {:_kind :Employee :_name "asalieri"})]
        (println "meta: " (meta f))
        (println f)
        (is (= (:fname f)
               "Antonio"))
        (is (= (type (:fname f))
               java.lang.String))
        (is (= (:lname f)
               "Salieri"))
      ))))

;; ################################################################
(deftest ^:entities entities-1
  (testing "entities 1"
    (let [input (ds/persist-list
                 :Person
                 [{:name "John" :sex :M :age 20}
                  {:name "Jane" :sex :F :age 30}
                  {:name "Joe" :sex :M :age 40}
                  {:name "Joan" :sex :F :age 50}
                  {:name "George" :sex :M :age 60}
                  {:name "Georgine" :sex :F :age 70}])]
          ;; q (dsqry/entities :kind :Person)
          ;; q- (log/trace "q: " q)
          ;; pq (dsqry/prepare q)
          ;; pq- (log/trace "prepared query: " pq)
          ;; r (dsqry/run pq)
          ;; r- (log/trace "run: " r)]
      (log/trace "entities 1" input)
      )))

;; ################################################################
(deftest ^:fetch entity-map-fetch-1
  (testing "entitymap deftype fetch 1"
    (let [em ^{:_kind :Employee,        ; 'Employee/asalieri1
               :_name "asalieri1"},
          {:fname "Antonio",
           :lname "Salieri 1"}]
      (let [e (ds/persist em)
            f (ds/fetch {:_kind :Employee :_name "asalieri1"})]
        (is (= (:_kind (meta f))
               :Employee))
        (is (= (:_name (meta f))
               "asalieri1"))
        (is (= (:fname f)
               "Antonio"))
        (is (= (type (:fname f))
               java.lang.String))
        (is (= (:lname f)
               "Salieri 1"))
      ))))

(deftest ^:fetch entity-map-fetch-2
  (testing "entitymap deftype fetch 2"
    (let [em ^{:_kind :Employee,
               :_id 99},
          {:fname "Antonio",
           :lname "Salieri 2"}]
      (let [e (ds/persist em)
            f (ds/fetch {:_kind :Employee :_id 99})]
        (is (= (:_kind (meta f))
               :Employee))
        (is (= (:_id (meta f))
               99))
        (is (= (:fname f)
               "Antonio"))
        (is (= (type (:fname f))
               java.lang.String))
        (is (= (:lname f)
               "Salieri 2"))
      ))))

(deftest ^:fetch entity-map-fetch-3
  (testing "entitymap deftype fetch 3"
    (let [em ^{:_kind :Employee},
          {:fname "Antonio",
           :lname "Salieri 3"}]
      (let [e (ds/persist em)
            i (:_id (meta e))
            f (ds/fetch {:_kind :Employee :_id i})]
        (is (= (:_kind (meta f))
               :Employee))
        (is (= (:_id (meta f))
               (:_id (meta e))))
        (is (= (:fname f)
               "Antonio"))
        (is (= (type (:fname f))
               java.lang.String))
        (is (= (:lname f)
               "Salieri 3"))
      ))))

;; ################################################################
(deftest ^:ancestor ancestor-1
  (testing "ancestor 1"
    ;; instead: ^{:_key ['Musician/Mozart 'Employee/asalieri4]}{:fname ...}
    (let [em ^{:_kind :Employee,
               :_name "asalieri4"
               :_parent {:_kind :Musician :_name "Mozart"}},
          {:fname "Antonio",
           :lname "Salieri 4"}]
      (let [e (ds/persist em)
            k (dse/key e)]
            ;; f (ds/fetch k)
            ;; f (ds/fetch {:_key k})]
        ;; (println "meta: " (meta f))
        ;; (log/info (format "child ent  %s" e))
        ;; (log/info (format "child meta  %s" (meta e)))
        ;; (log/info (format "child key  %s" k))
        (is (= (dskey/make {:_kind :Employee,
                            :_name "asalieri4"
                            :_parent {:_kind :Musician :_name "Mozart"}})
               k))
        (is (= (dse/key (ds/fetch k))
               k))
        (is (= (dse/key (ds/fetch {:_key k}))
               k))
        ;; (is (= (dse/key (ds/fetch {:_kind :Employee
        ;;                            :_name "asalieri4"
        ;;                            :_parent {:_kind :Musician
        ;;                                      :_name "Mozart"}}))
        ;;        k))
        (is (= (:fname e)
               "Antonio"))
        (is (= (type (:fname e))
               java.lang.String))
        (is (= (:lname e)
               "Salieri 4"))
      ))))

;; ################################################################

(deftest ^:pred pred-1a
  (ds/ptest :Person)
  (testing "pred 1a"))

(deftest ^:pred pred-1b
  (ds/ptest :_kind :Person)
  (testing "pred 1b"))

(deftest ^:pred pred-1c
  (ds/ptest :Person '(:sex = :M))
  (testing "pred 1c"))

(deftest ^:pred pred-1d
  (ds/ptest :_kind :Person '(:sex = :M))
  (testing "pred 1d"))

;; (deftest ^:pred pred-2
;;   (ds/ptest {:_kind :Person :sex :F}) ;; i.e. sex = F
;;   (testing "pred 1"))

(deftest ^:pred pred-3
;;  (ds/ptest {:_kind :Person :sex :F :a "a" :b "b" :c "c" :d "d" :e "e" :f "f"})
;;  (ds/ptest :_kind :Person '(:sex = :F && :a = "a" && :b = "b" && :c = "c"))
  (ds/ptest :_kind :Person '(:sex = :F :and :a = "a" :and :b = "b" :and :c = "c"))
  (testing "pred 1"))

;; ################################################################
(deftest ^:fetch fetch-1
  (testing "fetch 4"
    (let [em ^{:_kind :Employee,
               :_name "asalieri4"},
          {:fname "Antonio",
           :lname "Salieri 4"}]
      (let [e (ds/persist em)
            k (dse/key e)]
            ;; f (ds/fetch k)
            ;; f (ds/fetch {:_key k})]
        (is (= (dse/key (ds/fetch k))
               k))
        (is (= (dse/key (ds/fetch {:_key k}))
               k))
        (is (= (dse/key (ds/fetch {:_kind :Employee :_name "asalieri4"}))
               k))
        (is (= (:fname e)
               "Antonio"))
        (is (= (type (:fname e))
               java.lang.String))
        (is (= (:lname e)
               "Salieri 4"))
      ))))

(deftest ^:fetch fetch-keys-only
  (testing "fetch keys only"
    (let [em1 ^{:_kind :Employee,
               :_name "asalieri5"},
          {:fname "Antonio",
           :lname "Salieri 5"}
          e1 (ds/persist em1)
          em2 ^{:_kind :Employee,
               :_name "asalieri6"},
          {:fname "Antonio",
           :lname "Salieri 6"}
          e2 (ds/persist em2)
          f2 (ds/fetch {:_kind :Employee})]
      (doseq [e f2]
        (log/debug "ent: " e)
        (log/debug "key " (.getKey e))
       (log/debug "name " (.getName (.getKey e)))))))
;      (log/debug "keysonly fetch: " f2))))
      ;; (log/trace "fetched: " (dsqry/count f2))
      ;; (log/trace "iter: " (iterator-seq (dsqry/run f2))))))

(deftest ^:fetch fetch-5
  (testing "fetch 5"
    (let [input (ds/persist-list
                 :Person                ; instead:  'Person
                 [{:name "John" :sex :M :age 20}
                  {:name "Jane" :sex :F :age 30}
                  {:name "Joe" :sex :M :age 40}
                  {:name "Joan" :sex :F :age 50}
                  {:name "George" :sex :M :age 60}
                  {:name "Georgine" :sex :F :age 70}])
          es (ds/fetch {:_kind :Person :sex :M})]
      ;; (log/trace es)
;          es (ds/count :kind :Person :sex :M)]
      ;; (is (= (dse/count es) 3)) ;; count set of entities
       )))

(deftest ^:fetch fetch-6
  ;; (ds/fetch {:_kind :Person :age < 50})
  (testing "fetch 6"))

(deftest ^:fetch fetch-7
  ;; (ds/fetch :_kind :Person :sex :M :and :age < 50)
  (testing "fetch 7"))

(deftest ^:fetch fetch-8
  ;; (ds/fetch :_kind :Person :age < 30 :or :age > 50)
  (testing "fetch 8"))

(deftest ^:fetch fetch-9
  ;; (ds/fetch :_kind :Person :age >= 18 :and :age <= 64)
  (testing "fetch 9"))

(deftest ^:fetch fetch-10
  ;; (ds/fetch :_kind :Person :sex :F :and (:age < 18 :or :age > 64))
  (testing "fetch 10"))

(deftest ^:fetch fetch-11
  ;; (ds/fetch :_kind :Person :sex :F :and (:age < 18 :or :age > 64) :and :race :White)
  (testing "fetch 11"))


;; ################################################################
(deftest ^:keysym keysym1
  (testing "keyword - key literals: name"
    (log/trace (ds/key :Employee/asalieri))
    (is (ds/key? (ds/key :Employee/asalieri)) true)
    (log/trace (ds/key :Employee/d15))
    (is (ds/key? (ds/key :Employee/d15)) true)
    (log/trace (ds/key :Employee/x0A))
    (is (ds/key? (ds/key :Employee/x0A)) true))
    (is (= (ds/key :Employee/d15)
           (ds/key :Employee/x0F)))
    )

;; (deftest ^:keysym keysym2
;;   (testing "keymap literals: name"
;;     (is (ds/key? (ds/key :Employee :asalieri)) true)
;;     (is (ds/key? (ds/key "Employee" "asalieri")) true)))

    ;; (is (= (type (dskey/make 'Employee/x0F)) com.google.appengine.api.datastore.Key)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(deftest ^:keychain keychain-emap
  (testing "keychains and entities"
    (let [;; create and put three entities with same key-name :X/Y,
          ;; but distinct key-namespaces; they all have the same Kind (:X)
          ;; and Identifier (Y)
          e1 (ds/emap!! [:X/Y]{})
          e2 (ds/emap!! [:A/B :X/Y]{})
          e3 (ds/emap!! [:A/B :C/D :X/Y]{})
          ;; now fetch from datastore
          f1 (ds/emap?? [:X/Y])
          f2 (ds/emap?? [:A/B :X/Y])
          f3 (ds/emap?? [:A/B :C/D :X/Y])]
      ;; (log/trace "e1 " e1)
      ;; (log/trace "kind e1 " (ds/kind e1))
      ;; (log/trace "name e1 " (ds/name e1))
      ;; (log/trace "f1 " f1)
      ;; (log/trace "e2 " e2)
      ;; (log/trace "f2 " f2)
      ;; (log/trace "e3 " e3)
      ;; (log/trace "f3 " f3)
      ;; (log/trace "kind e3 " (ds/kind e3))
      ;; (log/trace "name e3 " (ds/name e3))
      (is (= (ds/kind e1) (ds/kind e2) (ds/kind e3)))
      (is (= (ds/name e1) (ds/name e2) (ds/name e3)))
      (should-fail (is (ds/key= e1 e2)))
      )))

(deftest ^:keychain keychain1a
  (testing "keychain sym key literals 1a"
    (log/trace (ds/key [:Genus/Felis :Species/Felis_catus]))
    (ds/key? (ds/key [:Genus/Felis :Species/Felis_catus]))))

(deftest ^:keychain keychain1b
  (testing "keychain sym key literals 1b"
    (log/trace (ds/key [:Subfamily/Felinae :Genus/Felis :Species/Felis_catus]))
    (ds/key? (ds/key [:Subfamily/Felinae :Genus/Felis :Species/Felis_catus]))))

(deftest ^:keychain keychain1c
  (testing "keychain sym key literals 1c"
    (log/trace (ds/key [:Family/Felidae :Subfamily/Felinae :Genus/Felis :Species/Felis_catus]))
    (ds/key? (ds/key [:Family/Felidae :Subfamily/Felinae :Genus/Felis :Species/Felis_catus]))))


(deftest ^:keychain keychain20
  (testing "keychain string key literals 2"
    (ds/key? (ds/key [:Subfamily :Felinae :Genus :Felis]))
    (ds/key? (ds/key ["Subfamily" "Felinae" "Genus" "Felis"]))))

;; TODO: support string syntax:
;; (deftest ^:keychain keychain30
;;   (testing "keychain - mixed key literals 30"
;;     (ds/key? (ds/key [:Subfamily/Felinae :Genus :Felis]))))
;;     (ds/key? (ds/key ["Subfamily" "Felinae" :Genus/Felis])))))

(deftest ^:keychain keychain3
  (testing "keychain literals 3"
    (let [chain (ds/key [:Family/Felidae :Subfamily/Felinae :Genus/Felis :Species/Felis_catus])]
      (log/trace chain))))

(deftest ^:keychain keychain20
  (testing "keychain literals 20"
    (let [e1 (ds/emap!! [:Family/Felidae :Subfamily/Felinae :Genus/Felis :Species/Felis_catus] {})
          e2 (ds/emap!! [:Family :Subfamily :Genus :Species] {})
          e3 (ds/emap!! [:Family :Subfamily :Genus :Species] {})]
      (log/trace (ds/key e1) (ds/to-edn e1))
      (log/trace (ds/to-edn e2))
      (log/trace (ds/to-edn e3))
      )))


    ;;        com.google.appengine.api.datastore.Key))
    ;; (is (= (type (dskey/make {:_kind :Employee :_id 99}))
    ;;        com.google.appengine.api.datastore.Key))
    ;; ))

;; ################################################################
(deftest ^:keys keymap3
  (testing "keymap literals 1"
    (is (= (type (dskey/make {:_kind :Employee :_name "asalieri"}))
           com.google.appengine.api.datastore.Key))
    (is (= (type (dskey/make {:_kind :Employee :_id 99}))
           com.google.appengine.api.datastore.Key))
    ))

(deftest ^:keys ekeymap1
  (testing "entity with keymap literal 2"
    (let [em ^{:_kind :Employee, :_name "asalieri"}
          {:fname "Antonio", :lname "Salieri"}]
      (let [ent (ds/persist em)]
        (is (= (dskey/id (:_key (meta ent))) 0))
        (is (= (dskey/name (:_key (meta ent))) "asalieri"))
        (is (= (dskey/name (dse/key ent)) "asalieri"))
        (is (= (dse/name em)) "asalieri")
        (is (= (dse/name ent)) "asalieri"))
      )))

(deftest ^:keys keys3
  (testing "entitymap deftype keys child"
    (let [key (dskey/make {:_kind :Genus :_name "Felis"})
          child (dskey/child key {:_kind :Genus :_name "Felis"})]
      (is (= (type child)
             com.google.appengine.api.datastore.Key))
      (is (= (dskey/parent child)
             key))
      (should-fail (is (= (dskey/name child)
             "felis catus")))
      (println child)
      )))

;; (deftest ^:one one-off
;;   (testing "entitymap deftype keys parent"
;;     (let [parent (dskey/make {:_kind :Genus :_name "Felis"})]
;;       (log/trace (dskey/kind parent))
;;       (is (= ((dskey/kind parent) :Genus))))))

(deftest ^:keys keys4
  (testing "entitymap deftype keys parent"
    (let [parent (dskey/make {:_kind :Genus :_name "Felis"})
          foo (log/trace "parent " parent)
          child  (dskey/make {:_parent parent :_kind :Species :_name "felis catus"})
          bar (log/trace "child " child)
          ]
      (is (= (type child)
             com.google.appengine.api.datastore.Key))
      (is (= (dskey/parent child)
             parent))
      (is (= (dskey/name child)
             "felis catus"))
      )))

(deftest ^:keys keys5
  (testing "entitymap deftype keys parent"
    (let [parent (dskey/make {:_kind :Genus :_name "Felis"})
          child  (dskey/make {:_parent {:_kind :Genus :_name "Felis"}
                              :_kind :Species :_name "felis catus"})]
      (is (= (type child)
             com.google.appengine.api.datastore.Key))
      (is (= (dskey/parent child)
             parent))
      (is (= (dskey/name child)
             "felis catus"))
      )))

;; ################################################################
;;  entity lists
(deftest ^:elist entity-list-1
  (testing "entity-list 1"
    (let [input (ds/persist-list
                 :Person
                 [{:name "John" :sex :M :age 20}])
          q (dsqry/entities :kind :Person)
          pq (dsqry/prepare q)
          r (dsqry/run pq)]
      (is (= (dsqry/count pq) 1))
      (doseq [entity r]
        (is (= (dse/prop entity "name") "John"))
        (is (= (dse/prop entity "sex") "M"))
        (is (= (dse/prop entity "age") 20)))
      )))

(deftest ^:elist entity-list-6
  (testing "entity-list 6"
    (let [input (ds/persist-list
                 :Person
                 [{:name "John" :sex :M :age 20}
                  {:name "Jane" :sex :F :age 30}
                  {:name "Joe" :sex :M :age 40}
                  {:name "Joan" :sex :F :age 50}
                  {:name "George" :sex :M :age 60}
                  {:name "Georgine" :sex :F :age 70}])
          q (dsqry/entities :kind :Person)
          ;; q- (log/trace "q: " q)
          pq (dsqry/prepare q)
          ;; pq- (log/trace "prepared query: " pq)
          r (dsqry/run pq)]
          ;; r- (log/trace "run: " r)]
      (is (= (dsqry/count pq) 6))
      (doseq [entity r]
        (log/trace "entity " (dse/key entity)))
      )))

;; ################################################################
;;  infix

(deftest ^:infix infix-1
  (testing "infix 1"
    (log/trace (infix/$= :sex = :M))
    (log/trace (infix/$= :age >= 50))
    (log/trace (infix/$= :age >= 18 && :age <= 65))
    ;; (is (= (infix/$= m = m)
    ;;        true))
    ;; (is (= (infix/$= :age < 50)
    ;;        true))
    ;; (is (= (infix/$= m > n)
    ;;        false))
    ))
