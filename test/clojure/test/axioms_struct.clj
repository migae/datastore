(ns test.axioms-struct
  "entity-map axioms for structural operations:  conj, into, assoc, etc."
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
            [migae.datastore :as ds]
            ;; [migae.datastore.api :as ds]
            ;; [migae.datastore.service :as dss]
            ;; [migae.datastore.entity :as dse]
            ;; [migae.datastore.query  :as dsqry]
            ;; [migae.datastore.key    :as dskey]
            [clojure.tools.logging :as log :only [trace debug info]]))
;            [ring-zombie.core :as zombie]))

(defmacro should-fail [body]
  `(let [report-type# (atom nil)]
     (binding [clojure.test/report #(reset! report-type# (:type %))]
       ~body)
     (testing "should fail"
       (is (= @report-type# :fail )))))

(defn dump
  [msg datum & data]
  (binding [*print-meta* true]
    (log/trace msg (pr-str datum) (pr-str data))))

;  (:require [migae.migae-datastore.PersistentEntityMap])
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
        ;; (ds/datastore)
        (test-fn)
        (.tearDown helper))))
        ;; (ApiProxy/setEnvironmentForCurrentThread environment)
        ;; (ApiProxy/setDelegate delegate))))

;(use-fixtures :once (fn [test-fn] (ds/get-datastore-service) (test-fn)))
(use-fixtures :each ds-fixture)

;; (deftest ^:init ds-init
;;   (testing "DS init"
;;     (is (= com.google.appengine.api.datastore.DatastoreServiceImpl
;;            (class (ds/datastore))))
;;     (is (= com.google.appengine.api.datastore.DatastoreServiceImpl
;;            (class @ds/*datastore-service*)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;   into axioms
;;
;; into means augment and replace
;; for consistency entity-map into should replace :migae/key
;; augment:       (into {a: 1} {:b 2}) => {:a 1 :b 2}
;; augment:       (into ^{:migae/key [:A/B]}{a: 1} ^{:migae/key [:X/Y]}{:b 2})
;;                   => ^{:migae/key [:X/Y]}{:a 1 :b 2}
;; replacement:  (into {a: 1} {:a 2}) => {:a 2}

(deftest ^:into into-axiom-1
  (testing "entity-map into axiom 1: into produces new entity-map"
    (let [em1 (ds/entity-map [:A/B] {:a/b 1})
          em2 (ds/entity-map [:C/D] {:c/d 2 :e/f 3})]
      (let [em3 (into em1 em2)]
        (log/trace "em1:" (ds/print em1))
        (log/trace "em2:" (ds/print em2))
        (log/trace "em3:" (ds/print em3))
        (log/trace "em3:" em3)
        (log/trace "seq em3:" (seq em3))
        (log/trace "type em3:" (type em3))
        (is (map? em3))
        (is (ds/emap? em3))
        (is (= (type em3) migae.datastore.PersistentEntityMap))
        (is (= (class em3) migae.datastore.PersistentEntityMap))
        (is (not= em1 em2 em3))
        (is (not= em3 em2)))
      ;; (let [em3 (into em1 {:a/b 2})]
      ;;   (log/trace "em1" (ds/print em1))
      ;;   (is (map? em3))
      ;;   (is (ds/emap? em3))
      ;;   (is (= (type em3) migae.datastore.PersistentEntityMap))
      ;;   (is (= (class em3) migae.datastore.PersistentEntityMap))
      ;;   (is (not= em3 em1))
      ;;   (is (not= em3 em2)))
      )))

(deftest ^:into emap-into-axiom2
  (testing "entity-map into axiom 2: from key replaces to key")
    (let [em1 (ds/entity-map [:A/B] {:a 1})
          em2 (ds/entity-map [:X/Y] {:b 2})]
      (let [em3 (into em1 em2)]
        (is (ds/key=? em3 em2))
        (is (not (ds/key=? em3 em1))))
      ))

(deftest ^:into emap-into-axiom3
  (testing "entity-map into axiom 3: from fields replace to fields")
    (let [em1 (ds/entity-map [:A/B] {:a 1})
          em2 (ds/entity-map [:X/Y] {:a 2})]
      ;; (log/trace "em1" (ds/print em1))
      ;; (log/trace "em2" (ds/print em2))
      (let [em3 (into em1 em2)]
        ;; (log/trace "(into em1 em2) => " (ds/print em3))
        (is (ds/key=? em3 em2))
        (is (not (ds/key=? em3 em1)))
        (is (= (:a em3) (:a em2)))
        (is (not= (:a em3) (:a em1))))
      ))

(deftest ^:into emap-into-axiom4
  (testing "entity-map into axiom 4: from fields augment to fields")
    (let [em1 (ds/entity-map [:A/B] {:a 1})
          em2 (ds/entity-map [:X/Y] {:b 2})]
      ;; (log/trace "em1" (ds/print em1))
      ;; (log/trace "em2" (ds/print em2))
      (let [em3 (into em1 em2)]
        ;; (log/trace "em3" (ds/print em3))
        (is (= (:a em3) (:a em1)))
        (is (= (:b em3) (:b em2))))
      ))

(deftest ^:into emap-into-axiom5
  (testing "entity-map into axiom 5: from obj may be plain clojure map")
    (let [em1 (ds/entity-map [:A/B] {:a 1})]
      (let [em3 (into em1 {:b 2})]
        ;; (log/trace "em3" (ds/print em3))
        (is (= (:a em3) (:a em1)))
        (is (= (:b em3) 2)))
      ))

(deftest ^:intoX emap-into-cljmap
  (testing "clojure map api: into"
    (log/trace "test: clojure map api: into")
    (let [em1 (ds/entity-map! [:A/B] {:a/b 1})
          em2 (ds/entity-map! [:A/B :C/D] {:c/d 1})
          em3 (ds/entity-map! [:A/B :C/D :E/F] {:e/f 1})]
      (dump "em1:" em1 (type em1))
      (dump "em2:" em2 (type em2))
      (dump "em3:" em3 (type em3))
      (let [em11 (with-meta (into {:x :y} em1) (meta em1))
            ;; em22 (ds/into {} em2)
            ;; em33 (ds/into {} em3)
            ]
        (log/trace "em11:" (meta em11) em11 (type em11))
      ;;   (dump "em22:" em22 (type em22))
      ;;   (dump "em33:" em33 (type em33))
      ;;   (is (= em33 em3))
      ;;   (should-fail (is (= em1 em2))))
      ;; (let [em11 (ds/into {:em1/a 1} em1)
      ;;       em22 (ds/into {:em2/a 1} em2)
      ;;       em33 (ds/into {:em3/a 1} em3)
      ;;       ]
      ;;   (dump "em11:" em11 (type em11))
      ;;   (dump "em22:" em22 (type em22))
      ;;   (dump "em33:" em33 (type em33))
      ;;   (should-fail (is (= em1 em2)))
        ))))


(deftest ^:intoX emap-seq-into
  (testing "clojure map api: put an emap-seq into a map"
    (do ;; construct elements of kind :A
      (ds/entity-map! [:A] {:a 1})
      (ds/entity-map! [:A] {:b 2})
      (ds/entity-map! [:A] {:c 3})
      (ds/entity-map! [:A] {:d 4})
      (ds/entity-map! [:A] {:d 4})           ; a dup
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

(deftest ^:meta meta-axiom-1
  (testing "entity-maps support metadata")
    (let [;;em1 (with-meta (ds/entity-map [:A/B] {:a/b 1})
            ;;    {:foo "bar"})
          em2 ^{:x 999}(ds/entity-map [:C/D] {:a 1})]
;;      (log/trace "em1:" (ds/print em1))
      (log/trace "seq em2:" (seq em2))
      (log/trace "em2:" (ds/print em2))
      (let [em3 ^{:foo "buz"} em2]
        (log/trace "em3:" em3)
        (log/trace "seq em3:" (seq em3))
        (log/trace "meta em3:" (meta em3)))
      ))

(deftest ^:meta interfaces
  (testing "supported interfaces")
    (let [em1 (ds/entity-map [:A/B] {:a/b 1})]
      (is (instance? clojure.lang.Associative em1))
      (is (instance? clojure.lang.Counted em1))
      (is (instance? clojure.lang.Seqable em1))
      (is (instance? clojure.lang.Indexed em1))
      (is (instance? clojure.lang.IFn em1))
      (is (instance? clojure.lang.ILookup em1))
      (is (instance? clojure.lang.IMeta em1))
      (is (instance? clojure.lang.IObj em1))
      (is (instance? clojure.lang.IPersistentCollection em1))
      (is (instance? clojure.lang.IPersistentMap em1))
      (is (instance? clojure.lang.IReduce em1))
      (is (instance? clojure.lang.IReference em1))
      (is (instance? clojure.lang.ITransientCollection em1))
      (is (instance? java.lang.Iterable em1))
      ))

