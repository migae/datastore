(ns test.axioms-struct
  "entity-map axioms for structural operations:  conj, into, assoc, etc."
  ;; (:refer-clojure :exclude [print-str println-str])
;;  (:refer-clojure :exclude [name hash])
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
            ;; [migae.datastore.signature.entity-map :as sig]
            [migae.datastore :as em :refer :all]
            [clojure.tools.logging :as log :only [trace debug info]]))
;            [ring-zombie.core :as zombie]))

(defmacro should-fail [body]
  `(let [report-type# (atom nil)]
     (binding [clojure.test/report #(reset! report-type# (:type %))]
       ~body)
     (testing "should fail"
       (is (= @report-type# :fail )))))

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
        ;; (em/datastore)
        (test-fn)
        (.tearDown helper))))
        ;; (ApiProxy/setEnvironmentForCurrentThread environment)
        ;; (ApiProxy/setDelegate delegate))))

;(use-fixtures :once (fn [test-fn] (em/get-datastore-service) (test-fn)))
(use-fixtures :each ds-fixture)

;; (deftest ^:init ds-init
;;   (testing "DS init"
;;     (is (= com.google.appengine.api.datastore.DatastoreServiceImpl
;;            (class (em/datastore))))
;;     (is (= com.google.appengine.api.datastore.DatastoreServiceImpl
;;            (class @em/*datastore-service*)))))

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
    (let [em1 (em/entity-map! [:A/B] {:a/b 1})
          em2 (em/entity-map! [:C/D] {:c/d 2 :e/f 3})]
      ;; (log/debug "construction done")
      ;; (log/debug "em1: " (em/dump-str em1) (type em1))
      ;; (log/debug "em2: " (em/dump-str em2) (type em2))
      (let [em3 (into em1 em2)]
        ;; (log/debug "em3:" (em/dump-str em3))
        ;; (log/debug "em3:" em3)
        ;; (log/debug "seq em3:" (seq em3))
        ;; (log/debug "type em3:" (type em3))
        (is (map? em3))
        (is (em/entity-map? em3))
        ;; (is (= (type em3) migae.datastore.PersistentEntityMap))
        ;; (is (= (class em3) migae.datastore.PersistentEntityMap))
        (is (not= em1 em2 em3))
        (is (not= em3 em2)))
      ;; (let [em3 (into em1 {:a/b 2})]
      ;;   (log/debug "em1" (em/dump em1))
      ;;   (is (map? em3))
      ;;   (is (em/entity-map? em3))
      ;;   (is (= (type em3) migae.datastore.PersistentEntityMap))
      ;;   (is (= (class em3) migae.datastore.PersistentEntityMap))
      ;;   (is (not= em3 em1))
      ;;   (is (not= em3 em2)))
      )))

(deftest ^:into entity-map-into-axiom2
  (testing "entity-map into axiom 2: from key is discarded (only maps affected by into)")
    (let [em1 (em/entity-map [:A/B] {:a 1})
          em2 (em/entity-map [:X/Y] {:b 2})]
      (let [em3 (into em1 em2)]
        (log/debug "em3" (em/dump em1))
        ;; (println "em3, em1, em2: " (em/keychain em3) (em/keychain em1) (em/keychain em2))
        (is (em/keys=? em3 em1))
        (is (not (em/keys=? em3 em2))))
      ))

(deftest ^:into entity-map-into-axiom3
  (testing "entity-map into axiom 3: from fields replace to fields")
    (let [em1 (em/entity-map [:A/B] {:a 1})
          em2 (em/entity-map [:X/Y] {:a 2})]
      ;; (log/debug "em1" (em/dump em1))
      ;; (log/debug "em2" (em/dump em2))
      (let [em3 (into em1 em2)]
        ;; (log/debug "(into em1 em2) => " (em/dump em3))
        (is (em/keys=? em3 em1))
        (is (not (em/keys=? em3 em2)))
        (is (= (:a em3) (:a em2)))
        (is (not= (:a em3) (:a em1))))
      ))

(deftest ^:into entity-map-into-axiom4
  (testing "entity-map into axiom 4: from fields augment to fields")
    (let [em1 (em/entity-map [:A/B] {:a 1})
          em2 (em/entity-map [:X/Y] {:b 2})]
      ;; (log/debug "em1" (em/dump em1))
      ;; (log/debug "em2" (em/dump em2))
      (let [em3 (into em1 em2)]
        ;; (log/debug "em3" (em/dump em3))
        (is (= (:a em3) (:a em1)))
        (is (= (:b em3) (:b em2))))
      ))

(deftest ^:into entity-map-into-axiom5
  (testing "entity-map into axiom 5: from obj may be plain clojure map")
    (let [em1 (em/entity-map [:A/B] {:a 1})]
      (let [em3 (into em1 {:b 2})]
        ;; (log/debug "em3" (em/dump em3))
        (is (= (:a em3) (:a em1)))
        (is (= (:b em3) 2)))
      ))

#_(deftest ^:intoX entity-map-into-cljmap
  (testing "clojure map api: into"
    ;; (log/debug "test: clojure map api: into")
    (let [em1 (em/entity-map! [:A/B] {:a/b 1})
          em2 (em/entity-map! [:A/B :C/D] {:c/d 1})
          em3 (em/entity-map! [:A/B :C/D :E/F] {:e/f 1})]
      ;; (log/debug "em1:" em1 (type em1))
      ;; (log/debug "em2:" em2 (type em2))
      ;; (log/debug "em3:" em3 (type em3))
      (let [em11 (with-meta (into {:x :y} em1) (meta em1))
            ;; em22 (em/into {} em2)
            ;; em33 (em/into {} em3)
            ]
        ;; (log/debug "em11:" (meta em11) em11 (type em11))
      ;;   (log/debug "em22:" em22 (type em22))
      ;;   (log/debug "em33:" em33 (type em33))
      ;;   (is (= em33 em3))
      ;;   (should-fail (is (= em1 em2))))
      ;; (let [em11 (em/into {:em1/a 1} em1)
      ;;       em22 (em/into {:em2/a 1} em2)
      ;;       em33 (em/into {:em3/a 1} em3)
      ;;       ]
      ;;   (log/debug "em11:" em11 (type em11))
      ;;   (log/debug "em22:" em22 (type em22))
      ;;   (log/debug "em33:" em33 (type em33))
      ;;   (should-fail (is (= em1 em2)))
        ))))


;; (deftest ^:intoX entity-map-seq-into
;;   (testing "clojure map api: put an entity-map-seq into a map"
;;     (log/debug "store-map " (.content store-map))
;;     (let [em1 (em/entity-map! [:A] {:a 1})
;;           em2 (em/entity-map! [:A] {:b 2})
;;           em3 (em/entity-map! [:A] {:c 3})
;;           em4 (em/entity-map! [:A] {:d 4})
;;           em5 (em/entity-map! [:A] {:d 4})]
;; ;;      (log/debug "em1: " em1 (type em1))
;;       ;; now do a Kind query, yielding an entity-map-seq
;;       ;; (let [ems (em/entity-map* [:A])
;;       ;;       foo (do (log/debug "ems" ems)
;;       ;;               (log/debug "(type ems)" (type ems))
;;       ;;               (log/debug "(class ems)" (class ems))
;;       ;;               (is (seq? ems))
;;       ;;               (is (= (count ems) 5))
;;       ;;               )
;;       ;;       em (into {} ems)]
;;       ;;   (log/debug "em" em))
;;       (let [ems (em/entity-map* [:A])
;;             foo (log/debug "NEXT" (count ems))
;;             ;; foo (do ;; (log/debug "ems" ems)
;;             ;;         (is (= (count ems) 5))
;;             ;;         )
;;             em1 {:as (into {} ems)}
;;             foo (log/debug "em1" (count em1))
;;             em2 {:bs ems}
;;             em3 {:cs (into #{} ems)}
;;             ]
;;         (log/debug "em1" em1 (type em1))
;;         (log/debug "em2" em2 (type em2))
;;         (log/debug "em3" em3 (type em3))
;;         (log/debug "(:cs em3)" (:cs em3) (type (:cs em3))))
;;         )))

(deftest ^:meta meta-axiom-1
  (testing "entity-maps support metadata")
    (let [;;em1 (with-meta (em/entity-map [:A/B] {:a/b 1})
            ;;    {:foo "bar"})
          em2 ^{:x 999}(em/entity-map [:C/D] {:a 1})]
;;      (log/debug "em1:" (em/dump em1))
      ;; (log/debug "seq em2:" (seq em2))
      ;; (log/debug "em2 type: " em2 (type em2))
      ;; (log/debug (str "em2: " (em/dump em2)))
      (let [em3 ^{:foo "buz"} em2]
        ;; (log/debug "em3:" em3)
        ;; (log/debug "seq em3:" (seq em3))
        ;; (log/debug "meta em3:" (meta em3))
        )
      ))

(deftest ^:meta interfaces
  (testing "supported interfaces")
    (let [em1 (em/entity-map [:A/B] {:a/b 1})]
      (is (satisfies? Entity-Map em1))
      (is (instance? clojure.lang.Associative em1))
      (is (instance? clojure.lang.Counted em1))
      (is (instance? clojure.lang.Seqable em1))
      ;; (is (instance? clojure.lang.Indexed em1))
      (is (instance? clojure.lang.IFn em1))
      (is (instance? clojure.lang.ILookup em1))
      (is (instance? clojure.lang.IMeta em1))
      (is (instance? clojure.lang.IObj em1))
      (is (instance? clojure.lang.IPersistentCollection em1))
      (is (instance? clojure.lang.IPersistentMap em1))
      ;; (is (instance? clojure.lang.IReduce em1))
      ;; (is (instance? clojure.lang.IReference em1))
      ;; (is (satisfies? clojure.lang.ITransientCollection em1))
      (is (instance? java.lang.Iterable em1))
      ))

;;(run-tests)
