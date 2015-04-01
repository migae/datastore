(ns migae.collections
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


(deftest ^:coll emap-dissoc
  (testing "emap dissoc"
    (let [e1 (ds/emap!! [:Foo/bar] {:a 1 :b 2})]
      (log/trace "e1 " e1)
      (let [e2 (dissoc e1 :a)]
        (log/trace "e1 " e1)
        (log/trace "e2 " e2)
        (is (= e1 e2))
        (is (ds/key= e1 e2))))))

(deftest ^:coll map-change
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
        (let [e2 (dissoc e1 :a)]
          (log/trace "dissoc e2" e2)))
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

(deftest ^:into emap-into-cljmap
  (testing "clojure map api: into"
    (log/trace "test: clojure map api: into")
    (let [em1 (ds/emap!! [:A/B] {:a/b 1})
          em2 (ds/emap!! [:A/B :C/D] {:c/d 1})
          em3 (ds/emap!! [:A/B :C/D :E/F] {:e/f 1})]
      (log/trace "em1" em1 (type em1))
      (log/trace "em2" em2 (type em2))
      (log/trace "em3" em3 (type em3))
      (let [em11 (ds/into {} em1)
            em22 (ds/into {} em2)
            em33 (ds/into {} em3)
          ]
      (log/trace "em11" em11 (meta em11) (type em11))
      (log/trace "em22" em22 (meta em22) (type em22))
      (log/trace "em33" (meta em33) em33 (type em33))
      (should-fail (is (= em1 em2)))
      (is (ds/key= em1 em2))
      ))))

(deftest ^:into emap-into-emap
  (testing "clojure map api: into"
    (log/trace "test: clojure map api: into")
    (let [em1 (ds/emap!! [:A/B] {:a 1})
          em2 (ds/emap!! [:X/Y] {:b 2})
          foo (do
                (log/trace "em1" em1)
                (log/trace "em2" em2))
          em3 (into em1 em2)
          ]
      (log/trace "em3" em3 (type em3))
      (is (= em1 em3))
      (is (ds/key= em1 em3))
      )))

(deftest ^:coll emap-into
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

(deftest ^:coll emap-seq-into
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

(deftest ^:coll emap-seq-merge
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

(deftest ^:coll merge-cljmap-emap
  (testing "clojure map api: merge emap to clj-map"
    (log/trace "test: clojure map api: merge-cljmap-emap")
    (let [em1 (ds/emap!! [:A/B] {:a 1})]
      ;; (log/trace "em1" em1)
      (let [cljmap (merge {} em1)]
        ;; (log/trace "em1" em1 (class em1))
        ;; (log/trace "cljmap" cljmap (class cljmap))
        ;; (is (map? em1))
        ;; (is (ds/emap? em1))
        ;; (is (map? cljmap))
        ;; (should-fail (is (ds/emap? cljmap)))
        ))))

;;      (should-fail (is (ds/key= em1 cljmap)))))))

      ;; (let [em4 (merge em3 {:c 3})]
      ;;   (log/trace "em4" em4)
      ;;   ;; emap into mutates in place
      ;;   (is (= em3 em4)))

      ;; (let [em4 (merge em3 {:c #{{:d 3}}})]
      ;;   (log/trace "em4" em4)
      ;;   ;; emap into mutates in place
      ;;   (is (= em3 em4)))

      ;; ;; what if we want into to generate a new Entity?
      ;; ;; (let [em4a (into! em3 {:c 3})]
      ;; ;;   (log/trace "em4a" em4a)
      ;; ;;   ;; emap into mutates in place
      ;; ;;   (is (= em3 em4a)))

      ;; ;; convert emap to clj persistent map
      ;; (let [em5 (merge {:x 9} em3)]
      ;;   (log/trace "em5" em5 (type em5))
      ;;   (is (map? em5))
      ;;   (should-fail (is (ds/emap? em5))))
      ;; )))

(deftest ^:coll merge-emap-cljmap
  (testing "clojure map api: merge emap to clj-map"
    (log/trace "test: clojure map api: merge-cljmap-emap")
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

(deftest ^:assoc emap-assoc
  (testing "emap assoc"
    (let [em1 (ds/emap!! [:Species/Felis_catus :Cat/Booger]
                        (fn [e]
                          (assoc e :sex "F")))
          em2 (ds/emap!! [:Species/Felis_catus :Cat/Booger]
                        (fn [e]
                          (assoc e :age 5)))
          ]
      (log/trace "em1 " em1)
      (log/trace "assoc! em1 " (ds/assoc!! em1 :weight 7))
      (log/trace "assoc! literal " (ds/assoc! (ds/emap! [:Species/Felis_catus :Cat/Booger]{})
                                              :name "Niki" :weight 7))
      (log/trace "emap!" (ds/emap! [:Species/Felis_catus :Cat/Booger]))
      )))
