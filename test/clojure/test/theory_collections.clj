(ns test.theory-collections
  "theory of entity-map collections"
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
            ;; [migae.infix :as infix]
            [migae.datastore :as ds]
            ;; [migae.datastore.api :as ds]
            ;; [migae.datastore.service :as dss]
            ;; [migae.datastore.entity :as dse]
            ;; [migae.datastore.query  :as dsqry]
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
;;        (datastore)
        (test-fn)
        (.tearDown helper))))
        ;; (ApiProxy/setEnvironmentForCurrentThread environment)
        ;; (ApiProxy/setDelegate delegate))))

;(use-fixtures :once (fn [test-fn] (ds/get-datastore-service) (test-fn)))
(use-fixtures :each ds-fixture)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;   collections api axioms

(deftest ^:coll coll-axiom1
  (testing "coll axiom 1: an entity-map is countable"
    (let [e1 (ds/entity-map [:A/B] {:a 1 :b 2})]
      (is (= (count e1) 2))
      )))

(deftest ^:coll empty-axiom1
  (testing "empty axiom 1: empty works"
    (let [e1 (ds/entity-map [:A/B] {:a 1 :b 2})
          e2 (empty e1)]
      (is (ds/emap? e2))
      (is (empty? e2))
      (is (ds/key= e1 e2)) ; an entity-map must have a key
      )))

(deftest ^:coll empty-axiom2
  (testing "empty axiom 2: not-empty works on non-empty emaps"
    (let [e1 (ds/entity-map [:A/B] {:a 1 :b 2})
          e2 (not-empty e1)]
      (is (ds/emap? e2))
      (is (not (empty? e2)))
      (is (ds/key= e1 e2))
      (is (= e1 e2))
      )))

(deftest ^:coll empty-axiom3
  (testing "empty axiom 3: not-empty works on empty emaps"
    (let [e1 (ds/entity-map [:A/B] {})]
      (is (nil? (not-empty e1)))
      (is (ds/emap? e1)) ;; FIXME:  entity-map?
      )))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;   merge theorems
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(deftest ^:merge merge-print
  (testing "merge print")
  (let [em1 (ds/entity-map [:A/B] {:a 1})
        em2 (ds/entity-map [:A/B] {:b 2})
        em3 (ds/entity-map [:A/B] {:a 2})
        em4 (ds/entity-map [:X/Y] {:x 9})]
    (log/trace "em1: " (ds/print-str em1))
    (log/trace "em2: " (ds/print-str em2))
    (log/trace "em3: " (ds/print-str em3))
    (log/trace "merge em1 em2: " (ds/print-str (merge em1 em2)))
    (log/trace "merge em1 em3: " (ds/print-str (merge em1 em3)))
    (log/trace "merge em1 em4: " (ds/print-str (merge em1 em4)))
    (log/trace "merge em1 {:x 9}: " (ds/print-str (merge em1 {:x 9})))
    (log/trace "merge {}  em1: " (ds/print-str (merge {} em1)))
    ))

(deftest ^:merge merge-1
  (testing "merge 1: merge m1 m2 adds m2 content to m1 content"
    (is (= (ds/keychain (merge (ds/entity-map [:A/B] {:a 1})
                               (ds/entity-map [:A/B] {:b 2})))
           [:A/B]))
    (is (= (ds/print-str (merge (ds/entity-map [:A/B] {:a 1})
                                (ds/entity-map [:A/B] {:b 2})))
           (ds/print-str (ds/entity-map [:A/B] {:a 1 :b 2}))))
    ))

(deftest ^:merge merge-2
  (testing "merge 2: from fld content overrides to fld content"
    (is (= (ds/print-str (merge (ds/entity-map [:A/B] {:a 1})
                                (ds/entity-map [:A/B] {:a 2})))
           (ds/print-str (ds/entity-map [:A/B] {:a 2}))))
    (is (= (keys (merge (ds/entity-map [:A/B] {:a 1})
                        (ds/entity-map [:A/B] {:a 2})))
           '(:a)))
      ))

(deftest ^:merge merge-3
  (testing "merge 2: emaps"
    (is (= (ds/keychain (merge (ds/entity-map [:A/B] {:a 1})
                               (ds/entity-map [:X/Y] {:b 1})))
           [:A/B])
      )))

;; (deftest ^:merge emap-merge-4
;;   (testing "merge map with an emap-seq"
;;     (do ;; construct elements of kind :A
;;       (ds/entity-map [:A] {:a 1})
;;       (ds/entity-map [:A] {:b 2})
;;       (ds/entity-map [:A] {:c 3})
;;       (ds/entity-map [:A] {:d 4})
;;       (ds/entity-map [:A] {:d 4})           ; a dup
;;       ;; now do a Kind query, yielding an emap-seq
;;       (let [ems (ds/emaps?? [:A])
;;             foo (do (log/trace "ems" ems)
;;                     (log/trace "(type ems)" (type ems))
;;                     (log/trace "(class ems)" (class ems))
;;                     (is (seq? ems))
;;                     (is (= (count ems) 5))
;;                     )
;;             ;; em (merge {} ems)]
;;             em (into {} ems)]
;;         (log/trace "em" em)
;;         (doseq [em ems]
;;           (log/trace "em" (ds/print em))))

;;       (let [ems (ds/emaps?? [:A])
;;             foo (do ;; (log/trace "ems" ems)
;;                     (is (= (count ems) 5))
;;                     )
;;             ;;em1 {:as (merge {} ems)}
;;             em2 {:bs ems}
;;             em3 {:cs (merge (into #{} ems) {:x 9})}
;;             ]
;;         ;;(log/trace "em1" em1 (type em1))
;;         (log/trace "em2" em2 (type em2))
;;         (log/trace "em3" em3 (type em3))
;;         (log/trace "(:cs em3)" (:cs em3) (type (:cs em3))))
;;         )))

(deftest ^:merge merge-cljmap-emap
  (testing "clojure map api: merge entity-map to clj-map"
    (log/trace "test: clojure map api: merge-cljmap-emap")
    (let [em1 (ds/entity-map [:A/B] {:a 1 :b 2})]
      ;; (log/trace "em1" em1)
      (let [cljmap (merge {} em1)]
        (log/trace "em1" em1 (class em1)))
        ;; (log/trace "cljmap" cljmap (class cljmap))
        ;; (is (map? em1))
        ;; (is (ds/emap? em1))
        ;; (is (map? cljmap))
        ;; (should-fail (is (ds/emap? cljmap)))

      (let [em2 (merge {:x 9} em1)]
        (log/trace "em1" (ds/print em1))
        (log/trace "em2" em2 (type em2))
        )
      )))

(deftest ^:merge merge-emap-cljmap
  (testing "clojure map api: merge entity-map to clj-map"
    (log/trace "test: clojure map api: merge-cljmap-emap")
    (let [em1 (ds/entity-map [:A/B] {:a 1})
          em2 (ds/entity-map [:X/Y] {:b 2 :c 7})
          foo (do
                (log/trace "em1" (ds/print em1))
                (log/trace "em2" (ds/print em2)))
          ;; FIXME:  merge is broken
          em3 (merge em2 {:foo "bar"}  em1)
          ]
      (log/trace "em1" (ds/print em1))
      (log/trace "em3" (ds/keychain em3) (ds/print em3))
      (log/trace "em3 type:" (type em3))
      (is (= em1 em3))
;;      (is (ds/key= em1 em3))

      (let [em4 (merge em3 {:d 27})]
        (log/trace "em4" em4)
        (is (= em3 em4)))

      (let [em5 (merge em3 {:c #{{:d 3}}})]
        (log/trace "em5" em5)
        )

      ;; what if we want into to generate a new Entity?
      ;; (let [em4a (into! em3 {:c 3})]
      ;;   (log/trace "em4a" em4a)
      ;;   ;; entity-map into mutates in place
      ;;   (is (= em3 em4a)))
      )))

