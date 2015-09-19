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
  (:require [clojure.test :refer :all]
            [migae.datastore.api :as ds]
            [clojure.tools.logging :as log :only [trace debug info]]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;  map api
;; hash-map, sorted-map, sorted-map-by
;; assoc, dissoc, select-keys, merge, merge-with, zipmap
;; get, contains?, find, keys, vals, map?
;; key, val


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
      (is (ds/entity-map? e2))
      (is (empty? e2))
      (is (ds/key=? e1 e2)) ; an entity-map must have a key
      )))

(deftest ^:coll empty-axiom2
  (testing "empty axiom 2: not-empty works on non-empty entity-maps"
    (let [e1 (ds/entity-map [:A/B] {:a 1 :b 2})
          e2 (not-empty e1)]
      (is (ds/entity-map? e2))
      (is (not (empty? e2)))
      (is (ds/key=? e1 e2))
      (is (= e1 e2))
      )))

(deftest ^:coll empty-axiom3
  (testing "empty axiom 3: not-empty works on empty entity-maps"
    (let [e1 (ds/entity-map [:A/B] {})]
      (is (nil? (not-empty e1)))
      (is (ds/entity-map? e1)) ;; FIXME:  entity-map?
      )))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;   contains theorems
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(deftest ^:contains contains-1
  (testing "entity-map contains?"
    (let [em1 (ds/entity-map [:A/B] {:a 1 :b 2})]
      (log/trace "em1" (pr-str em1))
      (is (contains? em1 :a))
      )))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;   find theorems
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(deftest ^:find find-1
  (testing "entity-map find"
    (let [em1 (ds/entity-map [:A/B] {:a 1 :b 2 :c 3 :d 4})]
      (log/trace "em1" (pr-str em1))
      (log/trace (find em1 :b))
      (is (= (find (ds/entity-map [:A/B] {:a 1 :b 2 :c 3 :d 4}) :b) [:b 2]))
      )))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;   get theorems
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(deftest ^:get get-1
  (testing "entity-map get"
    (let [em1 (ds/entity-map [:A/B] {:a 1 :b 2 :c 3 :d 4})]
      (log/trace "em1" (pr-str em1))
      (log/trace (get em1 :b))
      (is (= (get (ds/entity-map [:A/B] {:a 1 :b 2 :c 3 :d 4}) :b) 2))
      )))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;   select-key theorems
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(deftest ^:selkeys select-keys-1
  (testing "entity-map select-keys"
    (let [em1 (ds/entity-map [:A/B] {:a 1 :b 2 :c 3 :d 4})]
      (log/trace "em1" (pr-str em1))
      (log/trace (select-keys em1 [:a :c]))
      )))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;   seq theorems
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;   conj theorems
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(deftest ^:conj conj-1
  (testing "entity-map conj"
    (let [em1 (ds/entity-map [:Species/Felis_catus :Cat/Booger] {:name "Booger" :sex "F"})
          em2 (conj em1 {:weight 5})]
      (binding [*print-meta* true]
        (log/trace "em1" (pr-str em1))
        (log/trace "em1 type/class:" (type em1) (class em1))
        (log/trace "em2" (pr-str em2)))
        (log/trace "em2 type/class:" (type em2) (class em2))
      ;; (log/trace "assoc! em1 " (ds/assoc!! em1 :weight 7))
      ;; (log/trace "assoc! literal " (ds/assoc! (ds/entity-map! [:Species/Felis_catus :Cat/Booger]{})
      ;;                                         :name "Niki" :weight 7))
      ;; (log/trace "entity-map!" (ds/entity-map! [:Species/Felis_catus :Cat/Booger]))
                          )))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;   assoc theorems
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(deftest ^:assoc assoc-1
  (testing "entity-map assoc"
    (let [em1 (ds/entity-map [:Species/Felis_catus :Cat/Booger] {:sex "F"})
          em2 (assoc em1 :weight 5)]
      (binding [*print-meta* true]
        (log/trace "em1" (pr-str em1))
        (log/trace "em1 type/class:" (type em1) (class em1))
        (log/trace "em2" (pr-str em2)))
        (log/trace "em2 type/class:" (type em2) (class em2))
      ;; (log/trace "assoc! em1 " (ds/assoc!! em1 :weight 7))
      ;; (log/trace "assoc! literal " (ds/assoc! (ds/entity-map! [:Species/Felis_catus :Cat/Booger]{})
      ;;                                         :name "Niki" :weight 7))
      ;; (log/trace "entity-map!" (ds/entity-map! [:Species/Felis_catus :Cat/Booger]))
      )))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;   assoc theorems
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;   merge theorems
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(deftest ^:merge merge-print
  (testing "merge print")
  (let [em1 (ds/entity-map [:A/B] {:a 1})
        em2 (ds/entity-map [:A/B] {:b 2})
        em3 (ds/entity-map [:A/B] {:a 2})
        em4 (ds/entity-map [:X/Y] {:x 9})]
    (log/trace "em1: " (ds/dump-str em1))
    (log/trace "em2: " (ds/dump-str em2))
    (log/trace "em3: " (ds/dump-str em3))
    (log/trace "merge em1 em2: " (ds/dump-str (merge em1 em2)))
    (log/trace "merge em1 em3: " (ds/dump-str (merge em1 em3)))
    (log/trace "merge em1 em4: " (ds/dump-str (merge em1 em4)))
    (log/trace "merge em1 {:x 9}: " (ds/dump-str (merge em1 {:x 9})))
    ;; merging into IPersistentMap sheds keychain:
    (log/trace "merge {}  em1: " (ds/dump-str (merge {} em1)))
    ;; to retain keychain info:
    (log/trace "merge {}  em1: " (ds/dump-str (merge {:migae/keychain (ds/keychain em1)} em1)))
    ))

(deftest ^:merge merge-1
  (testing "merge 1: merge m1 m2 adds m2 content to m1 content"
    (is (= (ds/keychain (merge (ds/entity-map [:A/B] {:a 1})
                               (ds/entity-map [:A/B] {:b 2})))
           [:A/B]))
    (is (= (ds/dump-str (merge (ds/entity-map [:A/B] {:a 1})
                                (ds/entity-map [:A/B] {:b 2})))
           (ds/dump-str (ds/entity-map [:A/B] {:a 1 :b 2}))))
    ))

(deftest ^:merge merge-2
  (testing "merge 2: from fld content overrides to fld content"
    (is (= (ds/dump-str (merge (ds/entity-map [:A/B] {:a 1})
                                (ds/entity-map [:A/B] {:a 2})))
           (ds/dump-str (ds/entity-map [:A/B] {:a 2}))))
    (is (= (keys (merge (ds/entity-map [:A/B] {:a 1})
                        (ds/entity-map [:A/B] {:a 2})))
           '(:a)))
      ))

(deftest ^:merge merge-3
  (testing "merge 2: entity-maps"
    (is (= (ds/keychain (merge (ds/entity-map [:A/B] {:a 1})
                               (ds/entity-map [:X/Y] {:b 1})))
           [:A/B])
      )))

(deftest ^:merge merge-4
  (testing "merge 4: merging em into plain map loses key"
    (is (= (merge {} (ds/entity-map [:X/Y] {:b 1})))
        ^{:migae/keychain [:A/B]} {:b 1})
    (is (= (type (merge {} (ds/entity-map [:X/Y] {:b 1})))
           clojure.lang.PersistentArrayMap))
    (is (= (merge {:a 1} (ds/entity-map [:X/Y] {:b 1})))
        ^{:migae/keychain [:A/B]} {:a 1 :b 1})
    (is (= (type (merge {:a 1} (ds/entity-map [:X/Y] {:b 1})))
           clojure.lang.PersistentArrayMap))
    ))

;; (deftest ^:merge entity-map-merge-4
;;   (testing "merge map with an entity-map-seq"
;;     (do ;; construct elements of kind :A
;;       (ds/entity-map [:A] {:a 1})
;;       (ds/entity-map [:A] {:b 2})
;;       (ds/entity-map [:A] {:c 3})
;;       (ds/entity-map [:A] {:d 4})
;;       (ds/entity-map [:A] {:d 4})           ; a dup
;;       ;; now do a Kind query, yielding an entity-map-seq
;;       (let [ems (ds/entity-maps?? [:A])
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
;;           (log/trace "em" (ds/dump em))))

;;       (let [ems (ds/entity-maps?? [:A])
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

(deftest ^:merge merge-cljmap-entity-map
  (testing "clojure map api: merge entity-map to clj-map"
    (log/trace "test: clojure map api: merge-cljmap-entity-map")
    (let [em1 (ds/entity-map [:A/B] {:a 1 :b 2})]
      ;; (log/trace "em1" em1)
      (let [cljmap (merge {} em1)]
        (log/trace "em1" em1 (class em1)))
        ;; (log/trace "cljmap" cljmap (class cljmap))
        ;; (is (map? em1))
        ;; (is (ds/entity-map? em1))
        ;; (is (map? cljmap))
        ;; (should-fail (is (ds/entity-map? cljmap)))

      (let [em2 (merge {:x 9} em1)]
        (log/trace "em1" (ds/dump em1))
        (log/trace "em2" em2 (type em2))
        )
      )))

(deftest ^:merge merge-entity-map-cljmap
  (testing "clojure map api: merge entity-map to clj-map"
    (log/trace "test: clojure map api: merge-cljmap-entity-map")
    (let [em1 (ds/entity-map [:A/B] {:a 1})
          em2 (ds/entity-map [:X/Y] {:b 2 :c 7})
          foo (do
                (log/trace "em1" (ds/dump em1))
                (log/trace "em2" (ds/dump em2)))
          ;; FIXME:  merge is broken
          em3 (merge em2 {:foo "bar"}  em1)
          ]
      (log/trace "em1" (ds/dump em1))
      (log/trace "em3" (ds/keychain em3) (ds/dump em3))
      (log/trace "em3 type:" (type em3))
      (is (not= em1 em3))
;;      (is (ds/key=? em1 em3))

      (let [em4 (merge em3 {:d 27})]
        (log/trace "em4" em4)
        (is (not= em3 em4)))

      (let [em5 (merge em3 {:c #{{:d 3}}})]
        (log/trace "em5" em5)
        )

      ;; what if we want into to generate a new Entity?
      ;; (let [em4a (into! em3 {:c 3})]
      ;;   (log/trace "em4a" em4a)
      ;;   ;; entity-map into mutates in place
      ;;   (is (= em3 em4a)))
      )))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;   zipmap theorems
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(deftest ^:zipmap zipmap-1
  (testing "entity-map zipmap"
    (let [em1 (ds/entity-map [:A/B]
                             (zipmap [:a :b :c :d]
                                     [1 2 3 4]))]
      (log/trace "em1" (pr-str em1))
      (log/trace (get em1 :b))
      (is (= (get (ds/entity-map [:A/B]
                                 (zipmap [:a :b :c :d]
                                         [1 2 3 4]))
                  :b) 2))
      )))

