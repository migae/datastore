(ns test.tutorial
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
            [clojure.tools.logging :as log :only [trace debug info]]))

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
        ;; (ds/get-datastore-service)
        (test-fn)
        (.tearDown helper))))
        ;; (ApiProxy/setEnvironmentForCurrentThread environment)
        ;; (ApiProxy/setDelegate delegate))))

;(use-fixtures :once (fn [test-fn] (ds/get-datastore-service) (test-fn)))
(use-fixtures :each ds-fixture)

;; (deftest ^:init ds-init
;;   (testing "DS init"
;;     (is (= com.google.appengine.api.datastore.DatastoreServiceImpl
;;            (class (ds/get-datastore-service))))
;;     (is (= com.google.appengine.api.datastore.DatastoreServiceImpl
;;            (class @ds/*datastore-service*)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(deftest ^:api emap-predicates
  (testing "emap predicates"
    (let [e (ds/entity-map! [:Foo/bar] {})]
      (log/trace "test: emap-predicates")
      (log/trace "entity: " (.content e))
      (log/trace "(ds/emap? e): " (ds/emap? e))
      (log/trace "(ds/entity? (.content e)): " (ds/entity? (.content e)))
      (log/trace "(empty? e): " (empty? e))
      (log/trace "(ds/keychain? (ds/keychain e)): " (ds/keychain? (ds/keychain e)))
      (is (ds/emap? e))
      (is (ds/entity? (.content e)))
      (is (ds/keychain? (ds/keychain e)))
      (is (empty? e))
      )))

(deftest ^:api clojure-predicates
  (testing "clojure predicates, entity with 1 property"
    (let [e (ds/entity-map! [:Dept/IT] {:a 1})]
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
    (let [e (ds/entity-map! [:Dept/IT] {:a 1})]
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

(def test-study-members
  [["Libbie" "Greenlee" "Greenlee@example.org"]
   ["Mohammad" "Hoffert" "Hoffert@example.org"],
   ["Drucilla" "Sebastian" "Sebastian@example.org"],
   ["Marni" "Alsup" "Alsup@example.org"]])
;; ,
;;    ["Sue" "Moxley" "Moxley@example.org"],
;;    ["Clarissa" "Immel" "Immel@example.org"],
;;    ["Keeley" "Melville" "Melville@example.org"],
;;    ["Vernie" "Denner" "Denner@example.org"],
;;    ["Nena" "Seeger" "Seeger@example.org"],
;;    ["Lien" "Dedrick" "Dedrick@example.org"],
;;    ["Kurtis" "Kliebert" "Kliebert@example.org"],
;;    ["Siu" "Buch" "Buch@example.org"],
;;    ["Wm" "Hudnall" "Hudnall@example.org"],
;;    ["Hubert" "Tabb" "Tabb@example.org"],
;;    ["Garland" "Gifford" "Gifford@example.org"],
;;    ["Nakisha" "Frase" "Frase@example.org"],
;;    ["Thalia" "Drago" "Drago@example.org"],
;;    ["Eun" "Marlar" "Marlar@example.org"],
;;    ["Kortney" "Estep" "Estep@example.org"],
;;    ["Allene" "Albin" "Albin@example.org"]])

(deftest ^:api emap-add-children
  (testing "clojure map api: adding children by kind"
    (log/trace "test: clojure map api: add children by kind")
    (let [k (ds/keychain (ds/entity-map! [:Study] {:name "Test"}))]
      (doseq [mbr test-study-members]
        (let [mmap {:fname (first mbr), :lname (second mbr), :email (last mbr)}
              m (ds/entity-map! (conj k :Participant) mmap)]
          (log/trace "added: " (ds/keychain m) m)))
          )))

(deftest ^:api emap-ancestors
  (testing "clojure map api: ancestors"
    (let [e1 (ds/entity-map! [:Dept/IT] {:name "IT Dept"})
          k  (ds/keychain e1)
          emp1 (ds/entity-map! (conj k :Employee) {:lname "Bits" :fname "Joey" :email "bits@foo.org"})
          emp2 (ds/entity-map! [:Dept/IT :Employee] {:lname "Abend" :fname "Janey" :email "abend@foo.org"})
          emp3 (ds/entity-map! [:Dept/IT :Employee] {:lname "Bates" :fname "Bill" :email "bates@foo.org"})
          employees  (try (ds/entity-map* (conj k :Employee)) ; ancestory query
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
    (let [e (ds/entity-map! [:Foo/bar] {:a 1 :b 2})]
      (log/trace "test: clojure map api")
      (log/trace "entity: " (.content e))
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

(deftest ^:props emap-embedded-map-1
  (testing "using a map as a property value"
    (let [e (ds/entity-map! [:Foo/bar] {:a 1, :b {:c 3, :d 4}})]
      (log/trace "test: emap-embedded 1")
      (log/trace "e: " e)
      (log/trace "(e :b): " (e :b) (type (e :b)))
      (log/trace "(seq e): " (seq e)))))


;; FIXME:  recursive embedding doesn't work yet
(deftest ^:props emap-embedded-map-2
  (testing "using a map as a property value - dbl embed"
    (let [e (ds/entity-map! [:Foo/bar] {:a 1, :b {:c 3, :d {:e 4}}})]
      (log/trace "test: emap-embedded 2")
      (log/trace "e: " e)
      (log/trace "(e :b): " (e :b))
      (log/trace "(:c (:b e)): " (:c (:b e)))
      (log/trace "(seq e): " (seq e)))))

(deftest ^:props emap-embedded-vec
  (testing "using a vector as a property value"
    (let [e (ds/entity-map! [:Foo/bar] {:a 1, :b [1 2 3]})]
      (log/trace "test: emap-embedded-vec")
      (log/trace "e: " e)
      (log/trace "entity: " (.content e))
      (log/trace "(e :b): " (e :b))
      (log/trace "(e :b): " (e :b))
      (log/trace "(seq e): " (seq e)))))

(deftest ^:props emap-embedded-set
  (testing "using a set as a property value"
    (let [e (ds/entity-map! [:Foo/bar] {:a 1, :b #{1 2 3}})]
      (log/trace "test: emap-embedded-set")
      (log/trace "e: " e)
      (log/trace "entity: " (.content e))
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
  (let [e (ds/entity-map! [:Foo/bar] {:a 1, :b {:c 3, :d {:e 4, :f 5}}})
        indent 4]
    (seqtest e indent))))

(deftest ^:api apix
  (testing "clojure api: query"
    (let [e (ds/entity-map! [:Foo/d3] {:a 1 :b 2})]
      (log/trace e)
      (log/trace (.content e))
      (log/trace (type e))
      (log/trace "associative? " (associative? e))
      (log/trace "map? " (map? e))
      (log/trace "lookup1 " (:a e))
      )))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(deftest ^:meta emap-meta
  (testing "emap meta"
    (let [e (ds/entity-map! [:Foo/d3] {:a 1 :b 2})]
      (is (ds/emap? e))
      (is (= (type e) migae.datastore.PersistentEntityMap))
      (is (= (type (:migae/key (meta e))) clojure.lang.PersistentVector))
      (is (= (type (.getKey (.content e))) com.google.appengine.api.datastore.Key))
    )))

;; ################################################################
;;  infix

;; (deftest ^:infix infix-1
;;   (testing "infix 1"
;;     (log/trace (infix/$= :sex = :M))
;;     (log/trace (infix/$= :age >= 50))
;;     (log/trace (infix/$= :age >= 18 && :age <= 65))
;;     ;; (is (= (infix/$= m = m)
;;     ;;        true))
;;     ;; (is (= (infix/$= :age < 50)
;;     ;;        true))
;;     ;; (is (= (infix/$= m > n)
;;     ;;        false))
;;     ))
