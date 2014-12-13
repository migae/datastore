(ns migae.datastore-test
  (:refer-clojure :exclude [name hash])
  (:import [com.google.appengine.tools.development.testing
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
            [org.mobileink.migae.infix :as infix]
            [migae.datastore :as ds]
            [migae.datastore.service :as dss]
            [migae.datastore.entity :as dse]
            [migae.datastore.query  :as dsqry]
            [migae.datastore.key    :as dskey]
            [clojure.tools.logging :as log :only [trace debug info]]))
;            [ring-zombie.core :as zombie]))
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
    (do (.setUp helper)
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
    (let [em ^{:_kind :Employee,
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
        (log/info (format "child ent  %s" e))
        (log/info (format "child meta  %s" (meta e)))
        (log/info (format "child key  %s" k))
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
                 :Person
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
(deftest ^:keys entity-map-keys-1
  (testing "entitymap deftype keys 1"
    (let [em ^{:_kind :Employee, :_name "asalieri"}
          {:fname "Antonio", :lname "Salieri"}]
      (let [key (ds/persist em)]
        (is (= (dskey/id key)
               0))
        (is (= (dskey/name key)
               "asalieri")))
      )))

(deftest ^:keys entity-map-keys-2
  (testing "entitymap deftype keys 2"
    (is (= (type (dskey/make {:_kind :Employee :_name "asalieri"}))
           com.google.appengine.api.datastore.Key))
    (is (= (type (dskey/make {:_kind :Employee :_id 99}))
           com.google.appengine.api.datastore.Key))
    ))

(deftest ^:keys entity-map-keys-child
  (testing "entitymap deftype keys child"
    (let [key (dskey/make {:_kind :Genus :_name "Felis"})
          child (dskey/child key {:_kind :Genus :_name "Felis"})]
      (is (= (type child)
             com.google.appengine.api.datastore.Key))
      (is (= (dskey/parent child)
             key))
      (is (= (dskey/name child)
             "felis catus"))
      (println child)
      )))

(deftest ^:keys entity-map-keys-parent
  (testing "entitymap deftype keys parent"
    (let [parent (dskey/make {:_kind :Genus :_name "Felis"})
          child  (dskey/make {:_parent parent :_kind :Species :_name "felis catus"})]
      (is (= (type child)
             com.google.appengine.api.datastore.Key))
      (is (= (dskey/parent child)
             parent))
      (is (= (dskey/name child)
             "felis catus"))
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


(deftest ^:query query-ancestors-1
  (testing "query ancestors-1"
    (let [k (dskey/make "foo" "bar")
          q (dsqry/ancestors :key k)]
      (log/trace "query ancestors-1:" q)
      )))

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
