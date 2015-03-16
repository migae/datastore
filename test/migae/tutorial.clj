(ns migae.tutorial
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
(deftest ^:emap emap1
  (testing "emap key vector must not be empty"
    (try (ds/emap [] {})
         (catch IllegalArgumentException e
           (log/trace (.getMessage e))))))

(deftest ^:emap emap?
  (testing "entitymap deftype"
    (binding [*print-meta* true]
      (let [em1 (ds/emap [:Species/Felis_catus] {:name "Chibi"})
            em2 (ds/emap [:Genus/Felis :Species/Felis_catus] {:name "Chibi"})
            em3 (ds/emap [:Subfamily/Felinae :Genus/Felis :Species/Felis_catus] {:name "Chibi"})
            em4 (ds/emap [:Family/Felidae :Subfamily/Felinae :Genus/Felis :Species/Felis_catus] {:name "Chibi"})]
        (log/trace (pr-str em1))
        (is (ds/emap? em1))
        (log/trace (pr-str em2))
        (is (ds/emap? em2))
        (log/trace (pr-str em3))
        (is (ds/emap? em3))
        (log/trace (pr-str em4))
        (is (ds/emap? em4))
        ))))

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

(deftest ^:emap emap-props!
  (testing "emap! with properties"
    ;; (binding [*print-meta* true]
      (let [em1 (ds/emap! [:Species/Felis_catus] {:name "Chibi"})
            em2 (ds/emap! [:Genus/Felis :Species/Felis_catus]
                          {:name "Chibi" :size "small" :eyes 1})
            ]
        (log/trace em1)
        (is (ds/emap? em1))
        (log/trace em2)
        (is (ds/emap? em2))
        ;; (is (ds/emap? em3))
        ;; (is (ds/emap? em4))
        )))

(deftest ^:emap emap-fetch
  (testing "emap! new, update, replace"
    ;; ignore new if exists
    (let [em1 (ds/emap! [:Species/Felis_catus] {:name "Chibi"})
          em2 (ds/emap! [:Species/Felis_catus] {})]
        ;; (log/trace em1)
        (is (ds/emap= em1 em2))
        (is (= (ds/get em1 :name) "Chibi"))
        (is (= (ds/get em2 :name) "Chibi")))
    (let [em2 (ds/emap! [:Species/Felis_catus] {:name "Booger"})]
      (is (= (ds/get em2 :name) "Chibi")))

    ;; update existing
    (let [em3 (ds/emap!! [:Species/Felis_catus] {:name "Booger"})]
      (is (= (ds/get em3 :name) ["Chibi" "Booger"])))

    ;; replace existing
    (let [em4 (ds/emap!!! [:Species/Felis_catus] {:name "Max"})]
      (is (= (ds/get em4 :name) "Max")))

    (let [em (ds/emap! [:Species/Felis_catus :Name/Chibi]
                       {:size "small" :eyes 1})
          em4 (ds/emap!!!  [:Species/Felis_catus :Name/Booger]
                       {:size "lg" :eyes 2})]
      (log/trace em)
      (log/trace em4))
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
                          (ds/assoc e :sex "F")))
          em2 (ds/emap!! [:Species/Felis_catus :Cat/Booger]
                        (fn [e]
                          (ds/assoc e :age 5)))
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
;; ################################################################
(deftest ^:query entity-map-1
  (testing "query 1"
    ))


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

(deftest ^:keysym keysym2
  (testing "keymap literals: name"
    (is (ds/key? (ds/key :Employee :asalieri)) true)
    (is (ds/key? (ds/key "Employee" "asalieri")) true)))

    ;; (is (= (type (dskey/make 'Employee/x0F)) com.google.appengine.api.datastore.Key)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(deftest ^:keychain keychain1a
  (testing "keychain sym key literals 1a"
    (log/trace (ds/key :Genus/Felis :Species/Felis_catus))
    (ds/key? (ds/key :Genus/Felis :Species/Felis_catus))))

(deftest ^:keychain keychain1b
  (testing "keychain sym key literals 1b"
    (log/trace (ds/key :Subfamily/Felinae :Genus/Felis :Species/Felis_catus))
    (ds/key? (ds/key :Subfamily/Felinae :Genus/Felis :Species/Felis_catus))))

(deftest ^:keychain keychain1c
  (testing "keychain sym key literals 1c"
    (log/trace (ds/key :Family/Felidae :Subfamily/Felinae :Genus/Felis :Species/Felis_catus))
    (ds/key? (ds/key :Family/Felidae :Subfamily/Felinae :Genus/Felis :Species/Felis_catus))))


(deftest ^:keychain keychain20
  (testing "keychain string key literals 2"
    (ds/key? (ds/key :Subfamily :Felinae :Genus :Felis))
    (ds/key? (ds/key "Subfamily" "Felinae" "Genus" "Felis"))))

(deftest ^:keychain keychain30
  (testing "keychain - mixed key literals 30"
    (ds/key? (ds/key :Subfamily/Felinae :Genus :Felis)
    (ds/key? (ds/key "Subfamily" "Felinae" :Genus/Felis)))))

(deftest ^:keychain keychain3
  (testing "keychain literals 3"
    (let [chain (dskey/make :Family/Felidae :Subfamily/Felinae :Genus/Felis :Species/Felis_catus)]
      (log/trace chain))))


    ;;        com.google.appengine.api.datastore.Key))
    ;; (is (= (type (dskey/make {:_kind :Employee :_id 99}))
    ;;        com.google.appengine.api.datastore.Key))
    ;; ))

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
