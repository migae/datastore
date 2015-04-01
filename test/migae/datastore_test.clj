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

;; ################################################################
(deftest ^:xkeysym keysym1
  (testing "keyword - key literals: name"
    (is (ds/key? (ds/key :Employee/asalieri)) true)
    (is (ds/key? (ds/key :Employee/d15)) true)))

(deftest ^:xkeysym keysym2
  (testing "keymap literals: name"
    (is (ds/key? (ds/key :Employee :asalieri)) true)
    (is (ds/key? (ds/key "Employee" "asalieri")) true)))

    ;; (is (= (type (dskey/make 'Employee/x0F)) com.google.appengine.api.datastore.Key)))))
;    (is (= (type (dskey/make 'Employee/asalieri)) com.google.appengine.api.datastore.Key))))

(deftest ^:xkeysym keysym1-id
  (testing "keymap literals: id"
    (let [k (dskey/make 'Employee/_99)]
      (log/trace k)
      (is (= (type k) com.google.appengine.api.datastore.Key)))))

;; (deftest ^:xkeys keymap1-name
;;   (testing "keymap literals: name"
;;     (is (= (type (dskey/make :_kind :Employee :_name "asalieri"))
;;            com.google.appengine.api.datastore.Key))))

;; (deftest ^:xkeys keymap1-id
;;   (testing "keymap literals: id"
;;     (is (= (type (dskey/make :_kind :Employee :_id 99))
;;            com.google.appengine.api.datastore.Key))
;;     ))

(deftest ^:xkeys keymap2a
  (testing "keymap literals 2a"
    (is (= (type (dskey/make {:_kind :Employee :_name "asalieri"}))
           com.google.appengine.api.datastore.Key))))

(deftest ^:xkeys keymap2b
  (testing "keymap literals 2b"
    (is (= (type (dskey/make {:_kind :Employee :_id 99}))
           com.google.appengine.api.datastore.Key))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(deftest ^:xkeychain keychain1a
  (testing "keychain sym key literals 1a"
    (log/trace (ds/key :Genus/Felis :Species/Felis_catus))
    (ds/key? (ds/key :Genus/Felis :Species/Felis_catus))))

(deftest ^:xkeychain keychain1b
  (testing "keychain sym key literals 1b"
    (log/trace (ds/key :Subfamily/Felinae :Genus/Felis :Species/Felis_catus))
    (ds/key? (ds/key :Subfamily/Felinae :Genus/Felis :Species/Felis_catus))))

(deftest ^:xkeychain keychain1c
  (testing "keychain sym key literals 1c"
    (log/trace (ds/key :Family/Felidae :Subfamily/Felinae :Genus/Felis :Species/Felis_catus))
    (ds/key? (ds/key :Family/Felidae :Subfamily/Felinae :Genus/Felis :Species/Felis_catus))))


(deftest ^:xkeychain keychain20
  (testing "keychain string key literals 2"
    (ds/key? (ds/key :Subfamily :Felinae :Genus :Felis))
    (ds/key? (ds/key "Subfamily" "Felinae" "Genus" "Felis"))))

(deftest ^:xkeychain keychain30
  (testing "keychain - mixed key literals 30"
    (ds/key? (ds/key :Subfamily/Felinae :Genus :Felis)
    (ds/key? (ds/key "Subfamily" "Felinae" :Genus/Felis)))))

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
