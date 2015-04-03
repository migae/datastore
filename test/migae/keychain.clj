(ns migae.keychain
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

;; ################################################################
(deftest ^:keys keys1
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

(deftest ^:keys keys2
  (testing "keyword - key literals: name"
    (let [e1 (ds/emap!! [:A]{})
          e2 (ds/emap!! [:A/B]{})
          e3 (ds/emap!! [:A/d99]{})
          e4 (ds/emap!! [(keyword "A" "123")]{})]
    (log/trace "keys2 e1 key: " (ds/key e1))
    (is (ds/key? (ds/key e1)))
    (is (= (ds/key e1)) (ds/key :A))
    (is (= (ds/key e2)) (ds/key :A/B))
    )))

(deftest ^:keys identifiers
  (testing "identifiers from EntityMaps"
    (let [e1 (ds/emap!! [:A]{})
          e2 (ds/emap!! [:A/B]{})
          e3 (ds/emap!! [:A/d99]{})
          e4 (ds/emap!! [(keyword "A" "123")]{})]
    (log/trace "e1 identifier: " (ds/identifier e1))
    (log/trace "e2 identifier: " (ds/identifier e2))
    (is (= (ds/identifier e2) "B"))
    (log/trace "e3 identifier: " (ds/identifier e3))
    (is (= (ds/identifier e3) 99))
    (log/trace "e4 identifier: " (ds/identifier e4))
    (is (= (ds/identifier e4) 123))
    )))

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

