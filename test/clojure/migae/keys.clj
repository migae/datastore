(ns migae.keys
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
            [migae.datastore.keychain :as k]
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

;; ################################################################
(deftest ^:keys keys-1
  (testing "keys 1: keylink literals: name"
    (let [k1 (ds/entity-key :Employee/asalieri)
          k2 (ds/entity-key :Employee/d15)
          k3 (ds/entity-key :Employee/x0F)]
    (log/trace "k1: " k1)
    (is (ds/ekey? k1) true)
    (is (= (type k1) com.google.appengine.api.datastore.Key))
    (log/trace k2)
    (is (ds/ekey? k2) true)
    (log/trace k3)
    (is (ds/ekey? k3) true)
    (is (= k2 k3))
    )))

(deftest ^:keys keys-2
  (testing "keys 2: keychain literals: name"
    (let [e1 (ds/entity-map [:A/B]{})
          e2 (ds/entity-map [:A/d99]{})
          e3 (ds/entity-map [(keyword "A" "123")]{})]
    (log/trace "e1 key: " (ds/entity-key e1))
    (log/trace "e1 key to keychain: " (ds/to-keychain (ds/entity-key e1)))
    (log/trace "keys2 e1 keychain:" (ds/to-keychain e1))
    (is (ds/ekey? (ds/entity-key e1)))
    (is (= (ds/entity-key e1)) (ds/entity-key [:A/B]))
    (is (= (ds/entity-key e2)) (ds/entity-key :A/B))
    )))

(deftest ^:keys keys-3
  (testing "keys 3: keylink construction"
    (let [e1 (ds/entity-map [(keyword "A" "B")]{})
          e2 (ds/entity-map [(keyword "A" "99")]{})
          e3 (ds/entity-map [(keyword "A" "B") (keyword "C" "123")]{})]
    (log/trace "e1 key: " (ds/entity-key e1))
    (log/trace "e1 key to keychain: " (ds/to-keychain (ds/entity-key e1)))
    (log/trace "keys2 e1 keychain:" (ds/to-keychain e1))
    (is (ds/ekey? (ds/entity-key e1)))
    (is (= (ds/entity-key e1)) (ds/entity-key :A/B))
    )))

(deftest ^:keychain keychain-fail
  (testing "keys 3: keychain literals: name"
    (let [ex (try (k/keychain-to-key [:A/B :X])
                  (catch IllegalArgumentException e e))]
      (is (= (type ex) IllegalArgumentException))
      (is (= (.getMessage ex)
             "Invalid keychain: [:A/B :X]")))
    ;; FIXME: restructure to verifiy exception occurs, as above
    (try (k/keychain-to-key [:A/B :Y :D/E])
         (catch IllegalArgumentException ex
           (is (= (.getMessage ex)
                  "Invalid keychain: [:A/B :Y :D/E]"))))
    (try (k/keychain-to-key [:A/B 'Z :D/E])
         (catch IllegalArgumentException ex
           (is (= (.getMessage ex)
                  "Invalid keychain: [:A/B Z :D/E]"))))
    (try (k/keychain-to-key [:A/B "C" :D/E])
         (catch java.lang.RuntimeException e
           (is (= (.getMessage e)
                  "Invalid keychain: [:A/B \"C\" :D/E]"))))
    (try (k/keychain-to-key '(:A/B))
         (catch IllegalArgumentException ex
           (is (= "Invalid keychain: (:A/B)"
               (.getMessage ex)))))
    (try (k/keychain-to-key '{:a 1})
         (catch IllegalArgumentException ex
           (is (= "Invalid keychain: {:a 1}"
               (.getMessage ex)))))
    (try (k/keychain-to-key [:A])
         (catch IllegalArgumentException ex
           (is (= "missing namespace: :A")
               (.getMessage ex))))
    (try (k/keychain-to-key :A)
         (catch IllegalArgumentException ex
           (is (= "Invalid keychain: :A"
                  (.getMessage ex)))))
    (try (k/keychain-to-key [9])
         (catch IllegalArgumentException ex
           (is (=  "Invalid keychain: [9]"
                  (.getMessage ex)))))
    (try (k/keychain-to-key 9)
         (catch IllegalArgumentException ex
           (is (= "Invalid keychain: 9"
                   (.getMessage ex)))))
    (try (k/keychain-to-key ['a])
         (catch IllegalArgumentException ex
           (is (= "Invalid keychain: [a]"
                  (.getMessage ex)))))
    (try (k/keychain-to-key 'a)
         (catch IllegalArgumentException ex
           (is (= "Invalid keychain: a"
                  (.getMessage ex)))))
    (try (k/keychain-to-key ["a"])
         (catch IllegalArgumentException ex
           (is (= "Invalid keychain: [\"a\"]"
                  (.getMessage ex)))))
    (try (k/keychain-to-key "a")
         (catch IllegalArgumentException ex
           (is (= "Invalid keychain: a"
                  (.getMessage ex)))))
    ))

;; (deftest ^:keychain identifiers
;;   (testing "identifiers from PersistentEntityMaps"
;;     (let [e1 (ds/entity-map [:A]{})
;;           e2 (ds/entity-map [:A/B]{})
;;           e3 (ds/entity-map [:A/d99]{})
;;           e4 (ds/entity-map [(keyword "A" "123")]{})]
;;     (log/trace "e1 identifier: " (ds/identifier e1))
;;     (log/trace "e2 identifier: " (ds/identifier e2))
;;     (is (= (ds/identifier e2) "B"))
;;     (log/trace "e3 identifier: " (ds/identifier e3))
;;     (is (= (ds/identifier e3) 99))
;;     (log/trace "e4 identifier: " (ds/identifier e4))
;;     (is (= (ds/identifier e4) 123))
;;     )))

;; (deftest ^:keysym keysym2
;;   (testing "keymap literals: name"
;;     (is (ds/ekey? (ds/entity-key :Employee :asalieri)) true)
;;     (is (ds/ekey? (ds/entity-key "Employee" "asalieri")) true)))

    ;; (is (= (type (dskey/make 'Employee/x0F)) com.google.appengine.api.datastore.Key)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(deftest ^:keychain dogtags-1
  (testing "dogtags 1"
    (let [;; create three entities with same dogtag :X/Y,
          ;; but distinct keychain ancestry; they all have the same
          ;; dogtag [:X/Y], Kind (:X) and Identifier ("Y")
          e1 (ds/entity-map [:X/Y]{})
          e3 (ds/entity-map [:A/B :C/D :X/Y]{})
          e2 (ds/entity-map [:A/B :X/Y]{})]
      (log/trace "e1 (dogtag - keychain): (" (ds/dogtag e1) "-" (ds/keychain e1) ")")
      (log/trace "e2 (dogtag - keychain): (" (ds/dogtag e2) "-" (ds/keychain e2) ")")
      (log/trace "e3 (dogtag - keychain): (" (ds/dogtag e3) "-" (ds/keychain e3) ")")
      (is (= (ds/dogtag [:A/B]) (ds/dogtag [:X/Y :A/B])))
      (is (= (ds/dogtag e1) (ds/dogtag e2) (ds/dogtag e3)))
      (is (= (ds/kind e1) (ds/kind e2) (ds/kind e3)))
      (is (= (ds/kind [:A/B]) (ds/kind [:X/Y :A/B])))
      (is (= (ds/name e1) (ds/name e2) (ds/name e3)))
      (is (= (ds/identifier [:A/B]) (ds/identifier [:X/Y :A/B])))
      )))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(deftest ^:keychain keychain-1
  (testing "keychain 1"
    (let [;; create three entities with same dogtag :X/Y,
          ;; but distinct keychain ancestry; they all have the same
          ;; dogtag [:X/Y], Kind (:X) and Identifier ("Y")
          e1 (ds/entity-map [:X/Y]{})
          e2 (ds/entity-map [:A/B :X/Y]{})
          e3 (ds/entity-map [:A/B :C/D :X/Y]{})]
      (is (= (ds/keychain (ds/entity-key :A/B)) [:A/B]))
      (is (= (ds/keychain [:A/B :C/D]) [:A/B :C/D]))
      (is (= (ds/keychain e3) [:A/B :C/D :X/Y]))
      (is (not= (ds/keychain e1) (ds/keychain e2) (ds/keychain e3)))
      )))

(deftest ^:keychain keychain1a
  (testing "keychain sym key literals 1a"
    (log/trace (ds/entity-key [:Genus/Felis :Species/Felis_catus]))
    (ds/ekey? (ds/entity-key [:Genus/Felis :Species/Felis_catus]))))

(deftest ^:keychain keychain1b
  (testing "keychain sym key literals 1b"
    (log/trace (ds/entity-key [:Subfamily/Felinae :Genus/Felis :Species/Felis_catus]))
    (ds/ekey? (ds/entity-key [:Subfamily/Felinae :Genus/Felis :Species/Felis_catus]))))

(deftest ^:keychain keychain1c
  (testing "keychain sym key literals 1c"
    (log/trace (ds/entity-key [:Family/Felidae :Subfamily/Felinae :Genus/Felis :Species/Felis_catus]))
    (ds/ekey? (ds/entity-key [:Family/Felidae :Subfamily/Felinae :Genus/Felis :Species/Felis_catus]))))


(deftest ^:keychain keychain20
  (testing "keychain string key literals 2"
    (ds/ekey? (ds/entity-key [:Subfamily :Felinae :Genus :Felis]))
    (ds/ekey? (ds/entity-key ["Subfamily" "Felinae" "Genus" "Felis"]))))

;; TODO: support string syntax:
;; (deftest ^:keychain keychain30
;;   (testing "keychain - mixed key literals 30"
;;     (ds/ekey? (ds/entity-key [:Subfamily/Felinae :Genus :Felis]))))
;;     (ds/ekey? (ds/entity-key ["Subfamily" "Felinae" :Genus/Felis])))))

(deftest ^:keychain keychain3
  (testing "keychain literals 3"
    (let [chain (ds/entity-key [:Family/Felidae :Subfamily/Felinae :Genus/Felis :Species/Felis_catus])]
      (log/trace chain))))

(deftest ^:keychain proper
  (testing "keychain literals 20"
    (let [e1 (ds/entity-map [:Foo/Bar] {})]
      (log/trace "e1" (ds/print e1)))
    (let [e1 (ds/entity-map [:Foo/Bar] {})]
      (log/trace "e1" (ds/print e1)))
    (let [e1 (ds/entity-map [:Foo/Bar :Baz/Buz] {})]
      (log/trace "e1" (ds/print e1)))
    (let [e1 (ds/entity-map [:Foo/Bar :Baz/Buz :X/Y] {:a 1 :b 2})]
      (log/trace "e1" (ds/print e1)))
      ))
