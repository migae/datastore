(ns test.keys
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
           [com.google.apphosting.api ApiProxy]
           [com.google.appengine.api.datastore
            Key KeyFactory KeyFactory$Builder]
           [migae.datastore InvalidKeychainException])
  (:require [clojure.test :refer :all]
            [migae.datastore.signature.entity-map :as em]
            [migae.datastore.model.entity-map :as mod]
            [clojure.tools.logging :as log :only [trace debug info]]))

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
        ;; (em/get-datastore-service)
        (test-fn)
        (.tearDown helper))))
        ;; (ApiProxy/setEnvironmentForCurrentThread environment)
        ;; (ApiProxy/setDelegate delegate))))

;(use-fixtures :once (fn [test-fn] (em/get-datastore-service) (test-fn)))
(use-fixtures :each ds-fixture)

;; ################################################################
(defn theorem-keys-1
  [em]
  (and
   (is (vector? (em/keychain em)))
   (is (every? keyword? (em/keychain em)))
   (is (every? namespace (em/keychain em)))
   ))

(deftest ^:keys keys-theorem-1
  (testing "every keychain is a vector of namespaced keywords"
    (let [k1 [:A/B]
          m {:a 1}
          em1 (em/entity-map k1 m)
          k2 [:A/B :C/D :E/F]
          em2 (em/entity-map k2 m)
          ]
      (is (theorem-keys-1 em1) true)
      (is (theorem-keys-1 em2) true)
      )))

(defn theorem-keys-2
  [k m]
  (and
   (is (= (em/keychain (em/entity-map k m)) k))
   (is (= (em/entity-key (em/entity-map k m)) (em/entity-key k)))
   ))

(deftest ^:keys keys-theorem-2
  (testing "for all k, m: (= (em/keychain (em/entity-map k m)) k)  "
    (let [k [:A/B]
          m {:a 1}]
    (is (theorem-keys-2 k m) true)
    (is (theorem-keys-2 [:A/B :C/D] {:x :y}) true)
    )))

;; ################################################################
(deftest ^:keys keys-native
  (testing "native Keys"
    (let [k1 (KeyFactory/createKey "A" "B")
          k2 (em/entity-key [:A/B])
          k3 (em/keychain [:A/B])]
      (is (= (.getClass k1)
             com.google.appengine.api.datastore.Key))
      (is (= (.getClass k2)
             com.google.appengine.api.datastore.Key))
      (is (= k1 k2))
      (is (= (.getKind k1) "A"))
      (is (= (.getName k1) "B"))
      (is (= (.getKind k2) "A"))
      (is (= (.getName k2) "B"))
      (is (= (.getKind (KeyFactory/createKey "A" "B")) "A")) ;; native kinds are Strings
      (is (= (em/kind  (em/entity-key [:A/B])) "A")) ;; migae kinds are strings
      (is (= (em/kind [:A/B]) :A))
      (is (= (em/identifier [:A/B]) "B"))
      (is (= (em/kind (em/keychain (em/entity-map [:A/B] {:a 1}))) :A))
    )))

;; ################################################################
(deftest ^:keys keys-1
  (testing "keys 1: keylink literals: name"
    (let [k1 (em/entity-key [:Employee/asalieri])
          k2 (em/entity-key [:Employee/d15])
          k3 (em/entity-key [:Employee/x0F])]
    (log/trace "k1: " k1)
    (is (em/entity-key? k1) true)
    (is (= (type k1) com.google.appengine.api.datastore.Key))
    (log/trace k2)
    (is (em/entity-key? k2) true)
    (log/trace k3)
    (is (em/entity-key? k3) true)
    (is (= k2 k3))
    )))

(deftest ^:keys keys-2
  (testing "keys 2: keychain literals: name"
    (let [e1 (em/entity-map [:A/B]{})
          e2 (em/entity-map [:A/d99]{})
          e3 (em/entity-map [(keyword "A" "123")]{})]
      (log/trace "e1 key: " (em/entity-key e1))
      (log/trace "e1 key to keychain: " (em/keychain (em/entity-key e1)))
      (log/trace "keys2 e1 keychain:" (em/keychain e1))
      (is (em/entity-key? (em/entity-key e1)))
      (is (= (em/entity-key [:A/B]) (em/entity-key [:A/B])))
      (is (= (em/entity-key e1) (em/entity-key [:A/B])))
      ;; (is (= (em/entity-key e2) (em/entity-key :A/B)))
      )))

(deftest ^:keys keys-3
  (testing "keys 3: keylink construction"
    (let [e1 (em/entity-map [(keyword "A" "B")]{})
          e2 (em/entity-map [(keyword "A" "99")]{})
          e3 (em/entity-map [(keyword "A" "B") (keyword "C" "123")]{})]
      ;; (println "E1 type: " (type e1))
      (log/trace "e1 key: " (em/entity-key e1))
      (log/trace "e1 key to keychain: " (em/keychain (em/entity-key e1)))
      (log/trace "keys2 e1 keychain:" (em/keychain e1))
      (is (em/entity-key? (em/entity-key e1)))
      (is (= (em/entity-key e1) (em/entity-key [:A/B])))
      )
    (let [ex (try (em/entity-key [:A/B :X])
                  (catch InvalidKeychainException e e))]
      (is (= (.getMessage ex)
             "[:A/B :X]")))
    ;; FIXME: restructure to verifiy exception occurs, as above
    (try (em/entity-key [:A/B :Y :D/E])
         (catch InvalidKeychainException ex
           (is (= (.getMessage ex) "[:A/B :Y :D/E]"))))
    (try (em/entity-key [:A/B 'Z :D/E])
         (catch InvalidKeychainException ex
           (is (= (.getMessage ex) "[:A/B Z :D/E]"))))
    (try (em/entity-key [:A/B "C" :D/E])
         (catch InvalidKeychainException ex
           (is (= (.getMessage ex) "[:A/B \"C\" :D/E]"))))
    (try (em/entity-key '(:A/B))
         (catch IllegalArgumentException ex
           (is (= (.getMessage ex)
                  "No implementation of method: :entity-key of protocol: #'migae.datastore.signature.entity-map/Entity-Map found for class: clojure.lang.PersistentList"))))
    #_(try (em/entity-key '{:a 1})
           (catch java.lang.AssertionError ex
             (is (= "Assert failed: (and (vector? k) (not (empty? k)))"
                    (.getMessage ex)))))
    (try (em/entity-key [:A])
         (catch InvalidKeychainException ex
           (is (.getMessage ex) "[:A]")))
    (try (em/entity-key :A)
         (catch IllegalArgumentException ex
           (is (.getMessage ex) "No implementation of method: :entity-key of protocol: #'migae.datastore.signature.entity-map/Entity-Map found for class: clojure.lang.Keyword")))
    (try (em/entity-key [9])
         (catch InvalidKeychainException ex
           (is (= (.getMessage ex) "[9]"))))
    (try (em/entity-key 9)
         (catch IllegalArgumentException ex
           (is (= (.getMessage ex)
                  "No implementation of method: :entity-key of protocol: #'migae.datastore.signature.entity-map/Entity-Map found for class: java.lang.Long"))))
    (try (em/entity-key ['a])
         (catch InvalidKeychainException ex
           (is (= (.getMessage ex) "[a]"))))
    (try (em/entity-key 'a)
         (catch IllegalArgumentException ex
           (is (= (.getMessage ex)
                  "No implementation of method: :entity-key of protocol: #'migae.datastore.signature.entity-map/Entity-Map found for class: clojure.lang.Symbol"))))
    (try (em/entity-key ["a"])
         (catch InvalidKeychainException ex
           (is (= (.getMessage ex) "[\"a\"]"))))
    (try (em/entity-key "a")
         (catch IllegalArgumentException ex
           (is (= (.getMessage ex)
                  "No implementation of method: :entity-key of protocol: #'migae.datastore.signature.entity-map/Entity-Map found for class: java.lang.String"))))
    ))

;; (deftest ^:keychain identifiers
;;   (testing "identifiers from PersistentEntityMaps"
;;     (let [e1 (em/entity-map [:A]{})
;;           e2 (em/entity-map [:A/B]{})
;;           e3 (em/entity-map [:A/d99]{})
;;           e4 (em/entity-map [(keyword "A" "123")]{})]
;;     (log/trace "e1 identifier: " (em/identifier e1))
;;     (log/trace "e2 identifier: " (em/identifier e2))
;;     (is (= (em/identifier e2) "B"))
;;     (log/trace "e3 identifier: " (em/identifier e3))
;;     (is (= (em/identifier e3) 99))
;;     (log/trace "e4 identifier: " (em/identifier e4))
;;     (is (= (em/identifier e4) 123))
;;     )))

;; (deftest ^:keysym keysym2
;;   (testing "keymap literals: name"
;;     (is (em/entity-key? (em/entity-key :Employee :asalieri)) true)
;;     (is (em/entity-key? (em/entity-key "Employee" "asalieri")) true)))

    ;; (is (= (type (dskey/make 'Employee/x0F)) com.google.appengine.api.datastore.Key)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; (deftest ^:keychain dogtags-1
;;   (testing "dogtags 1"
;;     (let [;; create three entities with same dogtag :X/Y,
;;           ;; but distinct keychain ancestry; they all have the same
;;           ;; dogtag [:X/Y], Kind (:X) and Identifier ("Y")
;;           e1 (em/entity-map [:X/Y]{})
;;           e3 (em/entity-map [:A/B :C/D :X/Y]{})
;;           e2 (em/entity-map [:A/B :X/Y]{})]
;;       (log/trace "e1 (dogtag - keychain): (" (em/dogtag e1) "-" (em/keychain e1) ")")
;;       (log/trace "e2 (dogtag - keychain): (" (em/dogtag e2) "-" (em/keychain e2) ")")
;;       (log/trace "e3 (dogtag - keychain): (" (em/dogtag e3) "-" (em/keychain e3) ")")
;;       (is (= (em/dogtag [:A/B]) (em/dogtag [:X/Y :A/B])))
;;       (is (= (em/dogtag e1) (em/dogtag e2) (em/dogtag e3)))
;;       (is (= (em/kind e1) (em/kind e2) (em/kind e3)))
;;       (is (= (em/kind [:A/B]) (em/kind [:X/Y :A/B])))
;;       (is (= (em/identifier e1) (em/identifier e2) (em/identifier e3)))
;;       (is (= (em/identifier [:A/B]) (em/identifier [:X/Y :A/B])))
;;       )))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(deftest ^:keychain keychain-1
  (testing "keychain 1"
    (let [;; create three entities with same dogtag :X/Y,
          ;; but distinct keychain ancestry; they all have the same
          ;; dogtag [:X/Y], Kind (:X) and Identifier ("Y")
          e1 (em/entity-map [:X/Y]{})
          e2 (em/entity-map [:A/B :X/Y]{})
          e3 (em/entity-map [:A/B :C/D :X/Y]{})]
      (is (= (em/keychain (em/entity-key [:A/B])) [:A/B]))
      ;; (println "FOO: " (em/keychain [:A/B :C/D]))
      (is (= (em/keychain [:A/B :C/D]) [:A/B :C/D]))
      (is (= (em/keychain e3) [:A/B :C/D :X/Y]))
      (is (not= (em/keychain e1) (em/keychain e2) (em/keychain e3)))
      )))

(deftest ^:keychain keychain1a
  (testing "keychain sym key literals 1a"
    (log/trace (em/entity-key [:Genus/Felis :Species/Felis_catus]))
    (em/entity-key? (em/entity-key [:Genus/Felis :Species/Felis_catus]))))

(deftest ^:keychain keychain1b
  (testing "keychain sym key literals 1b"
    (log/trace (em/entity-key [:Subfamily/Felinae :Genus/Felis :Species/Felis_catus]))
    (is (em/entity-key? (em/entity-key [:Subfamily/Felinae :Genus/Felis :Species/Felis_catus])))))

(deftest ^:keychain keychain1c
  (testing "keychain sym key literals 1c"
    (log/trace (em/entity-key [:Family/Felidae :Subfamily/Felinae :Genus/Felis :Species/Felis_catus]))
    (em/entity-key? (em/entity-key [:Family/Felidae :Subfamily/Felinae :Genus/Felis :Species/Felis_catus]))))


;; (deftest ^:keychain keychain20
;;   (testing "keychain string key literals 2"
;;     (em/entity-key? (em/entity-key [:Subfamily :Felinae :Genus :Felis]))
;;     (em/entity-key? (em/entity-key ["Subfamily" "Felinae" "Genus" "Felis"]))))

;; TODO: support string syntax:
;; (deftest ^:keychain keychain30
;;   (testing "keychain - mixed key literals 30"
;;     (em/entity-key? (em/entity-key [:Subfamily/Felinae :Genus :Felis]))))
;;     (em/entity-key? (em/entity-key ["Subfamily" "Felinae" :Genus/Felis])))))

(deftest ^:keychain keychain3
  (testing "keychain literals 3"
    (let [chain (em/entity-key [:Family/Felidae :Subfamily/Felinae :Genus/Felis :Species/Felis_catus])]
      (log/trace chain))))

(deftest ^:keychain proper
  (testing "keychain literals 20"
    (let [e1 (em/entity-map [:Foo/Bar] {})]
      (log/trace "e1" (em/dump e1)))
    (let [e1 (em/entity-map [:Foo/Bar] {})]
      (log/trace "e1" (em/dump e1)))
    (let [e1 (em/entity-map [:Foo/Bar :Baz/Buz] {})]
      (log/trace "e1" (em/dump e1)))
    (let [e1 (em/entity-map [:Foo/Bar :Baz/Buz :X/Y] {:a 1 :b 2})]
      (log/trace "e1" (em/dump e1)))
      ))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(deftest ^:kinds kinds
  (testing "kinds"
    (let [e1 (em/entity-map [:A/B] {:a 1})]
      (is (= (em/kind e1) "A"))
      )))

;; (deftest ^:reader reader
;;   (testing "reader foo"
;;     (let [e1 #migae/em [:Foo/Bar] {:a 1}]
;;       (log/trace "e1" (em/dump e1))
;;       )))
