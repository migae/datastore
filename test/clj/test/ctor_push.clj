(ns test.ctor-push
  (:refer-clojure :exclude [name hash])
  (:import [com.google.appengine.tools.development.testing
            LocalServiceTestHelper
            LocalServiceTestConfig
            LocalMemcacheServiceTestConfig
            LocalMemcacheServiceTestConfig$SizeUnit
            LocalMailServiceTestConfig
            LocalDatastoreServiceTestConfig
            LocalUserServiceTestConfig]
           [com.google.apphosting.api ApiProxy]
           [com.google.appengine.api.datastore EntityNotFoundException]
           migae.datastore.DuplicateKeyException
           migae.datastore.InvalidKeychainException)
  ;; (:use [clj-logging-config.log4j])
  (:require [clojure.test :refer :all]
            [migae.datastore.model.entity-map :as em]
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
      ;; (em/init)
      (test-fn)
      (.tearDown helper))))
;; (ApiProxy/setEnvironmentForCurrentThread environment)
;; (ApiProxy/setDelegate delegate))))

                                        ;(use-fixtures :once (fn [test-fn] (dss/get-datastore-service) (test-fn)))
(use-fixtures :each ds-fixture)

(deftest ^:ctor-push ctor-push-fail-1.0
  (testing "ctor-push fail: non-empty keychain"
    (let [em (try (em/entity-map! [] {:a 2})
                  (catch java.lang.IllegalArgumentException x x))]
      (is (= "Null keychain '[]' not allowed"
         (.getMessage em)))
      )))

(deftest ^:ctor-push ctor-invalid-keychain
  (testing "ctor-push fail: invalid keychain"
    (is (thrown? InvalidKeychainException (em/entity-map! [1 2] {:a 2})))
    (is (thrown? InvalidKeychainException (em/entity-map! [:a/b 'x] {:a 2})))
    (is (thrown? InvalidKeychainException (em/entity-map! [:a :b/c] {:a 2})))
    (is (thrown? InvalidKeychainException (em/entity-map! [:A :B] {:a 2})))))

(deftest ^:ctor-push ctor-push-fail-1.3
  (testing "ctor-push fail: vector keychain"
    (let [em (try (em/entity-map! 2 {:a 2})
                  (catch java.lang.IllegalArgumentException x x))]
      (is (= "No implementation of method: :entity-map! of protocol: #'migae.datastore.signature.entity-map/Entity-Map found for class: java.lang.Long"
             (.getMessage em)))
      )))

(deftest ^:ctor-push ctor-push-fail-1.4
  (testing "ctor-push fail: vector keychain"
    (let [em (try (em/entity-map! :x {:a 2})
                  (catch java.lang.IllegalArgumentException x x))]
      (is (= "Invalid mode keyword: :x"
             (.getMessage em)))
      )))

(deftest ^:ctor-push ctor-push-fail-2
  (testing "ctor-push fail"
    (em/entity-map! [:A/B] {:a 1})
    (is (thrown? DuplicateKeyException (em/entity-map! [:A/B] {:a 2})))))

(deftest ^:ctor-push ctor-push-proper
  (testing "ctor-push proper keychains"
    (let [em1 (em/entity-map! [:A/A] {:x 9})
          em2 (em/entity-map! [:A/B] {:a 1})
          em3 (em/entity-map! [:X/Y :A/A] {})
          ]
      (log/trace "em1:" (em/dump em1))
      (log/trace "em2:" (em/dump em2))
      (log/trace "em3:" (em/dump em3))
      )))

(deftest ^:ctor-push ctor-push-proper-2
  (testing "entity-map! definite"
    (let [e1 (em/entity-map! [:Foo/Bar] {:a 1})
          e2 (em/entity-map! [:Foo/Bar :Baz/Buz] {:b 1})
          e3 (em/entity-map! [:Foo/Bar :Baz/Buz :X/Y] {:b 1})]
      (log/trace "e1" (em/dump e1))
      (log/trace "e2" (em/dump e2))
      (log/trace "e3" (em/dump e3))
    )))

(deftest ^:ctor-push emap-fetch
  (testing "entity-map! new, update, replace"
    (let [em1 (em/entity-map! [:Species/Felis_catus :Cat/Chibi] {:name "Chibi"})
          em2 (em/entity-map! [:Species/Felis_catus :Cat/Max] {:name "Max"})]
        ;; FIXME: keys=?  (is (em/keys=? em1 em2))
      (log/info "em1: " em1)
      (log/info "em2: " em2)
      (is (= (get em1 :name) "Chibi"))
      (is (= (em1 :name) "Chibi"))
      (is (= (:name em1) "Chibi"))
      (is (= (get em2 :name) "Max")))

    ;; !! - update existing
    (let [em3 (em/entity-map! :force [:Species/Felis_catus :Cat/Booger] {:name "Booger"})
          em3 (em/entity-map! :force [:Species/Felis_catus] {:name 4})]
      (log/info "EM3 " em3)
      (log/info "em3: " (.content em3))
      (log/info "em3 key: " (:migae/key em3))
      ;; (is (= (:name em3) ["Chibi", "Booger" 4]))
      ;; (is (= (first (:name em3)) "Chibi")))
      )

    ;; replace existing
    ;; (let [em4 (em/alter! [:Species/Felis_catus] {:name "Max"})]
    ;;   (log/info "em4 " em4)
    ;;   (is (= (:name em4) "Max")))

    ;; (let [em5 (em/entity-map! [:Species/Felis_catus :Name/Chibi]
    ;;                    {:name "Chibi" :size "small" :eyes 1})
    ;;       em6 (em/alter!  [:Species/Felis_catus :Name/Booger]
    ;;                    {:name "Booger" :size "lg" :eyes 2})]
    ;;   (log/info "em5" em5)
    ;;   (log/info "em6" em6))
    ))

(deftest ^:ctor-push emap-fn
  (testing "emap fn"
    (let [em1 (em/entity-map! [:Species/Felis_catus] {:name "Chibi"})]
      (log/info em1))
      ))

(deftest ^:ctor-push ctor-push-improper
  (testing "ctor-push improper keychains"
   (let [em1 (em/entity-map! [:A/B :C] {:a 1})
         em2 (em/entity-map! [:A/B :C] {})
         em3 (em/entity-map! [:X/Y :B] {:a 1})
          ]
      (log/trace "em1:" (em/dump em1))
      (log/trace "em2:" (em/dump em2))
      (log/trace "em3:" (em/dump em3))
      )))

(deftest ^:ctor-push ctor-push-force
  (testing "co-constructor"
    (let [em1  (em/entity-map! [:A/B] {:a 1})
;; FIXME          em1a (get em/store-map [:A/B])
          em2  (em/entity-map! :force [:A/B] {:a 2})
          em2a (em/entity-map* [:A/B] {})
    ;; (em/entity-map! :force [:Foo/Bar] {:a 1})
    ;; (em/entity-map! :force  [:A/A] {:a 1})
    ;; (em/entity-map! :force  [:A/B] {:a 1})
    ;; (em/entity-map! :force  [:A/C] {:a 1})
          ]
      (log/trace "em1:" (em/dump em1))
;; FIXME      (log/trace "em1a:" (em/dump em1a))
      (log/trace "em2:" (em/dump em2))
      (log/trace "em2a:" (em/dump em2a))
      )))

(deftest ^:ctor-push ctor-kinded-1
  (testing "entity-map kind ctor 1: keylink"
      (let [m {:a 1}
            em1 (em/entity-map! [:Foo] m)
            em2 (em/entity-map! [:Foo] m)]
        (log/trace "em1" (em/dump em1))
        (log/trace "em2" (em/dump em2))
        (is (em/entity-map? em1))
        )))

(deftest ^:ctor-push ctor-kinded-2
  (testing "entity-map kind ctor 2: keychain"
      (let [m {:a 1}
            em1 (em/entity-map! [:A/B :Foo] m)
            em2 (em/entity-map! [:A/B :Foo] m)]
        ;; (log/trace "em1" (em/dump em1))
        ;; (log/trace "em1 kind:" (em/kind em1))
        ;; (log/trace "em1 identifier:" (em/identifier em1))
        (is (= (em/kind em1) "Foo"))
        (is (= (em/kind em1) (em/kind em2)))
;; FIXME        (is (= (val em1) (val em2)))
        (is (not= (em/identifier em1) (em/identifier em2)))
        )))

(deftest ^:ctor-push ctor-kinded-3
  (testing "ctor kinded 3"
    (let [e1 (em/entity-map! [:Foo] {:a 1 :b "Foobar"})
          e2 (em/entity-map! [:Foo/Bar :Baz] {:b 1})
          e3 (em/entity-map! [:Foo/Bar :Baz/Buz :X] {:a "Foo/Bar Baz/Buz Z"})]
      (log/trace "e1" (em/dump e1))
      (log/trace "e2" (em/dump e2))
      (log/trace "e3" (em/dump e3))
    )))

(deftest ^:ctor-push ctor-kinded-4
  (testing "entity-map! with properties"
      (let [k [:Genus/Felis :Species/Felis_catus :Housecat]
            e1 (em/entity-map! k {:name "Chibi" :size "small" :eyes 1})
            e2 (em/entity-map! k {:name "Booger" :size "medium" :eyes 2})]
                    ;; (catch java.lang.RuntimeException ex
                    ;;   (= "Key already used" (.getMessage ex))))]
        (log/info "e1" e1)
        (log/info "e2" e2)
        (log/info "e2" (em/dump-str e2))
        (log/info "e2 entity" (.content e2))
        (is (= (e1 :name) "Chibi"))
        (is (= (e2 :name) "Booger"))
        ;; (should-fail (is (= e1 e2)))
        ;; (is (not (em/keys=? e1 e2)))
        )))

(deftest ^:ctor-push ctor-multi
  (testing "create multiple PersistentEntityMaps of a kind in one stroke"
;; FIXME: support :multi
    (em/entity-map! :multi [:Foo] [{:a 1} {:a 2} {:a 3}])
    (em/entity-map! :multi [:Foo/Bar :Baz] [{:b 1} {:b 2} {:b 3}])
    ))

(deftest ^:ctor-push ctor-push-multi-2
  (testing "Construct multiple emaps in one go"
    ;; FIXME
    (let [ems (em/entity-map! :multi [:Foo] [{:a 1} {:a 2} {:a 3}])]
      (log/trace "ems:" ems)
      (doseq [em ems]
        (log/trace (em/dump-str em)))
      )))

    ;; (em/entity-map! :multi [:A/B :Foo] [{:a 1} {:a 2} {:a 3}])
    ;; (em/entity-map! :multi [:Bar] [{:a 1} {:a 2} {:a 3}])
    ;; (em/entity-map! :multi [:A] [{:a 1} {:a 2} {:a 3}])
    ;; (em/entity-map! :multi [:A/A :A] [{:a 1} {:a 2} {:a 3}])
    ;; (em/entity-map! :multi [:A/A :B] [{:a 1} {:a 2} {:a 3}])
    ;; (em/entity-map! :multi [:A/A :C] [{:a 1} {:a 2} {:a 3}])
    ;; (em/entity-map! :multi [:B] [{:a 1} {:a 2} {:a 3}])
    ;; (em/entity-map! :multi [:A/B :A] [{:a 1} {:a 2} {:a 3}])
    ;; (em/entity-map! :multi [:A/B :B] [{:a 1} {:a 2} {:a 3}])
    ;; (em/entity-map! :multi [:A/B :C] [{:a 1} {:a 2} {:a 3}])
    ;; (em/entity-map! :multi [:C] [{:a 1} {:a 2} {:a 3}])
    ;; (em/entity-map! :multi [:A/C :A] [{:a 1} {:a 2} {:a 3}])
    ;; (em/entity-map! :multi [:A/C :B] [{:a 1} {:a 2} {:a 3}])
    ;; (em/entity-map! :multi [:A/C :C] [{:a 1} {:a 2} {:a 3}])
    ;; ))

    ;; preferred syntax: (filter <keypred>? <valpred>?  @em/DSMap)
    ;; e.g.   (filter [:Foo] @em/DSMap)

    ;; (let [es (filter #(= (em/kind %) :Foo)  @em/DSMap)]
    ;;   (log/trace "filtered by kind:")
    ;;   (doseq [e es]
    ;;     (log/trace (em/dump e)))
    ;;   )

    ;; (let [es (em/filter [:Foo])]
    ;;   (log/trace "filtered on key:")
    ;;   (doseq [e es]
    ;;     (log/trace (em/dump e)))
    ;;   )

;; FIXME: kindless queries cannot filter on properties
    ;; (let [es (em/filter [] {:a 1})]
    ;;   (log/trace "filtered on val:")
    ;;   (doseq [e es]
    ;;     (log/trace (em/dump e)))
    ;;   )

    ;; (let [es1 (em/filter [:Foo] {:a 1})
    ;;       es2 (em/filter [:Foo] {:a '(> 1)})]
    ;;   (log/trace "filtered on key and val:")
    ;;   (doseq [e es2]
    ;;     (log/trace (em/dump e)))
    ;;   )
