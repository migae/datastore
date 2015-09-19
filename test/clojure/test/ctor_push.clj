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
           [com.google.appengine.api.datastore EntityNotFoundException])
  ;; (:use [clj-logging-config.log4j])
  (:require [clojure.test :refer :all]
            [migae.datastore.api :as ds]
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
      ;; (ds/init)
      (test-fn)
      (.tearDown helper))))
;; (ApiProxy/setEnvironmentForCurrentThread environment)
;; (ApiProxy/setDelegate delegate))))

                                        ;(use-fixtures :once (fn [test-fn] (dss/get-datastore-service) (test-fn)))
(use-fixtures :each ds-fixture)

(deftest ^:ctor-push ctor-push-fail-1.0
  (testing "ctor-push fail: non-empty keychain"
    (let [em (try (ds/entity-map! [] {:a 2})
                  (catch java.lang.IllegalArgumentException x x))]
      (is (= "Null keychain '[]' not allowed"
         (.getMessage em)))
      )))

(deftest ^:ctor-push ctor-push-fail-1.1
  (testing "ctor-push fail: keychain of keylinks"
    (let [em (try (ds/entity-map! [1 2] {:a 2})
                  (catch java.lang.IllegalArgumentException ex ex))]
      (is (= "Invalid keychain[1 2]"
             (.getMessage em)))
      )))

(deftest ^:ctor-push ctor-push-fail-1.2
  (testing "ctor-push fail: keychain of keylinks"
    (let [em (try (ds/entity-map! [:A :B] {:a 2})
                   (catch java.lang.RuntimeException ex ex))]
      (is (= "Invalid keychain: '[:A :B]'"
             (.getMessage em)))
      )))

(deftest ^:ctor-push ctor-push-fail-1.3
  (testing "ctor-push fail: vector keychain"
    (let [em (try (ds/entity-map! 2 {:a 2})
                  (catch java.lang.IllegalArgumentException x x))]
      (is (= "No implementation of method: :entity-map! of protocol: #'migae.datastore.api/Entity-Map found for class: java.lang.Long"
             (.getMessage em)))
      )))

(deftest ^:ctor-push ctor-push-fail-1.4
  (testing "ctor-push fail: vector keychain"
    (let [em (try (ds/entity-map! :x {:a 2})
                  (catch java.lang.IllegalArgumentException x x))]
      (is (= "Invalid mode keyword: :x"
             (.getMessage em)))
      )))

(deftest ^:ctor-push ctor-push-fail-2
  (testing "ctor-push fail"
    (let [em1 (ds/entity-map! [:A/B] {:a 1})
          em2 (try (ds/entity-map! [:A/B] {:a 2})
                   (catch java.lang.RuntimeException ex ex))]
      (is (= (type em2) java.lang.RuntimeException))
      (is (= "Key already used" (.getMessage em2)))
      )))

(deftest ^:ctor-push ctor-push-proper
  (testing "ctor-push proper keychains"
    (let [em1 (ds/entity-map! [:A/A] {:x 9})
          em2 (ds/entity-map! [:A/B] {:a 1})
          em3 (ds/entity-map! [:X/Y :A/A] {})
          em4 (ds/entity-map! [:X/Y :B] {:a 1})
          ]
      (log/trace "em1:" (ds/dump em1))
      (log/trace "em2:" (ds/dump em2))
      (log/trace "em3:" (ds/dump em3))
      (log/trace "em4:" (ds/dump em4))
      )))

(deftest ^:ctor-push ctor-push-improper
  (testing "ctor-push improper keychains"
   (let [em1 (ds/entity-map! [:A/B :C] {:a 1})
         em2 (ds/entity-map! [:A/B :C] {})
          ]
      (log/trace "em1:" (ds/dump em1))
      (log/trace "em2:" (ds/dump em2))
      )))

(deftest ^:ctor-push ctor-push-force
  (testing "co-constructor"
    (let [em1  (ds/entity-map! [:A/B] {:a 1})
;; FIXME          em1a (get ds/store-map [:A/B])
          em2  (ds/entity-map! :force [:A/B] {:a 2})
          em2a (ds/entity-map* [:A/B] {})
    ;; (ds/entity-map! :force [:Foo/Bar] {:a 1})
    ;; (ds/entity-map! :force  [:A/A] {:a 1})
    ;; (ds/entity-map! :force  [:A/B] {:a 1})
    ;; (ds/entity-map! :force  [:A/C] {:a 1})
          ]
      (log/trace "em1:" (ds/dump em1))
;; FIXME      (log/trace "em1a:" (ds/dump em1a))
      (log/trace "em2:" (ds/dump em2))
      (log/trace "em2a:" (ds/dump em2a))
      )))

(deftest ^:ctor-push ctor-kinded-1
  (testing "entity-map kind ctor 1: keylink"
      (let [m {:a 1}
            em1 (ds/entity-map! [:Foo] m)
            em2 (ds/entity-map! [:Foo] m)]
        (log/trace "em1" (ds/dump em1))
        (log/trace "em2" (ds/dump em2))
        (is (ds/entity-map? em1))
        )))

(deftest ^:ctor-push ctor-kinded-2
  (testing "entity-map kind ctor 2: keychain"
      (let [m {:a 1}
            em1 (ds/entity-map! [:A/B :Foo] m)
            em2 (ds/entity-map! [:A/B :Foo] m)]
        ;; (log/trace "em1" (ds/dump em1))
        ;; (log/trace "em1 kind:" (ds/kind em1))
        ;; (log/trace "em1 identifier:" (ds/identifier em1))
        (is (= (ds/kind em1) :Foo))
        (is (= (ds/kind em1) (ds/kind em2)))
;; FIXME        (is (= (val em1) (val em2)))
        (is (not= (ds/identifier em1) (ds/identifier em2)))
        )))

(deftest ^:ctor ctor-push-multi
  (testing "construct multiple emaps in one go"
    ;; FIXME
    (let [ems (ds/entity-map! :multi [:Foo] [{:a 1} {:a 2} {:a 3}])]
      (log/trace "ems:" ems)
      (doseq [em ems]
        (log/trace (ds/dump-str em)))
      )))

    ;; (ds/entity-map! :multi [:A/B :Foo] [{:a 1} {:a 2} {:a 3}])
    ;; (ds/entity-map! :multi [:Bar] [{:a 1} {:a 2} {:a 3}])
    ;; (ds/entity-map! :multi [:A] [{:a 1} {:a 2} {:a 3}])
    ;; (ds/entity-map! :multi [:A/A :A] [{:a 1} {:a 2} {:a 3}])
    ;; (ds/entity-map! :multi [:A/A :B] [{:a 1} {:a 2} {:a 3}])
    ;; (ds/entity-map! :multi [:A/A :C] [{:a 1} {:a 2} {:a 3}])
    ;; (ds/entity-map! :multi [:B] [{:a 1} {:a 2} {:a 3}])
    ;; (ds/entity-map! :multi [:A/B :A] [{:a 1} {:a 2} {:a 3}])
    ;; (ds/entity-map! :multi [:A/B :B] [{:a 1} {:a 2} {:a 3}])
    ;; (ds/entity-map! :multi [:A/B :C] [{:a 1} {:a 2} {:a 3}])
    ;; (ds/entity-map! :multi [:C] [{:a 1} {:a 2} {:a 3}])
    ;; (ds/entity-map! :multi [:A/C :A] [{:a 1} {:a 2} {:a 3}])
    ;; (ds/entity-map! :multi [:A/C :B] [{:a 1} {:a 2} {:a 3}])
    ;; (ds/entity-map! :multi [:A/C :C] [{:a 1} {:a 2} {:a 3}])
    ;; ))

    ;; preferred syntax: (filter <keypred>? <valpred>?  @ds/DSMap)
    ;; e.g.   (filter [:Foo] @ds/DSMap)

    ;; (let [es (filter #(= (ds/kind %) :Foo)  @ds/DSMap)]
    ;;   (log/trace "filtered by kind:")
    ;;   (doseq [e es]
    ;;     (log/trace (ds/dump e)))
    ;;   )

    ;; (let [es (ds/filter [:Foo])]
    ;;   (log/trace "filtered on key:")
    ;;   (doseq [e es]
    ;;     (log/trace (ds/dump e)))
    ;;   )

;; FIXME: kindless queries cannot filter on properties
    ;; (let [es (ds/filter [] {:a 1})]
    ;;   (log/trace "filtered on val:")
    ;;   (doseq [e es]
    ;;     (log/trace (ds/dump e)))
    ;;   )

    ;; (let [es1 (ds/filter [:Foo] {:a 1})
    ;;       es2 (ds/filter [:Foo] {:a '(> 1)})]
    ;;   (log/trace "filtered on key and val:")
    ;;   (doseq [e es2]
    ;;     (log/trace (ds/dump e)))
    ;;   )
