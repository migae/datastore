(ns test.ctor-pull
  "Unit tests for pull constructor entity-map*"
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
            ;;            [migae.infix :as infix]
            [migae.datastore :as ds]
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
      ;; store some entities
      (ds/entity-map! [:A/B] {:em 1 :a "foo"})
      (ds/entity-map! [:A/B :A/C] {:em 2 :a "bar"})
      (ds/entity-map! [:A/B :A/D :A/X] {:em 3})
      (ds/entity-map! [:A/B :A/C :X/Y] {:em 4})
      (ds/entity-map! [:A/B :C] {:em 5}) ; autogen id
      (ds/entity-map! [:A/B :A/C :X] {:em 6})
      (ds/entity-map! [:A/B :A/D :X] {:em 7})
      (ds/entity-map! [:A/B :A/D :X] {:em 8})
      (ds/entity-map! [:A/B :A/C :D/E :F/G :X] {:em 9})
      (test-fn)
      (.tearDown helper))))
;; (ApiProxy/setEnvironmentForCurrentThread environment)
;; (ApiProxy/setDelegate delegate))))

                                        ;(use-fixtures :once (fn [test-fn] (dss/get-datastore-service) (test-fn)))
(use-fixtures :each ds-fixture)

(deftest ^:ctor-pull ctor-pull-fail
  (testing "pull constructor: entity not found"
    (let [ex (try (ds/entity-map* [:Foo/BAR :NOT/FOUND])
                  (catch EntityNotFoundException e e))]
      (is (= (type ex) EntityNotFoundException)))))

(deftest ^:ctor-pull ctor-pull
  (testing "pull ctor"
    (let [em1 (ds/entity-map* [:A/B])        ; co-ctor
          em2 (ds/entity-map* [:A/B :C/D])
          ]
      (log/trace "em1:" (ds/print em1))
      (log/trace "em2:" (ds/print em2))
      (is (= (:a em1) 1))         ; previously fetched emap unaffected
      (is (= (:b em2) nil))
      )))

(deftest ^:ctor-pull ctor-pull-all
  (testing "pull ctor"
    (let [ems (ds/entity-map* [])]
      (log/trace "ems count:" (count ems) (type ems))
      (doseq [em ems]
        (log/trace "em:" (ds/keychain em) " kind:" (ds/kind em))
      ))))

(deftest ^:ctor-pull ctor-pull-kinded
  (testing "pull ctor"
    (let [as (ds/entity-map* [:A]) ; fetch emaps of kind :A
          xs (ds/entity-map* [:X]) ; fetch emaps of kind :X
          ]
      (log/trace "as count:" (count as) (type as))
      (log/trace "xs count:" (count xs) (type xs))
      )))

(deftest ^:ctor-pull ctor-pull-kinded-desc
  (testing "pull ctor"
    (let [abxs (ds/entity-map* [:A/B :X]) ; fetch emaps of kind :A with ancestor :A/B
          abas (ds/entity-map* [:A/B :A])
          acxs (ds/entity-map* [:A/B :A/C :X])
          ]
      (log/trace "abxs count:" (count abxs) (type abxs))
      (log/trace "abas count:" (count abas) (type abas))
      (log/trace "acxs count:" (count acxs) (type acxs))
      )))

(deftest ^:ctor-pull ctor-pull-pfx
  (testing "keychain prefix ctors return all entities whose keychains start with the prefix"
    (let [abs (ds/entity-map* :prefix [:A/B])
          abac (ds/entity-map* :prefix [:A/B :A/C])
          ]
      (log/trace "abs count:" (count abs) (type abs))
      (log/trace "abac count:" (count abac) (type abac))
      )))
