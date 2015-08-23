(ns migae.dsm
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
        (ds/init)
        (test-fn)
        (.tearDown helper))))
        ;; (ApiProxy/setEnvironmentForCurrentThread environment)
        ;; (ApiProxy/setDelegate delegate))))

;(use-fixtures :once (fn [test-fn] (dss/get-datastore-service) (test-fn)))
(use-fixtures :each ds-fixture)

(deftest ^:init ds-init
  (testing "DS init"
    (log/trace (ds/init))
    (is (= migae.datastore.DatastoreMap
           (class @ds/DSMap)))
    ))

;; ################################################################
(deftest ^:ds dsmap-lookup
  (testing "ds map lookup"
    (ds/emap!! [:Foo/Bar] {:a 1})
    (let [em1 (@ds/DSMap [:Foo/Bar])
          em2 (get @ds/DSMap [:Foo/Bar])]
      (log/trace "em1:" em1)
      (is (= (:a em1) 1))
      (is (= (get em1 :a) 1))
      (log/trace "em2:" em2)
      (is (= (:a em2) 1))
      (is (= (get em2 :a) 1))
      )))

(deftest ^:ds dsmap-associative
  (testing "ds map associative"
    (ds/emap!! [:Foo/Bar] {:a 1})
    (ds/emap!! [:Foo/Bar :Baz/A] {:a 1})
    (let [em1 (:Foo/Bar @ds/DSMap)
          em2 (@ds/DSMap :Foo/Bar)
          ;; em3 (:Foo/Bar/Baz/A @ds/DSMap)
          ;; em4 (@ds/DSMap :Foo/Bar/Baz/A)
          ]
      (log/trace "em1" em1)
      (log/trace "em2" em2)
      ;; (log/trace "em3" em3)
      ;; (log/trace "em4" em4)
      )))

;; filters instead of queries.
;; todo: predicate-maps - maps that serve as predicates
;; e.g. suppose ds contains {:a 1 :b 2 :c 3}
;; then given  pred = {:a 1 :c 3} we have
;;   (filter pred ds) --> a satisfies pred so it is returned
;; iow, a predicate-map is literally a subset of the maps it matches
;; signature: arg 1 is a map, arg 2 is a collection of maps

(deftest ^:ds dsmap-filter
  (testing "ds map filter"
    (ds/emap!! [:Foo/Bar :Baz/A] {:a 1})
    (ds/emap!! [:Foo/Bar :Baz/B] {:b 2})
    (let [ems (ds/ds-filter [:Foo/Bar :Baz/A]) ;; redundant:  @ds/DSMap)
          ;; same as:
          ems2 (ds/emaps?? [:Foo/Bar :Baz/A])
          ;; or:  #migae/filter [:Foo/Bar] using a reader?
          ]
      (log/trace "ems" ems)
      )))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(deftest ^:ds kvw-mapentry
  (testing "entity as key-valued map entry"
    (let [em1 (ds/emap!! [:A/B] {:a 1})
          em2 (ds/emap!! [:A/C] {:a 1 :b 2})
          em3 (ds/emap!! [:A/B :C/D] {:a 1 :b 2 :c "foo"})]
      (log/trace "em1:" (ds/epr em1))
      (log/trace "(key em1)" (key em1))
      (log/trace "(val em1)" (val em1))
      (log/trace "(keys em1)" (keys em1))
      (log/trace "(vals em1)" (vals em1))
      (log/trace "")
      (log/trace "em2:" (ds/epr em2))
      (log/trace "(key em2)" (key em2))
      (log/trace "(val em2)" (val em2))
      (log/trace "(keys em2)" (keys em2))
      (log/trace "(vals em2)" (vals em2))

      (log/trace "")
      (log/trace "em3:" (ds/epr em3))
      (log/trace "type em3:" (type em3))
      (log/trace "class em3:" (class em3))
      (log/trace "(key em3)" (key em3))
      (log/trace "(val em3)" (pr-str (val em3)))
      (log/trace "(keys em3)" (keys em3))
      (log/trace "(vals em3)" (pr-str (vals em3)))

      (let [cm (into {} em3)            ; copy into {} loses metadata!
            cm2 (into ^migae.datastore.EntityMap {} em3)]
        (log/trace "")
        (log/trace "cm:" (ds/epr cm))
        (log/trace "meta cm:" (meta cm))
        (log/trace "type cm:" (type cm))
        (log/trace "class cm:" (class cm))
        (log/trace "(key cm): " (key cm))
        ;; (log/trace "(val cm)" (pr-str (val cm)))
        (log/trace "(keys cm)" (keys cm))
        (log/trace "(vals cm)" (pr-str (vals cm))))
      )))
