(ns test.ctor-bulk
  "unit tests for bulk constructor: entity-map!"
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
            [clojure.tools.reader.edn :as edn]
            [schema.core :as s] ;; :include-macros true]
            [migae.datastore.adapter.gae :as gae]
            [migae.datastore.signature.entity-map :as ds]
            [clojure.tools.logging :as log :only [trace debug info]]))
;            [ring-zombie.core :as zombie]))

(defmacro should-fail [body]
  `(let [report-type# (atom nil)]
     (binding [clojure.test/report #(reset! report-type# (:type %))]
       ~body)
     (testing "should fail"
       (is (= @report-type# :fail )))))

(defn- ds-fixture
  [test-fn]
  (let [helper (LocalServiceTestHelper.
                (into-array LocalServiceTestConfig
                            [(LocalDatastoreServiceTestConfig.)]))]
    (do
        (.setUp helper)
        ;; (ds/init)
        (test-fn)
        (.tearDown helper))))

;(use-fixtures :once (fn [test-fn] (dss/get-datastore-service) (test-fn)))
(use-fixtures :each ds-fixture)

(deftest ^:ctor bulk
  (testing "entity-map bulk push ctor"
    (ds/register-schema
     :Person/#1 [(s/one s/Str "fname") (s/one s/Str "lname") (s/one s/Str "email")])
    (ds/register-schema
     :Study#1 [(s/one s/Str "name")])
    (let [data (edn/read-string
                "{:kind :Person
                  :schema :Person/#1
                  :data [[\"Libbie\" \"Greenlee\" \"Greenlee@example.org\"]
                        [\"Mohammad\" \"Hoffert\" \"Hoffert@example.org\"]
                        [\"Marni\" \"Alsup\" \"Alsup@example.org\"]
                        [\"Sue\" \"Moxley\" \"Moxley@example.org\"]]}")
          ems (ds/entity-map! data)]
      (log/debug "ems: " (str ems))
      (doseq [em (ds/entity-map* [:Person] {:email "Greenlee@example.org"})]
        (log/debug "matched: " (ds/dump-str em)))
      )))
