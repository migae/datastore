(ns test.props
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
            [migae.datastore.api :as ds]
            [clojure.tools.logging :as log :only [trace debug info]]))

(defmacro should-fail [body]
  `(let [report-type# (atom nil)]
     (binding [clojure.test/report #(reset! report-type# (:type %))]
       ~body)
     (testing "should fail"
       (is (= @report-type# :fail )))))

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

(deftest ^:props props1
  (testing "properties 1"
    (let [int_ (ds/entity-map! [:Foo] {:a 1})
          float_ (ds/entity-map! [:Foo] {:a 1.0})
          string_ (ds/entity-map! [:Foo] {:a "Hello"})
          bool_ (ds/entity-map! [:Foo] {:a true})

          ;; keywords
          map_ (ds/entity-map! [:Foo] {:a {:b :c}})
          vec_ (ds/entity-map! [:Foo] {:a [:b :c]})
          lst_ (ds/entity-map! [:Foo] {:a '(:b :c)})
          set_ (ds/entity-map! [:Foo] {:a #{:b :c}})

          ;; symbols
          map2 (ds/entity-map! [:Foo] {:a {'b 'c}})
          vec2 (ds/entity-map! [:Foo] {:a ['b 'c]})
          lst2 (ds/entity-map! [:Foo] {:a '(b c)})
          set2 (ds/entity-map! [:Foo] {:a #{'b 'c}})
          ;; FIXME: make nested quote work
          ;; lst2 (ds/entity-map! [:Foo] {:x '('b 'c)})

          mix1 (ds/entity-map! [:Foo] {:a {:b :c}
                                  :b [1 2]
                                  :c '(foo bar)
                                  :d #{1 'x :y "z"}})
          ;; nested
          ]
      ;; (log/trace "int_" int_)
      ;; (log/trace "bool_" bool_)

      ;; (log/trace "map_" map_)
      ;; (log/trace "vec_" vec_)
      ;; (log/trace "lst_" lst_)
      (log/trace "set_" set_)
      (log/trace "set2" set2)

      (log/trace "mix1" mix1)

      ;; (is (map? (:a map_)))
      ;; (is (vector? (:a vec_)))
      ;; (is (list? (:a lst_)))
      (is (set? (:a set_)))
      )))

(deftest ^:props emap-props
  (testing "Entity property types"
    (let [e1 (ds/entity-map [:Foo/bar]
                        {:int 1 ;; BigInt and BigDecimal not supported
                         :float 1.1
                         :bool true
                         :string "I'm a string"
                         :today (java.util.Date.)
                         :email (Email. "foo@example.org")
                         :dskey [:Foo/bar] ;; (ds/key :Foo/bar)
                         :link (Link. "http://example.org")
                         ;; TODO :embedded (EmbeddedEntity. ...
                         ;; TODO: Blob, ShortBlob, Text, GeoPt, PostalAddress, PhoneNumber, etc.
                         })]
      (log/trace "test: emap-props")
      (log/trace "e1: " e1)
      (log/trace "(e1 :notfound) " (e1 :notfound))
      (is (= (type (:int e1)) java.lang.Long))
      (is (integer? (:int e1)))
      (is (= (type (:float e1)) java.lang.Double))
      (is (float? (:float e1)))
      (is (= (type (:bool e1)) java.lang.Boolean))
      (is (= (type (:string e1)) java.lang.String))
      (is (= (type (:today e1)) java.util.Date))
      )))


;; ################################################################
;;  infix

;; (deftest ^:infix infix-1
;;   (testing "infix 1"
;;     (log/trace (infix/$= :sex = :M))
;;     (log/trace (infix/$= :age >= 50))
;;     (log/trace (infix/$= :age >= 18 && :age <= 65))
;;     ;; (is (= (infix/$= m = m)
;;     ;;        true))
;;     ;; (is (= (infix/$= :age < 50)
;;     ;;        true))
;;     ;; (is (= (infix/$= m > n)
;;     ;;        false))
;;     ))
