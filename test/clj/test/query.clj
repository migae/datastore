;; MOSTLY OUTDATED

(ns test.query
  (:refer-clojure :exclude [name hash])
  (:import [com.google.appengine.tools.development.testing
            LocalServiceTestHelper
            LocalServiceTestConfig
            LocalMemcacheServiceTestConfig
            LocalMemcacheServiceTestConfig$SizeUnit
            LocalMailServiceTestConfig
            LocalDatastoreServiceTestConfig
            LocalUserServiceTestConfig]
           [com.google.appengine.api.datastore
            EntityNotFoundException]
           [com.google.apphosting.api ApiProxy])
  (:require [migae.datastore :as ds]
            [clojure.test :refer :all]
            [clojure.tools.logging :as log :only [trace debug info]]))

(defmacro should-fail [body]
  `(let [report-type# (atom nil)]
     (binding [clojure.test/report #(reset! report-type# (:type %))]
       ~body)
     (testing "should fail"
       (is (= @report-type# :fail )))))

;  (:require [migae.migae-datastore.PersistentEntityMap])
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
        ;; (ds/init)
        (test-fn)
        (.tearDown helper))))
        ;; (ApiProxy/setEnvironmentForCurrentThread environment)
        ;; (ApiProxy/setDelegate delegate))))

;(use-fixtures :once (fn [test-fn] (dss/get-datastore-service) (test-fn)))
(use-fixtures :each ds-fixture)

(deftest ^:query q1
  (testing "ds query"
    (let [em1 (ds/entity-map! :force [:Foo/Bar] {:a 1})
          em2 (ds/entity-map! :force [:Foo/Bar] {:a 2})
          em3 (ds/entity-map! :force [:Foo/Bar] {:a 3})]

      )))

(deftest ^:query kindless
  (testing "ds kindless queries"
    ;; populate ds with test entities
    (ds/entity-map! :multi [:A] [{:a 1} {:a 2} {:a 3}])
    (ds/entity-map! :force  [:A/A] {:a 1})
    (ds/entity-map! :multi [:A/A :A] [{:a 1} {:a 2} {:a 3}])
    (ds/entity-map! :multi [:A/A :B] [{:a 1} {:a 2} {:a 3}])
    (ds/entity-map! :multi [:A/A :C] [{:a 1} {:a 2} {:a 3}])
    (ds/entity-map! :multi [:B] [{:a 1} {:a 2} {:a 3}])
    (ds/entity-map! :force  [:A/B] {:a 1})
    (ds/entity-map! :multi [:A/B :A] [{:a 1} {:a 2} {:a 3}])
    (ds/entity-map! :multi [:A/B :B] [{:a 1} {:a 2} {:a 3}])
    (ds/entity-map! :multi [:A/B :C] [{:a 1} {:a 2} {:a 3}])
    (ds/entity-map! :multi [:C] [{:a 1} {:a 2} {:a 3}])
    (ds/entity-map! :force  [:A/C] {:a 1})
    (ds/entity-map! :multi [:A/C :A] [{:a 1} {:a 2} {:a 3}])
    (ds/entity-map! :multi [:A/C :B] [{:a 1} {:a 2} {:a 3}])
    (ds/entity-map! :multi [:A/C :C] [{:a 1} {:a 2} {:a 3}])

    (let [ems (ds/entity-map* [])]
      ;; ems: all entities (6)
      (log/trace "ems" ems)
      (doseq [em ems] (log/trace (meta em) em)))

;; FIXME
    ;; (let [ems (ds/entity-map* [] {:migae/gt [:A/B :B/A]})]
    ;; (let [ems (ds/entity-map* [] '(> [:A/B :B/A]))]
    ;;   (log/trace "ems" ems)
    ;;   (doseq [em ems] (log/trace (meta em) em)))

    ;; (let [ems (ds/entity-map* [] '(< [:A/B :B/d1 :C/d2]))]
    ;;   (log/trace "ems" ems)
    ;;   (doseq [em ems] (log/trace (meta em) em)))

    ;; ;; illegal: no filters allowed on kindless queries
    ;; (let [ems2 (ds/entity-map* [] {:a 2})]
    ;;   )
    ))

(deftest ^:query by-kind-1
  (testing "ds query by kind 1"
    (ds/entity-map! [:a] {:x 1})
    (ds/entity-map! [:a/b :a] {:x 2})
    (ds/entity-map! [:a] {:x 2})

    (let [ems (ds/entity-map* [:a] {:x 2})]
      (log/trace "ems" ems)
      )))

(deftest ^:query by-kind
  (testing "ds query by kind"
    ;; populate ds with test entities
    (ds/entity-map! :multi [:A] [{:a 1} {:a 2} {:a 3 :b "foo"}])
    (ds/entity-map! :force  [:A/A] {:a 1})
    (ds/entity-map! :multi [:A/A :A] [{:a 1} {:a 2} {:a 3}])
    (ds/entity-map! :multi [:A/A :B] [{:a 1} {:a 2} {:a 3}])
    (ds/entity-map! :multi [:A/A :C] [{:a 1} {:a 2} {:a 3}])
    (ds/entity-map! :multi [:B] [{:a 1} {:a 2} {:a 3}])
    (ds/entity-map! :force  [:A/B] {:a 1})
    (ds/entity-map! :multi [:A/B :A] [{:a 1} {:a 2} {:a 3}])
    (ds/entity-map! :multi [:A/B :B] [{:a 1} {:a 2} {:a 3}])
    (ds/entity-map! :multi [:A/B :C] [{:a 1} {:a 2} {:a 3}])
    (ds/entity-map! :multi [:C] [{:a 1} {:a 2} {:a 3}])
    (ds/entity-map! :force  [:A/C] {:a 1})
    (ds/entity-map! :multi [:A/C :A] [{:a 1} {:a 2} {:a 3}])
    (ds/entity-map! :multi [:A/C :B] [{:a 1} {:a 2} {:a 3}])
    (ds/entity-map! :multi [:A/C :C] [{:a 1} {:a 2} {:a 3}])

    (let [ems1 (ds/entity-map* [:A])
          ems2 (ds/entity-map* [:A] {:a 2})]
          ;; ems2 (ds/entity-map* [:A] {:a '(= 2)})]
      (log/trace "ems1" ems1)
      (log/trace "ems2" ems2)
      )

;; FIXME    (let [ems1 (ds/entity-map* [:A/B :A] {:a 2})
;;           ems2 (ds/entity-map* [:A/B :B] {:a 2})]
;;           )
    ))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; (deftest ^:query by-ancestor
;;   (testing "query by ancestor"
;;     ;; populate ds with test entities
;;     (ds/entity-map! :force [:A/B]{})
;;     (ds/entity-map! :multi [:A/B :C] [{:a 1} {:a 2} {:a 3}])
;; FIXME     (let [parent (try (ds/entity-map* [:A/B]{})
;;                       (catch EntityNotFoundException e
;;                         (log/trace (.getMessage e))
;;                         (throw e)))
;;           childs (try (ds/entity-map* [:A/B :C])
;;                       (catch EntityNotFoundException e
;;                         (log/trace (.getMessage e))
;;                         nil))
;;           k (key parent) ;; k is a keychain (vector) of keylinks (keywords)
;;           foo (log/trace "k:" k (type k) (class k))
;;           children (try (ds/entity-map* (merge k :C)) ; <- merge keylinks into [] keychain
;;                       (catch EntityNotFoundException e
;;                         (log/trace (.getMessage e))
;;                         nil))
;;           ]
;;       (log/trace "parent" (ds/dump parent))
;;       (log/trace "childs")
;;       (doseq [child childs] (log/trace "child" (ds/dump child)))
;;       (log/trace "children")
;;       (doseq [child children] (log/trace "child" (ds/dump child)))
;;       )))

(deftest ^:query by-property
  (testing "ds property filter query"
    ;; populate ds with test entities
    (ds/entity-map! :multi [:A] [{:a 1 :b 2} {:a 2} {:a 3}])
    (ds/entity-map! :multi [:A] [{:a 1 :b 2}])
    (ds/entity-map! :multi [:A/B :A] [{:a 1} {:a 2} {:a "foo@example.org"}])
    (ds/entity-map! :multi [:X/Y :A] [{:a 1} {:a 2} {:a 3}])
    (ds/entity-map! :multi [:A/C :A] [{:a 1 :b 2} {:a 2} {:a 3}])

    (let [ems (ds/entity-map* [])]
      (log/trace "all ems:")
      (doseq [em ems]
        (log/trace (ds/dump em))))

    ;;  (:: (:a = 1 & :b > 2) | (:a < 5))

;; better: use a reader to mimic function
;;     (ds/entity-map* [:A] {:a #migae/fn (= % 2)})
;; or: (ds/entity-map* [:A] {:a '(= % 2)})

    ;;FIXME:  java.lang.IllegalArgumentException: A collection of values is not allowed.
    #_(let [ems (ds/entity-map* [:A] {:a '(= 2)})]
    ;; (let [ems (ds/entity-map* [:A] (& :a = 1 ;  and
    ;;                               :b = 2))
    ;;       ems2 (ds/entity-map* [:A] (| :a = 1 ; or
    ;;                                :b = 2))]
      (log/trace "ems:")
      (doseq [em ems]
        (log/trace (ds/dump em))))

    #_(let [ems (ds/entity-map* [:A] {:a '(>= 2)})]
      (log/trace "ems:")
      (doseq [em ems]
        (log/trace (ds/dump em))))

    #_(let [ems (ds/entity-map* [:A] {:a '(> 2)})]
      (log/trace "ems:")
      (doseq [em ems]
        (log/trace (ds/dump em))))

    #_(let [m {:a "foo@example.org"}
          ems (ds/entity-map* [:A] {:a (list '= (:a m))})]
      (log/trace "ems:")
      (doseq [em ems]
        (log/trace (ds/dump em))))

    ;; default is '=;  {:a "foo@example.org"} as predicate
    (let [ems (ds/entity-map* [:A] {:a "foo@example.org"})]
      (log/trace "ems:")
      (doseq [em ems]
        (log/trace (ds/dump em))))
    ))

;; (deftest ^:query by-key
;;   (testing "entitymap get by key")
;;   (let [em1 (ds/entity-map! :force [:Species/Felis_catus] {:nm "Chibi"})
;; FIXME        em2 (ds/entity-map?? [:Species/Felis_catus])
;;         ;; em3 (ds/entity-map?? :Species/Felis_catus)
;;         em4 (ds/entity-map?? [(keyword "Species" "Felis_catus")])]
;;     (log/trace "em1 " (ds/dump em1))
;;     (log/trace "key em1 " (key em1))
;;     (log/trace "em2 " em2 (type em1))
;;     ;; (log/trace "em3 " em3)
;;     (log/trace "em4 " em4)

;;     ;; FIXME:
;;     ;; (is (= (ds/key em1) (ds/key getit) (ds/key em4))) ; (ds/key em3)
;;     (is (= (type em1) migae.datastore.PersistentEntityMap))
;;     (is (= (try (ds/entity-map?? [(keyword "Group" "foo")])
;;                       (catch EntityNotFoundException e
;;                         (log/trace "Exception:" (.getMessage e))
;;                         EntityNotFoundException))
;;            EntityNotFoundException))
;;     (is (= (try (ds/entity-map?? [:A])
;;                       (catch IllegalArgumentException e
;;                         (log/trace "Exception:" (.getMessage e))
;;                         (.getClass e)))
;;            IllegalArgumentException))
;;     (is (= (try (ds/entity-map?? [:A/B :C])
;;                       (catch IllegalArgumentException e
;;                         (log/trace "Exception:" (.getMessage e))
;;                         (.getClass e)))
;;            IllegalArgumentException))
;;     (is (= (try (ds/entity-map?? [:A/B 9])
;;                       (catch IllegalArgumentException e
;;                         (log/trace "Exception:" (.getMessage e))
;;                         (.getClass e)))
;;            IllegalArgumentException))
;;     (is (= (try (ds/entity-map?? [:A/B 'C/D])
;;                       (catch IllegalArgumentException e
;;                         (log/trace "Exception:" (.getMessage e))
;;                         (.getClass e)))
;;            IllegalArgumentException))
;;     (is (= (try (ds/entity-map?? [:A/B "C/D"])
;;                       (catch IllegalArgumentException e
;;                         (log/trace "Exception:" (.getMessage e))
;;                         (.getClass e)))
;;            IllegalArgumentException))
;;     ))

;; ################################################################
;; (deftest ^:query emaps-q
;;   (testing "entity-map* 1"
;;     (let [em1 (ds/entity-map! :force [:Group] {:name "Acme"})
;; FIXME          em2 (ds/entity-map! :force [:Group] (fn [e] (assoc e :name "Tekstra")))
;;           ems (ds/entity-map* [:Group])]
;;       (log/trace "ems " ems)
;;       (log/trace "ems type " (type ems))
;;       (log/trace (format "(seq? ems) %s\n" (seq? ems)))
;;       [(doseq [e ems]
;;          (do
;;            (log/trace (format "e: %s" e))
;;            (log/trace "(meta e): " (meta e))
;;            (log/trace "(type e): " (type e))
;;            (log/trace "(.content e): " (.content e))
;;            {:id (ds/id e) :name (e :name)}))]
;;       )))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; (deftest ^:query ancestor-path
;;   (testing "entity-map* ancestor query"
;;     (let [e1 (ds/entity-map! :force [:Family/Felidae :Subfamily/Felinae :Genus/Felis :Species/Felis_catus]{})
;;           f1 (ds/entity-map?? [:Family/Felidae :Subfamily/Felinae :Genus/Felis :Species/Felis_catus]{})
;;           ;; f2 (ds/entity-map?? [:Species/Felis_catus])
;;           e2 (ds/entity-map! :force [:Species/Felis_catus]{})]
;;       (log/trace "e1 " e1)
;;       (log/trace "f1 " f1)
;;       (log/trace "e2 " e2)
;;       ;; (log/trace "f2 " f2)
;;       )))

(deftest ^:query ancestor-query
  (testing "entity-map* ancestor query"
    (let [acme (ds/entity-map! :force [:Group] {:name "Acme"})
          k (ds/keychain acme)        ; FIXME
          _ (log/trace "ancestor-query key: " k)
          _ (flush)
          ;; id (ds/id k)
          joe (ds/entity-map! :force (merge k :Member/Joe) {:fname "Joe" :lname "Cool"})
          ;; joeq  (ds/entity-map* [:Group/Acme :Member/Joe])
          ;; joev  (ds/entity-map* :Member/Joe)
;          jane (ds/entity-map! :force [k :Member/Jane] {:fname "Jane" :lname "Hip"})
          frank (ds/entity-map! :force [:Member/Frank] {:fname "Frank" :lname "Funk"})
;; FIXME          root (ds/entity-map* k {})
          members (ds/entity-map* [:Member])
          membersV (ds/entity-map* [:Member])
;; FIXME          members-acme (ds/entity-map* (merge k :Member))
          ] ; ancestor query
      ;; (log/trace "root: " root)
      ;; (log/trace "all members: " members)
      ;; (log/trace "all membersV: " membersV)
;; FIXME      (log/trace "acme members: " members-acme)
      (log/trace "joe " joe)
      ;; (log/trace "joeq " joeq)
      ;; (log/trace "joev " joev)
      ;; (is (=  (ds/entity-map* :Group/Acme)  (ds/entity-map* [:Group/Acme])))
      ;; (is (ds/keys=  (first (ds/entity-map* [:Group/Acme :Member/Joe])) joe))
      (log/trace "FOO" (ds/entity-map* [:Member]))
      (is (=  (count (ds/entity-map* [:Member])) 2))
;; FIXME      (log/trace ":Group" (ds/entity-map* (merge k :Member)))
;; FIXME      (is (=  (count (ds/entity-map* (merge k :Member))) 1))
      )))

;; ################################################################

;; (deftest ^:query query-entities-1
;;   (testing "query 1"
;;     (let [q (dsqry/entities)]
;;       (log/trace "query entities 1" q)
;;       )))

;; (deftest ^:query query-entities-2
;;   (testing "query 2"
;;     (let [q (dsqry/entities :kind :Employee)]
;;       (log/trace "query entities 2" q)
;;       )))


;; ;; (deftest ^:query query-ancestors-1
;; ;;   (testing "query ancestors-1"
;; ;;     (let [k (dskey/make "foo" "bar")
;; ;;           q (dsqry/ancestors :key k)]
;; ;;       (log/trace "query ancestors-1:" q)
;; ;;       )))

;; (deftest ^:query query-ancestors-2
;;   (testing "query ancestors-2"
;;     (let [q (dsqry/ancestors :kind "foo" :name "bar")]
;;       (log/trace "query ancestors-2:" q)
;;       )))

;; (deftest ^:query query-ancestors-3
;;   (testing "query ancestors-3"
;;     (let [q (dsqry/ancestors :kind "foo" :id 99)]
;;       (log/trace "query ancestors-3:" q)
;;       )))

;; (deftest ^:query query-ancestors-3
;;   (testing "query ancestors-3"
;;     (let [q (dsqry/ancestors :kind "foo" :id 99)]
;;       (log/trace "query ancestors-3:" q)
;;       )))



