(in-ns 'migae.datastore)
;;   (:refer-clojure :exclude [ancestors count hash keys name])
;;   (:import [com.google.appengine.api.datastore
;;             Entity
;;             Query
;;             Query$Filter
;;             Query$FilterPredicate
;;             Query$FilterOperator
;;             Query$CompositeFilter
;;             Query$CompositeFilterOperator
;;             Query$SortDirection
;;             FetchOptions
;;             FetchOptions$Builder
;;             PreparedQuery
;;             KeyFactory
;;             Key
;;             DatastoreService
;;             DatastoreServiceFactory])
;;            ;; [org.apache.log4j PatternLayout FileAppender
;;            ;;  EnhancedPatternLayout])
;; ;; [clj-logging-config.log4j])
;;   (:require [migae.datastore :as ds]
;;             ;; [migae.datastore.entity :as dsentity]
;;             ;; [migae.datastore.key :as dskey]
;;             [clojure.tools.logging :as log :only [trace debug info]]))

;; client:  (:require [migae.datastore.query :as dsqry]...

;; keys-only query
;;   Query q = new Query("Person").setKeysOnly();
(defn pull-all
  []
  ;; (log/trace "pull-all")
  (let [q  (Query.)
        prepared-query (.prepare (.content store-map) q) ;; FIXME
        iterator (.asIterator prepared-query)
        res (PersistentEntityMapSeq. (iterator-seq iterator))
        ]
    ;; (log/trace "iter res: " (type res) " count:" (count res))
    res))

(defn keys-only [kind]
  (let [q  (Query. (clojure.core/name kind))
        foo (log/debug "keys-only q: " q)
        qq (.setKeysOnly q)
        bar (log/debug "after setKeysOnly: " qq)]
    qq))

;; ################
(defn entity-count [preppedqry]
  (.countEntities preppedqry (FetchOptions$Builder/withDefaults)))

;; ################
(defmulti entities
  (fn [& {kind :kind key :key name :name id :id}]
    (cond
     name :kindname
     id   :kindid
     kind :kind
     key  :key
     ;; (= (type s) java.lang.String) :kind
     ;; (= (type rest) com.google.appengine.api.datastore.Key) :key
     :else :kindless)))

(defmethod entities :kindless
  [& {kind :kind key :key name :name id :id}]
  (Query.)
  )

(defmethod entities :kind
  [& {kind :kind key :key name :name id :id}]
  (Query. (clojure.core/name kind))
  )

(defn predicate
  [property]
  )

(defn prepare
  [query]
  (.prepare store-map query))

(defn run
  [prepared-query]
  (do (log/debug "run query " prepared-query)
      ;; (.asList prepared-query (FetchOptions$Builder/withDefaults))))
      (iterator-seq (.asIterator prepared-query))))

;; use ds/fetch????
;; (defn fetch
;;   [{kind :_kind}]
;;   (let [;; q  (.setKeysOnly (Query. "Employee"))
;;         q (keys kind)
;;         pq (prepare q)
;;         ;; l (.asList pq (FetchOptions$Builder/withDefaults))
;;         ;; lit (iterator-seq l)
;;         keys (.asIterator pq)
;;         kit (iterator-seq keys)]
;;     (do
;;       (log/trace (format "query: %s" (type q)))
;;       (log/trace (format "prepped query: %s" (type pq)))
;;       ;; (log/trace (format "result list: %s" lit))
;;       ;; (doseq [key lit]
;;       ;;   (log/trace (format "list item: %s" key)))
;;       (log/trace (format "result seq: %s" kit))
;;       (doseq [key kit]
;;         (log/trace (format "seq item: %s" key))))))


;; ################################################################

;; Filter heightMinFilter =
;;   new FilterPredicate("height",
;;                       FilterOperator.GREATER_THAN_OR_EQUAL,
;;                       minHeight);

;; (def heightMinFilter (dsqry/predicate :prop :height :ge minheight))
;; (def heightMaxFilter (dsqry/predicate :prop :height :ge maxheight))

;; //Use CompositeFilter to combine multiple filters
;; Filter heightRangeFilter =
;;   CompositeFilterOperator.and(heightMinFilter, heightMaxFilter);

;; (dsqry/comp heightMinFilter :and heightMaxFilter)

;; // Use class Query to assemble a query
;; Query q = new Query("Person").setFilter(heightRangeFilter);

;; (-> (dsqry/entities :kind :Person)
;;     (dsqry/filter heightRangeFilter))
;; or
;; (-> (dsqry/entities :kind :Person)
;;     (dsqry/filters heightMinFilter :and heightMaxFilter))

;; (-> (dsqry/entities :kind :Person)
     ;; (dsqry/filters {:prop "height" :op :ge :val minHeight})
     ;;                {:prop "height" :op :le :val maxHeight}))

;; Or write a macro so we can use >, <, etc. e.g.
;;    :height >= minHeight :and :height <= maxHeight

;; i.e. we want some kind of higher-order function combinator


;; filter operators:

;; Query.FilterOperator.EQUAL
;; Query.FilterOperator.GREATER_THAN
;; Query.FilterOperator.GREATER_THAN_OR_EQUAL
;; Query.FilterOperator.IN
;; Query.FilterOperator.LESS_THAN
;; Query.FilterOperator.LESS_THAN_OR_EQUAL
;; Query.FilterOperator.NOT_EQUAL
