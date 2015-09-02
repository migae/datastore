(in-ns 'migae.datastore
  ;; (:refer-clojure :exclude [name hash key])
  ;; (:import [com.google.appengine.api.datastore])
  ;;           ;; Entity
  ;;           ;; Key])
  ;; (:require [clojure.tools.logging :as log :only [trace debug info]]
  ;;           [migae.datastore.emap :as em))
)

;; (load "datastore/emap")
;; (load "datastore/keychain")

(defn- get-emap                            ;FIXME: use a multimethod?
  "return matching emaps"
  [keylinks & filter-map]  ; keychain and predicate-map
  ;; {:pre []} ;; check types
  (log/trace "get-emap keylinks" keylinks (type keylinks))
  (log/trace "get-emap filter-map" filter-map (type filter-map))
;; )
  (if (clj/empty? keylinks)
    (do
      (log/trace "get-emap predicate-map filter" filter-map (type filter-map))
      )
      ;; (let [ks (clj/keylinks filter-map)
      ;;       vs (vals filter-map)
      ;;       k  (subs (str (first ks)) 1)
      ;;       v (last vs)
      ;;       f (Query$FilterPredicate. k Query$FilterOperator/EQUAL v)]
      ;;   (log/trace (format "key: %s, val: %s" k v))))
    (let [k (if (coll? keylinks)
              (apply keychain-to-key keylinks)
              (apply keychain-to-key [keylinks]))
          ;; foo (log/trace "get-emap kw keylinks: " k)
          e (try (.get (ds/datastore) k)
                 (catch EntityNotFoundException e (throw e))
                 (catch DatastoreFailureException e (throw e))
                 (catch java.lang.IllegalArgumentException e (throw e)))]
      (EntityMap. e))))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(deftype DatastoreMap [ds]

  clojure.lang.Associative
  (containsKey [_ k]
    (log/trace "DatastoreMap containsKey " k)
    )
  (entryAt [this k]
    (log/trace "DatastoreMap entryAt " k)
    )

  clojure.lang.IFn
  (invoke [_ k]
    {:pre [(keyword? k)]}
    (log/trace "IFn invoke" k)
    (get-emap k))

  clojure.lang.ILookup
  (valAt [_ k]
    ;; (log/trace "valAt " k)
    (get-emap k))
  (valAt [_ k not-found]
    (log/trace "valAt w/notfound: " k)
    )

  clojure.lang.Seqable
  (seq [this]
    ;; seq is called by: into, merge, "print", e.g. (log/trace em)
    (log/trace "DatastoreMap seq")

    ;; (let [props (.getProperties entity)
    ;;       kprops (clj/into {}
    ;;                        (for [[k v] props]
    ;;                          (do
    ;;                            ;; (log/trace "v: " v)
    ;;                            (let [prop (keyword k)
    ;;                                  val (get-val-clj v)]
    ;;                              ;; (log/trace "prop " prop " val " val)
    ;;                              {prop val}))))
    ;;       res (clj/seq kprops)]
    ;;   ;; (log/trace "seq result:" entity " -> " res)
    ;;   (flush)
    ;;   res))
    )
  ) ;; deftype DatastoreMap

;; (defonce ^{:dynamic true} DSMap (atom nil))

;; (defn init []
;;   (when (nil? @DSMap)
;;     (do
;;         (reset! DSMap (ds/datastoreMap. *datastore-service*))
;;         ))
;;   @DSMap)

