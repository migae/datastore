;;(clojure.core/println "Start loading migae.datastore.entity_map_seq")

;;(ns migae.datastore.types.entity-map-seq

(in-ns 'migae.datastore)
(import
;; (:refer-clojure :exclude [name hash])
 (java.lang RuntimeException)
 (clojure.lang IFn ILookup IMapEntry IObj
               IPersistentCollection IPersistentMap IReduce IReference ISeq ITransientCollection)
 (com.google.apphosting.api ApiProxy)
 ;;(com.google.appengine.api.datastore
 (com.google.appengine.api.datastore Blob
                                     DatastoreFailureException
                                     DatastoreService
                                     DatastoreServiceFactory
                                     DatastoreServiceConfig
                                     DatastoreServiceConfig$Builder
                                     Email
                                     EntityNotFoundException
                                     Entity EmbeddedEntity EntityNotFoundException
                                     Key KeyFactory KeyFactory$Builder
                                     Query Query$SortDirection)
 ;; (migae.datastore Interfaces)
 )
(clojure.core/refer 'clojure.core)
  ;; (:use [clj-logging-config.log4j])
(require '[clojure.test :refer :all]
         ;; [migae.datastore :as ds]
         ;; [migae.datastore.types.entity-map :as em]
         ;;[migae.datastore.types.store-map :refer :all]
         ;; [migae.datastore.keys :as k]
         '[clojure.tools.logging :as log :only [trace debug info]]
         '[clojure.tools.reader [edn :as edn]])
;;
;;(clojure.core/println "loading migae.datastore.entity_map_seq")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;(in-ns 'migae.datastore)
;; (clojure.core/refer 'clojure.core)
;; (require '(clojure.tools [logging :as log :only [debug info]])
;;          ;; '(migae.datastore.adapter [gae :as gae])
;;          ;; '(migae.datastore [keys :as k])
;;          ;; '(migae.datastore.types [entity-map :as em])
;;          '(clojure.tools.reader [edn :as edn])
;;          )

;; (clojure.core/require '(clojure.tools [logging :as log :only [debug info]]))
;; (clojure.core/require '(migae.datastore.adapter [gae :as gae]))
;; (clojure.core/require '(migae.datastore [keys :as k]))

;;(import '(clojure.lang IPersistentCollection))

(declare ->PersistentEntityMap)
(declare ->PersistentEntityMapSeq)

(deftype PersistentEntityMapSeq [content]

  migae.datastore.IPersistentEntityMapSeq

  ;;Protocol: java.util.Iterator
  (^boolean
    hasNext [this]
    (log/trace "PersistentEntityMapSeq.hasNext" (class content))
    (> (.size (.toArray content) 0)))
  ;; (next [this]
  ;;   (log/trace "PersistentEntityMapSeq.next"))
  (remove [this]
    (log/trace "PersistentEntityMapSeq.remove"))

  ;;Protocol: clojure.lang.ISeq ;; < IPersistentCollection (< Seqable)
  (^Object
    first [_]
    ;; (log/trace "PersistentEntityMapSeq.first")
    (let [r  (first content)
          rm (->PersistentEntityMap r nil)]
      ;; rm (migae.datastore.PersistentEntityMap. r nil)]
      ;; (log/trace "rm:" rm)
      rm))
  (^ISeq
    next [_]
    ;; (log/trace "PersistentEntityMapSeq.next")
    (let [res (next content)]
      (if (nil? res)
        nil
        (PersistentEntityMapSeq. res))))
  (^ISeq
    more [_]  ;;  same as next?
    ;; (log/trace "PersistentEntityMapSeq.more")
    (let [res (next content)]
      (if (nil? res)
        nil
        (PersistentEntityMapSeq. res))))
  (^ISeq ;;^clojure.lang.IPersistentVector
    cons  ;; -> ^ISeq ;;
    [this ^Object obj]
    #_(log/trace "PersistentEntityMapSeq.cons"))

  ;;;; Seqable interface
  (^ISeq
    seq [this]  ; specified in Seqable
    ;; (log/trace "PersistentEntityMapSeq.seq " (.hashCode this))
    this)

  ;;Protocol: IPersistentCollection interface
  (^int
    count [_]
    ;; (log/trace "PersistentEntityMapSeq.count")
    (count content))
  ;; cons - overridden by ISeq
  (^IPersistentCollection
   empty [_]
   #_(log/trace "PersistentEntityMapSeq.empty"))
  (^boolean
    equiv [_ ^Object obj]
    #_(log/trace "PersistentEntityMapSeq.equiv"))

  ;; clojure.lang.IndexedSeq extends ISeq, Sequential, Counted{
  ;;public int index();

  ;; clojure.lang.Indexed                  ; extends Counted
  ;; (count [this]                         ; Counted
  ;;   (log/trace "PersistentEntityMapSeq.clojure.lang.Indexed.count")
  ;;   (count content))
  (nth [this i]                         ; called by get(int index)
    #_(log/trace "Indexed nth" i))
  ;; (next em-iter)) ;; HACK
  (nth [this i not-found]
    #_(log/trace "Indexed nth with not-found" i " nf: " not-found)
    (if-let [r (nth content i)]
      r
      not-found))

  (reduce [this ^IFn f]  ; -> Object
    (log/trace "PersistentEntityMapSeq.HELP! reduce") (flush)
    this)
  (reduce [this ^IFn f ^Object to-map]  ; -> Object
    ;; called by "print" stuff
    #_(log/trace "PersistentEntityMapSeq.reduce func:" f)
    #_(log/trace "PersistentEntityMapSeq.reduce to:" (class to-map) (type to-map))
    (cond
      (= (class to-map) clojure.lang.PersistentArrayMap$TransientArrayMap)
      (do
        ;; apply f to this and first elt of to-map, etc
        #_(log/trace "PersistentEntityMapSeq.reduce to-map first: " (first (persistent! to-map)))
        (map
         #_(log/trace "PersistentEntityMapSeq.reduce to-map item: " %)
         to-map)
        to-map)

      ;; FIXME:  solve cirular ref problem
      ;; (= (class to-map) PersistentStoreMap)
      ;; (do
      ;;   (let [ds (.content to-map)]
      ;;     ;; (log/trace "PersistentEntityMapSeq.reduce ds: " ds)
      ;;     (.put ds content)
      ;;     to-map))

      (= (class to-map) com.google.appengine.api.datastore.DatastoreServiceImpl)
      (do
        (.put to-map content)
        to-map)
      ;; (= (class to-map) clojure.lang.PersistentArrayMap)
      ;; (do
      ;;   (log/trace "to-map:  PersistentArrayMap")
      ;;   )
      ;; (= (class to-map) clojure.lang.PersistentVector)
      ;; (do
      ;;   (log/trace "to-map:  clojure.lang.PersistentVector")
      ;;   )
      ;; (= (class to-map) clojure.lang.PersistentArrayMap$TransientArrayMap)
      ;; (do
      ;;   (log/trace "to-map:  PersistentArrayMap$TransientArrayMap"))
      :else (log/trace "PersistentEntityMapSeq.HELP! reduce!" (class to-map)))
      )
  )


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; (in-ns 'migae.datastore.types.entity-map-seq)

;;(clojure.core/println "Done loading migae.datastore.types.entity_map_seq")
