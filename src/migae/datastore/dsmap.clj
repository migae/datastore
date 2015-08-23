(in-ns 'migae.datastore)
  ;; (:refer-clojure :exclude [name hash key])
  ;; (:import [com.google.appengine.api.datastore])
  ;;           ;; Entity
  ;;           ;; Key])
  ;; (:require [clojure.tools.logging :as log :only [trace debug info]]
  ;;           [migae.datastore.emap :as em))

(load "datastore/emap")

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
    (emap?? k))

  clojure.lang.ILookup
  (valAt [_ k]
    ;; (log/trace "valAt " k)
    (emap?? k))
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

(defonce ^{:dynamic true} DSMap (atom nil))

(defn init []
  (when (nil? @DSMap)
    (do
        (reset! DSMap (DatastoreMap. *datastore-service*))
        ))
  @DSMap)

