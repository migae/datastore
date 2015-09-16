(in-ns 'migae.datastore)

;; (clojure.core/println "loading PersistentEntityMapSeq")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(deftype PersistentEntityMapSeq [content]

  migae.datastore.IPersistentEntityMapSeq

  ;;;;;;;;;;;;;;;; java.util.Iterator
  (^boolean
    hasNext [this]
    (log/debug "PersistentEntityMapSeq.hasNext" (class content))
    (> (.size (.toArray content) 0)))
  ;; (next [this]
  ;;   (log/debug "PersistentEntityMapSeq.next"))
  (remove [this]
    (log/debug "PersistentEntityMapSeq.remove"))


  ;; clojure.lang.ISeq ;; < IPersistentCollection (< Seqable)
  (^Object
    first [_]
    (log/trace "PersistentEntityMapSeq.ISeq.first of" (type content))
    (let [r  (first content)
          rm (->PersistentEntityMap r nil)]
      ;; rm (migae.datastore.PersistentEntityMap. r nil)]
      ;; (log/trace "rm:" rm)
      rm))
  (^ISeq
    next [_]
    (log/trace "ISeq next")
    (let [res (next content)]
      (if (nil? res)
        nil
        (PersistentEntityMapSeq. res))))
  (^ISeq
    more [_]  ;;  same as next?
    (log/trace "PersistentEntityMapSeq.ISeq.more")
    (let [res (next content)]
      (if (nil? res)
        nil
        (PersistentEntityMapSeq. res))))
  (^ISeq ;;^clojure.lang.IPersistentVector
    cons  ;; -> ^ISeq ;;
    [this ^Object obj]
    (log/trace "ISeq cons"))

  ;;;; Seqable interface
  (^ISeq
    seq [this]  ; specified in Seqable
     (log/trace "PersistentEntityMapSeq.ISeq.seq")
    this)

  ;;;; IPersistentCollection interface
  (^int
    count [_]
    ;; (log/trace "PersistentEntityMapSeq.count")
    (count content))
  ;; cons - overridden by ISeq
  (^IPersistentCollection
    empty [_]
    (log/trace "PersistentEntityMapSeq.empty"))
  (^boolean
    equiv [_ ^Object obj]
    (log/trace "PersistentEntityMapSeq.equiv"))

  ;; clojure.lang.IndexedSeq extends ISeq, Sequential, Counted{
  ;;public int index();

  ;; clojure.lang.Indexed                  ; extends Counted
  ;; (count [this]                         ; Counted
  ;;   (log/trace "PersistentEntityMapSeq.clojure.lang.Indexed.count")
  ;;   (count content))
  (nth [this i]                         ; called by get(int index)
    (log/trace "Indexed nth" i))
  ;; (next em-iter)) ;; HACK
  (nth [this i not-found]
    (log/trace "Indexed nth with not-found" i))

  (reduce [this ^IFn f]  ; -> Object
    (log/debug "PersistentEntityMapSeq.HELP! reduce") (flush)
    this)
  (reduce [this ^IFn f ^Object to-map]  ; -> Object
    ;; called by "print" stuff
    (log/debug "PersistentEntityMapSeq.reduce func:" f)
    (log/debug "PersistentEntityMapSeq.reduce to:" (class to-map) (type to-map))
    (cond
      (= (class to-map) clojure.lang.PersistentArrayMap$TransientArrayMap)
      (do
        ;; apply f to this and first elt of to-map, etc
;;        (log/debug "PersistentEntityMapSeq.reduce to-map first: " (first (persistent! to-map)))
        (map
         #(log/debug "PersistentEntityMapSeq.reduce to-map item: " %)
         to-map)
        to-map)
      (= (class to-map) migae.datastore.PersistentStoreMap)
      (do
        (let [ds (.content to-map)]
          ;; (log/debug "PersistentEntityMapSeq.reduce ds: " ds)
          (.put ds content)
          to-map))
      (= (class to-map) com.google.appengine.api.datastore.DatastoreServiceImpl)
      (do
        (.put to-map content)
        to-map)
      (= (class to-map) clojure.lang.PersistentArrayMap)
      (do
        (log/debug "to-map:  PersistentArrayMap")
        )
      (= (class to-map) clojure.lang.PersistentVector)
      (do
        (log/debug "to-map:  clojure.lang.PersistentVector")
        )
      (= (class to-map) clojure.lang.PersistentArrayMap$TransientArrayMap)
      (do
        (log/debug "to-map:  PersistentArrayMap$TransientArrayMap")
      :else (log/debug "PersistentEntityMapSeq.HELP! reduce!" (class to-map)))
      ))

  )


