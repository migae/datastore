(in-ns 'migae.datastore)

;; (clojure.core/println "loading PersistentEntityMapSeq")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(deftype PersistentEntityMapSeq [query]

  migae.datastore.IPersistentEntityMapSeq
  ;; clojure.lang.ISeq ;; < IPersistentCollection (< Seqable)
  (^Object
    first [_]
    (log/trace "PersistentEntityMapSeq.ISeq.first of" (type query))
    (let [r  (first query)
          rm (->PersistentEntityMap r nil)]
      ;; rm (migae.datastore.PersistentEntityMap. r nil)]
      ;; (log/trace "rm:" rm)
      rm))
  (^ISeq
    next [_]
    (log/trace "ISeq next")
    (let [res (next query)]
      (if (nil? res)
        nil
        (PersistentEntityMapSeq. res))))
  (^ISeq
    more [_]  ;;  same as next?
    (log/trace "PersistentEntityMapSeq.ISeq.more")
    (let [res (next query)]
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
    (count query))
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
  ;;   (count query))
  (nth [this i]                         ; called by get(int index)
    (log/trace "Indexed nth" i))
  ;; (next em-iter)) ;; HACK
  (nth [this i not-found]
    (log/trace "Indexed nth with not-found" i))
  )

