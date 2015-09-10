(in-ns 'migae.datastore)

;; (declare ->PersistentEntityMap)

(deftype PersistentEntityMapSeq [query]

;;   java.lang.Iterable
;;   (^java.util.Iterator iterator [this]
;;     (log/trace "Iterable iterator")
;;     this)
;;     ;; (let [props (.getProperties content) ;; java.util.Map<java.lang.String,java.lang.Object>
;;     ;;       entry-set (.entrySet props)
;;     ;;       e-iter (.iterator entry-set)
;;     ;;       em-iter (PersistentEntityMapIterator. e-iter) ]
;;     ;; ;; (log/trace "Iterable res:" em-iter)
;;     ;; em-iter))

;;   java.util.Iterator
;;   ;; hasNext, next, remove
;;   ;; Java 8: forEachRemaining
;;   (^boolean hasNext [this]
;;     (do
;; ;;      (log/trace "PersistentEntityMapIterator.hasNext")
;;       (> (count query) 0)))
;;   (next [this] ;-> migae.datastore.IPersistentEntityMap
;;     (do
;;       (log/trace "PersistentEntityMapIterator.next")
;;       (log/trace "content type: " (type query))
;;       ))
;;       ;; (let [r (get-next-emap-prop this)
;;       ;;       k (.getKey r)
;;       ;;       v (.getValue r)
;;       ;;       res {(keyword k) v}]
;;       ;;   (log/trace "emap-iter next" res)
;;       ;;   res)))
;;   (remove    [this] ;-> void
;;     (do
;;       (log/trace "PersistentEntityMapIterator.remove")))

  clojure.lang.ISeq ;; < IPersistentCollection (< Seqable)
  (^Object
    first [_]
    (log/trace "ISeq first of" (type query))
    (let [r  (first query)
          ;; rm (->PersistentEntityMap r nil)]
          rm (migae.datastore.PersistentEntityMap. r nil)]
      ;; (log/trace "rm:" rm)
      rm))
  (^ISeq
    next [_]
    (let [res (next query)]
      (log/trace "ISeq next" (type res))
      (if (nil? res)
        nil
        (PersistentEntityMapIterator. res))))
  (^ISeq
    more [_]  ;;  same as next?
    (log/trace "ISeq more")
    (let [res (next query)]
      (log/trace "ISeq next" (type res))
      (if (nil? res)
        nil
        (PersistentEntityMapIterator. res))))
  (^ISeq ;;^clojure.lang.IPersistentVector
  ;; FIXME!  this compiles with return type ISeq, but when running
  ;; sparky/gae, which uses the lib, we get a "wrong type" msg, it
  ;; wants IPersistentVector.  but if we use that, then compile
  ;; complains that it wants ISeq!!!  So we remove the ISeq interface.
  ;; Maybe we don't want that anyway for an Interator.
    cons  ;; -> ^ISeq ;;
    [this ^Object obj]
    (log/trace "ISeq cons"))

  ;;;; Seqable interface
  (^ISeq
    seq [this]  ; specified in Seqable
    (log/trace "PersistentEntityMapIterator.ISeq.seq")
    this)

  ;;;; IPersistentCollection interface
  (^int
    count [_]
    ;; (log/trace "PersistentEntityMapIterator.count")
    (count query))
  ;; cons - overridden by ISeq
  (^IPersistentCollection
    empty [_]
    (log/trace "PersistentEntityMapIterator.empty"))
  (^boolean
    equiv [_ ^Object obj]
    (log/trace "PersistentEntityMapIterator.equiv"))

  ;; clojure.lang.IndexedSeq extends ISeq, Sequential, Counted{
  ;;public int index();

  clojure.lang.Indexed                  ; extends Counted
  ;; (count [this]                         ; Counted
  ;;   (log/trace "PersistentEntityMapIterator.clojure.lang.Indexed.count")
  ;;   (count query))
  (nth [this i]                         ; called by get(int index)
    (log/trace "Indexed nth" i))
  ;; (next em-iter)) ;; HACK
  (nth [this i not-found]
    (log/trace "Indexed nth with not-found" i))

)

