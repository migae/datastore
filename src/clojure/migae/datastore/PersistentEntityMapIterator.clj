(in-ns 'migae.datastore)

(declare ->PersistentEntityMap)

(deftype PersistentEntityMapIterator [query]
  ;; public class IteratorSeq extends ASeq{
  ;; public abstract class ASeq extends Obj implements ISeq, Sequential, List, Serializable, IHashEq {
  ;; ISeq, Sequential, List, Serializable, IHashEq
  ;; public class SeqIterator implements Iterator{
  ;; java.util.List
  ;; Cursor getCursor()
  ;; java.util.List<Index>	getIndexList()

  ;; clojure.lang.AMapEntry ; extends APersistentVector implements IMapEntry, extends Map.Entry
  ;; clojure.lang.APersistentVector ; extends AFn
  ;;     implements IPersistentVector, Iterable, List, RandomAccess, Comparable, Serializable, IHashEq

  ;; clojure.lang.IFn ;extends Callable, Runnable

  ;; clojure.lang.IMapEntry
  ;; Object key();
  ;; Object val();

  ;; java.util.Map$Entry
  ;; boolean	equals(Object o)
  ;; K	getKey()
  ;; V	getValue()
  ;; int	hashCode()
  ;; V	setValue(V value)

  java.lang.Iterable
  (iterator [this]
    (log/trace "Iterable iterator" (.content this)))
    ;; (let [props (.getProperties content) ;; java.util.Map<java.lang.String,java.lang.Object>
    ;;       entry-set (.entrySet props)
    ;;       e-iter (.iterator entry-set)
    ;;       em-iter (PersistentEntityMapIterator. e-iter) ]
    ;; ;; (log/trace "Iterable res:" em-iter)
    ;; em-iter))

  java.util.Iterator
  (hasNext [this]
    (do
      (log/trace "PersistentEntityMapIterator Iterator hasNext")))
      ;; (.hasNext query)))
  ;; (next    [this]                       ;
  ;;   (do
  ;;     (log/trace "PersistentEntityMapIterator Iterator next")))
  ;;     ;; (let [r (get-next-emap-prop this)
  ;;     ;;       k (.getKey r)
  ;;     ;;       v (.getValue r)
  ;;     ;;       res {(keyword k) v}]
  ;;     ;;   (log/trace "emap-iter next" res)
  ;;     ;;   res)))

  clojure.lang.ISeq ;; extends IPersistentCollection, extends Seqable
  ;;;; Seqable interface
  (seq [this]  ; specified in Seqable
    (log/trace "PersistentEntityMapIterator.ISeq.seq")
    this)
  ;;;; IPersistentCollection interface
  ;;int count();
  (count [_]
    ;; (log/trace "ISeq count")
    (count query))
  ;;IPersistentCollection IPersistentCollection.cons(Object o) - overridden by ISeq.cons
  ;;IPersistentCollection IPersistentCollection.empty()
  (empty [_] (log/trace "ISeq empty"))
  ;;boolean IPersistentCollection.equiv(Object o);
  (equiv [_ obj] (log/trace "ISeq equiv"))
  ;;;; ISeq interface
  ;;Object first();
  (first [_]
    (log/trace "ISeq first of" (type query))
    (->PersistentEntityMap (first query)))
  ;;ISeq next();
  (next [_]
    (let [res (next query)]
      (log/trace "ISeq next" (type res))
      (if (nil? res)
        nil
        (PersistentEntityMapIterator. res))))
  ;;ISeq more();
  (more [_] (log/trace "ISeq more"))
  ;;ISeq cons(Object o);
  (cons [_ obj] (log/trace "ISeq cons"))
  ;; clojure.lang.IPersistentCollection
  ;;int count()

  clojure.lang.IPersistentVector ; extends Associative, Sequential, IPersistentStack, Reversible, Indexed
  ;; int length();
  (length [_]
    (log/trace "IPersistentVector length"))
  ;; IPersistentVector assocN(int i, Object val);
  (assocN [_ i val]
    (log/trace "IPersistentVector assocN"))
  ;; IPersistentVector cons(Object o);
  ;; (cons [_ obj]
  ;;   (log/trace "IPersistentVector cons"))

  ;; clojure.lang.IPersistentStack ; extends IPersistentCollection
  ;; Object peek();
  (peek [_]
    (log/trace "IPersistentStack peek"))
  ;; IPersistentStack pop();
  (pop [_]
    (log/trace "IPersistentStack pop"))

  ;; clojure.lang.IPersistentCollection
  ;; ;;int count()
  ;; (count [_] (log/trace "IPersistentCollection count"))
  ;;IPersistentCollection cons(Object o);
  ;; (cons [_ obj] (log/trace "IPersistentCollection cons"))
  ;;IPersistentCollection empty();
  ;; (empty [_] (log/trace "IPersistentCollection empty"))
  ;; ;;boolean equiv(Object o);
  ;; (equiv [_ obj] (log/trace "IPersistentCollection equiv"))

  ;; from clojure.lang.Seqable
    ;; (let [props (.getProperties content)
    ;;       kprops (clj/into {}
    ;;                        (for [[k v] props]
    ;;                          (do
    ;;                          ;; (log/trace "v: " v)
    ;;                          (let [prop (keyword k)
    ;;                                val (get-val-clj v)]
    ;;                            ;; (log/trace "prop " prop " val " val)
    ;;                            {prop val}))))
    ;;       res (clj/seq kprops)]
    ;;   ;; (log/trace "seq result:" content " -> " res)
    ;;   (flush)
    ;;   res))

  ;; clojure.lang.IndexedSeq extends ISeq, Sequential, Counted{
  ;;public int index();

  clojure.lang.Indexed                  ; extends Counted
  ;; (count [this]                         ; Counted
  ;;   (log/trace "count"))
  (nth [this i]                         ; called by get(int index)
    (log/trace "Indexed nth" i))
  ;; (next em-iter)) ;; HACK
  (nth [this i not-found]
    (log/trace "Indexed nth with not-found" i))

)

