(ns Interfaces)

(clojure.core/println "loading Interfaces")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;  IPersistentEntityKeychain
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; PersistentVector extends APersistentVector implements IObj, IEditableCollection, IReduce, IKVReduce
;;     public static class Node implements Serializable {
;; APersistentVector extends AFn implements IPersistentVector, Iterable,
;;                                                                List, IMapEntry,
;;                                                                RandomAccess, Comparable,
;;                                                                Serializable, IHashEq {

(gen-interface
 :name migae.datastore.IPersistentEntityKeychain
 :extends [java.io.Serializable
           java.lang.Comparable
           java.lang.Iterable
           java.util.List
           java.util.Map$Entry
           java.util.RandomAccess ;; marker interface

           clojure.lang.IPersistentVector ;; length, assocN, cons
           ;; extends Associative, Sequential, IPersistentStack, Reversible, Indexed
           ;;     Associative (containsKey, entryAt, assoc) extends IPersistentCollection, ILookup
           ;;     IPersistentStack (peek, pop) extends IPersistentCollection
           ;;     IPersistentCollection (count, cons, empty, equiv) extends Seqable (seq)

           ;; clojure.lang.Associative     ;; containsKey, entryAt, assoc
           clojure.lang.IHashEq ;; int hasheq()
           clojure.lang.Indexed ;; nth; extends Counted count
           clojure.lang.IndexedSeq  ;; extends ISeq, Sequential, Counted
           clojure.lang.IEditableCollection ;; ITransientCollection asTransient();
           clojure.lang.IFn                 ;; invoke; extends Callable, Runnable
           clojure.lang.IKVReduce  ;; kvreduce
           ;; clojure.lang.ILookup    ;; valAt
           clojure.lang.IMapEntry  ;; key, val; extends Map.Entry
           clojure.lang.IObj       ;; extends IMeta
           ;; clojure.lang.IPersistentCollection ;; count, cons, empty, equiv
           ;; clojure.lang.IPersistentStack ;; peek, pop
           clojure.lang.IReduce    ;; Object reduce(IFn f) extends IReduceInit
           clojure.lang.IReduceInit  ;;  Object reduce(IFn f, Object start)
           clojure.lang.IReference ; extends IMeta; required to support metadata reader syntax?
           clojure.lang.Reversible ;; ISeq rseq() ;
           clojure.lang.ISeq ;; first, next, more, cons extends IPersistentCollection
           ;; clojure.lang.Seqable         ;; ISeq seq();
           ;; clojure.lang.Seqential ; marker interface
           clojure.lang.ITransientCollection]
 ;; :methods []
 ) ;; end gen-interface IPersistentEntityMap

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;  IPersistentEntityMap
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; PersistentArrayMap APersistentMap, IEditableCollection, IObj, IMapIterable, IKVReduce
;; PersistentHashMap APersistentMap, IEditableCollection, IObj, IMapIterable, IKVReduce
;; PersistentStructMap APersistentMap, IObj
;; PersistentTreeMap APersistentMap, IObj, Reversible, Sorted, IKVReduce

;; APersistenMap
;;     AFn
;;         IFn
;;             java.lang.Runnable
;;             java.util.concurrent.Callable
;;     IPersistentMap
;;         Associative
;;             IPersistentCollection
;;                 Seqable    ; ISeq seq()
;;             ILookup
;;         Counted
;;         java.lang.Iterable
;;     java.lang.Iterable
;;     java.util.Map
;;     java.io.Serializable
;;     MapEquivalence  ;; marker interface
;;     IHashEq
;; IEditableCollection   ;; ITransientCollection asTransient();
;; IObj
;;     IMeta
;; IMapIterable
;; IKVReduce

;; ATransientMap
;;     AFn
;;     ITransientMap
;;         ITransientAssociative
;;             ITransientCollection
;;             ILookup
;;         Counted

(gen-interface
 :name migae.datastore.IPersistentEntityMap
 :extends [java.lang.Iterable
           java.util.Map
           java.io.Serializable

           clojure.lang.IFn             ; AFn: call(), run(), invoke()
           clojure.lang.IPersistentMap
           clojure.lang.IHashEq
           clojure.lang.IKVReduce       ; support clojure.core/reduce-kv
           clojure.lang.MapEquivalence
           clojure.lang.IObj ;; extends IMeta

           ;; clojure maps support transient; dunno if we need to
           ;; clojure.lang.IEditableCollection
           ;; Not sure if we really need this:
           clojure.lang.ITransientCollection

           ;; do we need IReduce?  clojure.core/seq will call seq on our map, no?
           ;; clojure.lang.IReduce  ; extends IReduceInit  reduce(), reduce(IFn f)

           ;; do we need IReference to support alterMeta, resetMeta?
           ;; clojure.lang.IReference ; extends IMeta; required to support metadata reader syntax?

           ]
 ;; :methods []
 ) ;; end gen-interface IPersistentEntityMap


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;  IPersistentEntityMapSeq
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(gen-interface
 :name migae.datastore.IPersistentEntityMapSeq

;; ASeq extends Obj implements ISeq, Sequential, List, Serializable, IHashEq {
 ;; LazySeq extends Obj implements ISeq, Sequential, List, IPending, IHashEq{


 :extends [clojure.lang.Indexed
           clojure.lang.IReduce ; support into
           clojure.lang.ISeq
           java.util.Iterator]
 ;; :methods []
 ) ;; end gen-interface IPersistentEntityMapSeq

;; java.lang.Iterable
    ;; Iterator<T>	iterator()
    ;; java 8: void forEach(Consumer<? super T> action)
    ;; java 8: default Spliterator<T>	spliterator()

;; java.io.Serializable (no methods or fields)

;; Seqable (extends nothing)
;; IPersistentCollection extends Seqable
;; ILookup (extends nothing)
;; Counted (extends nothing)
;; Associative extends IPersistentCollection, ILookup
;; IPersistentMap extends Iterable, Associative, Counted

;; AFn implements IFn
;; IHashEq (extends nothing):  int hasheq()
;; MapEquivalence (extends nothing, no methods)
;; APersistentMap extends AFn
;;     implements IPersistentMap, Map, Iterable, Serializable, MapEquivalence, IHashEq {

;; PersistentArrayMap extends APersistentMap implements IObj, IEditableCollection, IMapIterable, IKVReduce{
;; PersistentHashMap extends APersistentMap implements IEditableCollection, IObj, IMapIterable, IKVReduce {


