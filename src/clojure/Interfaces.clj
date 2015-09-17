(ns Interfaces)

(clojure.core/println "loading Interfaces")

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


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;  IPersistentEntityMap
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(gen-interface
 :name migae.datastore.IPersistentEntityMap
 :extends [java.io.Serializable
           java.lang.Iterable
           java.util.Map
           ;; void	clear()
           ;; boolean containsKey(Object key)
           ;; boolean containsValue(Object value)
           ;; Set<Map.Entry<K,V>> entrySet()
           ;; boolean equals(Object o)
           ;; V get(Object key)
           ;; int hashCode()
           ;; boolean isEmpty()
           ;; Set<K> keySet()
           ;; V put(K key, V value)
           ;; void putAll(Map<? extends K,? extends V> m)
           ;; V remove(Object key)
           ;; int size()
           ;; Collection<V> values()
           ;;j8 default V compute(K key, BiFunction<? super K,? super V,? extends V> remappingFunction)
           ;;j8 default V computeIfAbsent(K key, Function<? super K,? extends V> mappingFunction)
           ;;j8 default V computeIfPresent(K key, BiFunction<? super K,? super V,? extends V> remappingFunction)
           ;;j8 default void forEach(BiConsumer<? super K,? super V> action)
           ;;j8 default V getOrDefault(Object key, V defaultValue)
           ;;j8 default V merge(K key, V value, BiFunction<? super V,? super V,? extends V> remappingFunction)
           ;;j8 default V putIfAbsent(K key, V value)
           ;;j8 default boolean remove(Object key, Object value)
           ;;j8 default V replace(K key, V value)
           ;;j8 default boolean replace(K key, V oldValue, V newValue)
           ;;j8 default void replaceAll(BiFunction<? super K,? super V,? extends V> function)

           clojure.lang.MapEquivalence

           clojure.lang.Indexed ; extends Counted
           clojure.lang.IFn
           clojure.lang.IHashEq
           clojure.lang.IObj ;; extends IMeta
           clojure.lang.IPersistentMap
           ;; < Iterable, (Associative < (IPersistentCollection < Seqable, ILookup), Counted
           clojure.lang.IReduce  ; extends IReduceInit
           clojure.lang.IReference ; extends IMeta; required to support metadata reader syntax?
           clojure.lang.ITransientCollection]
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
