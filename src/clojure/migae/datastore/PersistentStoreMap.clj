(in-ns 'migae.datastore)

;; (clojure.core/println "loading PersistentStoreMap")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;  PersistentStoreMap
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(deftype PersistentStoreMap [content txns ds-meta]

  migae.datastore.IPersistentEntityMap
  ;; java.lang.Iterable
  (iterator [this]
         (throw (RuntimeException. "PersistentStoreMap.iterator")))

  ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;  java.util.Map
  ;; void	clear()
  ;; boolean containsKey(Object key) => implemented by Associative, below
  ;; boolean containsValue(Object value)
  ;; Set<Map.Entry<K,V>> entrySet()
  ;; boolean equals(Object o)
  ;; V get(Object key)
  (get [_ k]
    (.get content k))
  (hashCode [_]
         (throw (RuntimeException. "PersistentStoreMap.hashCode")))
  (^boolean isEmpty [_]
         (throw (RuntimeException. "PersistentStoreMap.isEmpty")))
  ;; Set<K> keySet()
  (keySet [_]
         (throw (RuntimeException. "PersistentStoreMap.keySet")))
  ;; V put(K key, V value)
  ;; void putAll(Map<? extends K,? extends V> m)
  ;; V remove(Object key)
  ;; int size()
  (size [_]
         (throw (RuntimeException. "PersistentStoreMap.size")))
  ;; Collection<V> values()
  (values [_]
         (throw (RuntimeException. "PersistentStoreMap.values")))
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

  ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;; clojure.lang.IFn
  ;; invoke, applyTo
  (invoke [this k]  ; -> Object
    {:pre [(keychain? k)]}
         (throw (RuntimeException. "PersistentStoreMap.invoke")))

  ;; Object applyTo(ISeq arglist) ;
  (applyTo [_ ^ISeq arglist]  ; -> Object
         (throw (RuntimeException. "PersistentStoreMap.applyTo")))

  ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;; clojure.lang.IObj ;; extends IMeta
  ;; withMeta; meta
  (^IPersistentMap meta [this]
    nil)
;    {:type PersistentStoreMap})
  (^IObj withMeta [this ^IPersistentMap md]
    (do
      (log/debug "PersistentStoreMap.withMeta" md)
      (PersistentStoreMap. content txns md)))
         ;; (throw (RuntimeException. "PersistentStoreMap.withMeta")))
    ;; (let [em (PersistentStoreMap. (.clone content) nil md]
    ;;   em))

  ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;; clojure.lang.IPersistentMap ; extends Iterable, Associative, Counted
  ;; assoc, assocEx, without; containsKey, entryAt; valAt; cons, count, empty, equiv; seq
  (assoc [this k v] ; -> IPersistentMap
    (do
      (log/debug "PersistentStoreMap.assoc")
      (throw (RuntimeException. "PersistentStoreMap.assoc"))))
  (assocEx [_ k v]
         (throw (RuntimeException. "PersistentStoreMap.assocEx")))
  (without [this k]                     ; = dissoc!, return new datum with k removed
         (throw (RuntimeException. "PersistentStoreMap.without")))
  ;;;; extends Counted
  (count [_]  ; -> int
    "number of entities in datastore"
    (do
      ;; (log/debug "PersistentStoreMap.count")
      ;; DatastoreService datastore = DatastoreServiceFactory.getDatastoreService();
      ;; Entity globalStat = datastore.prepare(new Query("__Stat_Total__")).asSingleEntity();
      (let [q (Query. "__Stat_Total__")
            ;; foo (log/debug "query: " q)
            pq (.prepare content q)
            ;; foo (log/debug "p query: " pq)
            globalStat (.asSingleEntity pq)]
        (if (nil? globalStat)
          (do
            ;; (log/debug "totalEntities: " 0)
            0)
          (let [foo (log/debug "global stat: " globalStat)
                ;; Long totalEntities = (Long) globalStat.getProperty("count");
                totalEntities (.getProperty globalStat "count")]
            ;; (log/debug "totalEntities: " totalEntities)
            totalEntities)))))

  ;;;; extends Associative extends IPersistentCollection, ILookup
  ;; containsKey, entryAt, assoc
  (containsKey [_ k] ; -> boolean
         (throw (RuntimeException. "PersistentStoreMap.countainsKey")))
  (entryAt [this k] ; -> IMapEntry
         (throw (RuntimeException. "PersistentStoreMap.entryAt")))

  ;;;; extends ILookup
  ;; valAt(Object key), valAt(Object key, Object notFound)
  (valAt [_ k]  ; -> Object
         (throw (RuntimeException. "PersistentStoreMap.valAt k")))
  (valAt [_ k not-found]  ; -> Object
         (throw (RuntimeException. "PersistentStoreMap.valAt k not-found")))

  ;;;;  extends IPersistentCollection extends Seqable
  ;; cons(Object o), count(), empty(), equiv(Object o);
  (cons [this o] ; -> IPersistentCollection
    (do
      (log/trace "PersistentStoreMap.cons")
      (throw (RuntimeException. "PersistentStoreMap.cons"))))
  ;;int count();
  ;; (count  overridden by Counted
  (empty [this]  ; -> IPersistentCollection
         (throw (RuntimeException. "PersistentStoreMap.empty")))
  (equiv [this o]  ; -> boolean
         (throw (RuntimeException. "PersistentStoreMap.equiv")))

  ;;;;;;;;;;;;;;;; extends Seqable
  (^ISeq seq [this]
    (do
      (log/debug "PersistentStoreMap.seq")
      (seq {})))
         ;; (throw (RuntimeException. "PersistentStoreMap.seq")))

  ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;; clojure.lang.IReduce  ; extends IReduceInit
  (reduce [this ^IFn f]  ; -> Object
    (do
      (log/debug "PersistentStoreMap.reduce 1")
      this))
      ;; (throw (RuntimeException. "PersistentStoreMap.reduce f"))))

  (reduce [this ^IFn f ^Object to-map]  ; -> Object
    (do
      (log/debug "PersistentStoreMap.reduce 2")
      (log/debug "to-map: " (type to-map))
      to-map))
      ;; (throw (RuntimeException. "PersistentStoreMap.reduce f o"))))

  ;;;;;;;;;;;;;;;;;;;;;;;; clojure.lang.IReference; < IMeta
  (^IPersistentMap alterMeta [this, ^IFn alter, ^ISeq args]
         (throw (RuntimeException. "PersistentStoreMap.alterMeta")))
  (^IPersistentMap resetMeta [this ^IPersistentMap m]
         (throw (RuntimeException. "PersistentStoreMap.resetMeta")))

  ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;; clojure.lang.ITransientCollection
  ;; conj, persistent
  (conj [this args]  ; -> ITransientCollection
    (do
      (log/trace "PersistentStoreMap.conj")
      (throw (RuntimeException. "PersistentStoreMap.conj"))))
  (persistent [this]  ; -> IPersistentCollection
         (throw (RuntimeException. "PersistentStoreMap.persistent")))

  ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;; clojure.lang.Indexed                  ; extends Counted
  (nth [this i]                         ; called by get(int index)
         (throw (RuntimeException. "PersistentStoreMap.nth i")))

  (nth [this i not-found]
       (throw (RuntimeException. "PersistentStoreMap.nth i not-found")))
  ) ;; end deftype PersistentStoreMap
