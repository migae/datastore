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
    (log/debug "PersistentStoreMap.hashCode")
    (.hashCode content))
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
    (log/debug "PersistentStoreMap.size")
    0)
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
    (log/debug "PersistentStoreMap.invoke" k (type k))
    (cond
      (keychain? k)
      (let [e (.get content (keychain-to-key k))]
        (->PersistentEntityMap e nil))
      :else (throw (RuntimeException. "PersistentStoreMap.invoke"))))


  ;; Object applyTo(ISeq arglist) ;
  (applyTo [_ ^ISeq arglist]  ; -> Object
         (throw (RuntimeException. "PersistentStoreMap.applyTo")))

  ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;; clojure.lang.IObj ;; extends IMeta
  ;; withMeta; meta
  (^IPersistentMap meta [this]
    (do
      (log/debug "PersistentEntityMap.meta" ds-meta)
      (let [md (into (some identity [ds-meta {}]) {:type 'migae.datastore.PersistentStoreMap})]
        ;; (log/debug "meta: " md)
        md)))
  (^IObj withMeta [this ^IPersistentMap md]
    (log/debug "PersistentStoreMap.withMeta" md)
    (PersistentStoreMap. content txns md))
         ;; (throw (RuntimeException. "PersistentStoreMap.withMeta")))
    ;; (let [em (PersistentStoreMap. (.clone content) nil md]
    ;;   em))

  ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;; clojure.lang.IPersistentMap ; extends Iterable, Associative, Counted
  ;; assoc, assocEx, without; containsKey, entryAt; valAt; cons, count, empty, equiv; seq
  (assoc [this k m] ; -> IPersistentMap
    (do
      (log/debug "PersistentStoreMap.assoc" k m)
      (let [key (keychain-to-key k)
            e (Entity. key)] ;; FIXME: validate k
        (doseq [[k v] m]
          (.setProperty e (subs (str k) 1) v))
        (.put content e)
        (->PersistentEntityMap e nil))))

  (assocEx [_ k v]
         (throw (RuntimeException. "PersistentStoreMap.assocEx")))
  (without [this k]                     ; = dissoc!, return new datum with k removed
    (log/debug "PersistentStoreMap.without k:" k (type k))
    ;; FIXME: support all possible arg types
    (cond
      (keyword? k)
      (do
        (log/debug "withouting keyword " k)
        this)
      (keychain? k)
      (do
        (log/debug "withouting keychain: " k)
        (let [key (keychain-to-key k)]
          (.delete content (into-array Key [key]))
          this))
      :else
      (do
        (log/debug "PersistentStoreMap.without exception")
        (throw (IllegalArgumentException. "PersistentStoreMap.without only supports keychain args at this time")))))

  ;;;; extends Counted
  (count [_]  ; -> int
    "number of entities in datastore"
    (do
      (log/debug "PersistentStoreMap.count")
      ;; DatastoreService datastore = DatastoreServiceFactory.getDatastoreService();
      ;; Entity globalStat = datastore.prepare(new Query("__Stat_Total__")).asSingleEntity();
      (let [q (Query. "__Stat_Total__")
            ;; foo (log/debug "query: " q)
            pq (.prepare content q)
            ;; foo (log/debug "p query: " pq)
            globalStat (.asSingleEntity pq)]
        (if (nil? globalStat)
          (do
            (log/debug "globalStat nil, totalEntities: " 0)
            0)
          (let [foo (log/debug "global stat: " globalStat)
                ;; Long totalEntities = (Long) globalStat.getProperty("count");
                totalEntities (.getProperty globalStat "count")]
            (log/debug "totalEntities: " totalEntities)
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
    (log/debug "PersistentStoreMap.valAt k" k (type k) (entity-map? k))
    ;; TODO:  support query filter syntax
    ;; (if (instance? migae.datastore.PersistentEntityMap k)
    (if (entity-map? k)
      (if (not (empty? k))
        (do
          (log/debug "PersistentStoreMap.valAt entity-map arg:" k)
          (throw (IllegalArgumentException.
                  "PersistentStoreMap.valAt does not yet support map filters")))))
    (let [e (cond
              ;; (entity-map? k)
              (instance? migae.datastore.PersistentEntityMap k)
              (let [dskey (.getKey (.content k))]
                (.get content dskey))
              (instance? clojure.lang.IPersistentMap k)
              (do
                (log/debug "PersistentStoreMap.valAt IPersistentMap" k)
                (let [dskey (keychain-to-key (:migae/keychain (meta k)))]
                  (.get store-map dskey)))
              (instance? clojure.lang.IPersistentVector k)
              (do
                (log/debug "PersistentStoreMap.valAt IPersistentVector" k)
                (let [dskey (keychain-to-key k)]
                  (.get store-map dskey)))
              (keychain? k)
              (.get content (keychain-to-key k))
              (keyword? k)
              (.get content (keychain-to-key [k]))
              :else (throw (RuntimeException. "PersistentStoreMap.valAt uncaught")))]
      (->PersistentEntityMap e nil)))

  (valAt [_ k not-found]  ; -> Object
         (throw (RuntimeException. "PersistentStoreMap.valAt k not-found")))

  ;;;;  extends IPersistentCollection extends Seqable
  ;; cons(Object o), count(), empty(), equiv(Object o);
  (cons [this o] ; -> IPersistentCollection
    (do
      (log/trace "PersistentStoreMap.cons" o)
      this))
;      (throw (RuntimeException. "PersistentStoreMap.cons"))))
  ;;int count();
  ;; (count  overridden by Counted
  (empty [this]  ; -> IPersistentCollection
         (throw (RuntimeException. "PersistentStoreMap.empty")))
  (equiv [this o]  ; -> boolean
    (log/debug "PersistentStoreMap.equiv " (type o))
    (cond ;; FIXME: what's the right way to do this?
      (instance? PersistentStoreMap o) true
      (instance? clojure.lang.PersistentArrayMap o) (empty? o)
      :else false))

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

(defn store-map?
  [arg]
  (log/debug "PersistentStoreMap.store-map?" (type arg))
  (log/debug "bool" (= (type arg) 'migae.datastore.PersistentStoreMap))
  (= (type arg) 'migae.datastore.PersistentStoreMap))
