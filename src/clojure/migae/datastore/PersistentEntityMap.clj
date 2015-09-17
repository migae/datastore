(in-ns 'migae.datastore)

(clojure.core/println "loading PersistentEntityMap")

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;  PersistentEntityMap
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(deftype PersistentEntityMap [content ^IPersistentMap em-meta]

  migae.datastore.IPersistentEntityMap
  ;; java.lang.Iterable
  (iterator [this]
    (log/debug "Iterable iterator" (.content this))
    (let [props (.getProperties content) ;; java.util.Map<java.lang.String,java.lang.Object>
          entry-set (.entrySet props)
          e-iter (.iterator entry-set)
          em-iter (->PersistentEntityMapSeq e-iter) ]
      ;; (log/debug "Iterable res:" em-iter)
      em-iter))

  ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;  java.util.Map
  ;; void	clear()
  ;; boolean containsKey(Object key) => implemented by Associative, below
  ;; boolean containsValue(Object value)
  ;; Set<Map.Entry<K,V>> entrySet()
  ;; boolean equals(Object o)
  ;; V get(Object key)
  (get [_ k]
    (log/debug "PersistentEntityMap.get: " k)
    ;; (let [prop (name k)
    ;;       r    (.hasProperty content prop)]
    (get (.getProperties content) (name k)))
  ;; int hashCode()
  (hashCode [_]
    (log/debug "PersistentEntityMap.hashCode")
    (.hashCode content))
  ;; boolean isEmpty()
  (isEmpty [_]
    (log/debug "PersistentEntityMap.hashCode")
    (.isEmpty (.getProperties content)))
  ;; Set<K> keySet()
  (keySet [_]
    (log/debug "PersistentEntityMap.keySet")
    (.keySet (.getProperties content)))
  ;; V put(K key, V value)
  ;; void putAll(Map<? extends K,? extends V> m)
  ;; V remove(Object key)
  ;; int size()
  (size [_]
    (log/debug "PersistentEntityMap.SIZE ")
    (.size (.getProperties content)))
  ;; Collection<V> values()
  (values [_]
    (log/debug "PersistentEntityMap.size")
    (.values (.getProperties content)))
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
    {:pre [(keyword? k)]}
    (log/debug "IFn.invoke(" k ")")
    (if (= k :migae/keychain)
      (keychain content)
      (let [kw (subs (str k) 1)
            prop (.getProperty content kw)]
        (if (not (nil? prop))
          (ds-to-clj prop)
          nil))))
  ;; Object applyTo(ISeq arglist) ;
  (applyTo [_ ^ISeq arglist]  ; -> Object
    (log/debug "IFn.applyTo"))

  ;; clojure.lang.IMapEntry
  ;; key, val
  ;; (key [this]  ; -> Object
  ;;   (log/debug "PersistentEntityMap.IMapEntry key")
  ;;   (keychain content))
  ;; (val [this]  ; -> Object
  ;;   (log/debug "PersistentEntityMap.IMapEntry val")
  ;;   (.printStackTrace (Exception.))
  ;;   (let [props (.getProperties content)
  ;;         coll (into {} (for [[k v] props]
  ;;                             (let [prop (keyword k)
  ;;                                   val (ds-to-clj v)]
  ;;                               {prop val})))
  ;;         keychain (keychain content)
  ;;         ;; res (into coll {:migae/keychain keychain})]
  ;;         ]
  ;;     coll))

  ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;; clojure.lang.IObj ;; extends IMeta
  ;; withMeta; meta
  (^IPersistentMap meta [this]
    (do
      ;; (log/debug "PersistentEntityMap.meta" (keychain content))
      (let [md (into (some identity [em-meta {}]) {:migae/keychain (keychain content)})]
        ;; (log/debug "meta: " md)
        md)))
  ;;;; extends IMeta
  (^IObj withMeta [this ^IPersistentMap md]
    (do
      ;; (log/debug "IObj withMeta" md)
      (let [em (PersistentEntityMap. (.clone content) md)]
        ;; (log/debug "entity with meta" em)
        em)))

  ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;; clojure.lang.IPersistentMap ; extends Iterable, Associative, Counted
  ;; assoc, assocEx, without; containsKey, entryAt; valAt; cons, count, empty, equiv; seq
  (assoc [this k v] ; -> IPersistentMap
    (do
      (log/debug "PersistentEntityMap.assoc")
      (let [prop (subs (str k) 1)]
        (log/debug "IPersistentMap assoc: " k v "(" prop v ")")
        ;; (.setProperty content prop v)
        ;; this))
        (let [to-props (.getProperties content)
              to-coll (into {} (for [[k v] to-props]
                                 (let [prop (keyword k)
                                       val (ds-to-clj v)]
                                   {prop val})))
              key-chain (keychain this)
              res (assoc to-coll k v)]
          (log/debug "IPersistentMap assoc res: " res)
          (with-meta res {:migae/keychain key-chain})))))
  (assocEx [_ k v]
    (do
      (log/debug "assocEx")
      (PersistentEntityMap. (.assocEx content k v) nil)))
  (without [this k]                     ; = dissoc!, return new datum with k removed
    (do
      (log/debug "PersistentEntityMap.without")
      (let [prop (name k)]
        (log/debug "without: removing prop " k "->" prop)
        (.removeProperty content prop)
        this)))

  ;;;; extends Counted
  (count [_]  ; -> int
    "number of properties in container, plus one for the keychain"
    (do
      (log/debug "PersistentEntityMap.count")
      (let [props (.getProperties content)
            c (.size props)]
        c)))
  ;;;; extends Associative extends IPersistentCollection, ILookup
  ;; containsKey, entryAt, assoc
  ;; (containsKey [_ k]
  ;;   (log/debug "PersistentEntityMap.containsKey: " k)
  ;;   (.containsKey (.getProperties content) k))
  (containsKey [_ k] ; -> boolean
    (do
      (log/debug "PersistentEntityMap.containsKey")
      (let [prop (name k)
            r    (.hasProperty content prop)]
        ;; (log/debug "PersistentEntityMap.containsKey: " k r)
        r)))
  (entryAt [this k] ; -> IMapEntry
    (do (log/debug "Associative.entryAt " k)
    (let [val (.getProperty content (name k))
          entry (clojure.lang.MapEntry. k val)]
      ;; (log/debug "entryAt " k val entry)
      entry)))
  ;; (assoc) -> Associative => overridden at IPersistentMap

  ;;;; extends ILookup
  ;; valAt(Object key), valAt(Object key, Object notFound)
  (valAt [_ k]  ; -> Object
    (log/debug "PersistentEntityMap.ILookup.valAt: " k)
   (if (= k :migae/keychain)
      (keychain content)
      (let [prop (ds-to-clj (subs (str k) 1))]
        (if-let [v  (.getProperty content prop)]
          (do ;;(log/debug "prop:" prop ", val:" v)
              (ds-to-clj v))
          nil))))
  (valAt [_ k not-found]  ; -> Object
    (log/debug "PersistentEntityMap.ILookup.valAt w/notfound: " k)
    ;; FIXME: is k a keyword or a string?
    (.getProperty content (str k) not-found))

  ;;;;  extends IPersistentCollection extends Seqable
  ;; cons(Object o), count(), empty(), equiv(Object o);
  (cons [this o] ; -> IPersistentCollection
    ;; this is called on:  (into em {:a }); not called on (into em1 em2)
    (log/debug "PersistentEntityMap.cons: " o (type o))
    ;; (log/debug "IPersistentCollection this: " this (type this))
    (cond
      (= (type o) PersistentEntityMap)
      (do
        ;; (log/debug "cons PersistentEntityMap to this")
        ;; (log/debug "this:" this)
        ;; (log/debug "that:" o)
        (let [props (.getProperties (.content o))
              newe (.clone content)]
          (.setPropertiesFrom newe content) ;; is this needed, or is clone enough?
          (doseq [[k v] props]
           (.setProperty newe k v))
          (PersistentEntityMap. newe em-meta)))
      ;; (= (type o) java.util.Collections$UnmodifiableMap$UnmodifiableEntrySet$UnmodifiableEntry)
      (nil? o)
      (do
        (log/debug "nil object")
        this)
      (= (type o) clojure.lang.MapEntry)
      (do
        (log/debug "cons clojure.lang.MapEntry" o " to emap" this)
        (let [new-entity (.clone content)]
          (.setProperty new-entity (subs (str (first o)) 1) (get-val-ds (second o)))
          (PersistentEntityMap. new-entity nil)))
      (= (type o) clojure.lang.PersistentArrayMap)
      (do
        (log/debug "cons PersistentArrayMap to emap")
        (let [newe (.clone content)
              newm (into {} em-meta)]
          (doseq [[k v] o]
            (.setProperty newe (subs (str k) 1) (get-val-ds v)))
          ;; (.put (datastore) content)
          (PersistentEntityMap. newe newm)))
      (= (type o) java.util.Map$Entry)
      (do
        (log/debug "cons java.util.Map$Entry to emap")
        (.setProperty content (.getKey o) (.getValue o))
        ;; (.put (datastore) content)
        this)
      :else (log/debug "cons HELP?" o (type o))))
  ;;int count();
  ;; (count  overridden by Counted
  (empty [this]  ; -> IPersistentCollection
    (log/debug "PersistentEntityMap.empty")
    (let [k (.getKey (.content this))
          e (Entity. k)]
      (PersistentEntityMap. e nil)))
  (equiv [this o]  ; -> boolean
    ;; FIXME: double-check the logic
    (cond
      (instance? migae.datastore.IPersistentEntityMap o)
      (do
        ;; (log/debug "comparing PersistentEntityMap")
        (if (.equals content (.content o))
          (do ;; the entities are key=
            ;; (log/debug "(.equals content (.content o)) is true: key=")
            ;; (log/debug "content:" content)
            ;; (log/debug "(.content o):" (.content o))
            (let [this-map (.getProperties content)
                  that-map (.getProperties (.content o))]
              ;; (log/debug "this-map:" this-map)
              ;; (log/debug "that-map:" that-map)
              ;; (log/debug "(= this-map that-map)" (= this-map that-map))
              (= this-map that-map)))
          (do ;; (log/debug "entities have different keys")
              false)))
      (instance? clojure.lang.PersistentArrayMap o)
      (do ;; (log/debug "comparing " (type o))
          (let [props (.getProperties content)
                pmap (into {}
                           (for [[k v] props]
                             (do
                               (let [prop (keyword k)
                                     val (ds-to-clj v)]
                                 ;; (log/debug "prop " prop " val " val)
                                 {prop val}))))]
            (= pmap o)))
      :else (throw (RuntimeException. "PersistentEntityMap.equiv " (type o)))))

  ;;;;;;;;;;;;;;;; extends Seqable
  (^ISeq seq [this]
    ;; seq is called by: into, merge, "print", e.g. (log/debug em)
    ;; (log/debug "PersistentEntityMap.seq:" (type this)) ;; (.hashCode this) (.getKey content))
    ;; (log/debug "    on content" content)
    (let [props (.getProperties content)
          ;; foo (log/debug "props" props)
          emap (into {}
                           (for [[k v] props]
                             (do
                               (let [prop (keyword k)
                                     val (ds-to-clj v)]
                                 ;; (log/debug "prop " prop " val " val)
                                 {prop val}))))
          k (keychain content)
          res (with-meta emap {:migae/keychain k})
          ;; res (into r {:migae/keychain k})
          ]
      ;; (log/debug "seq result:" #_(meta res) (type res) res)
      ;; (log/debug "result as seq:" (seq res))
      (seq res)))
  ;;      this))

  ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;; clojure.lang.IReduce  ; extends IReduceInit
  (reduce [this ^IFn f]  ; -> Object
    (throw (RuntimeException. "PersistentEntityMap.reduce" f)))
  (reduce [this ^IFn f ^Object seed]  ; -> Object
    ;; called by "print" stuff
    (log/debug "PersistentEntityMap.reduce func:" f)
    (log/debug "PersistentEntityMap.reduce seed:" (.getSimpleName (class seed)))
    (cond
      ;; (= (class seed) migae.datastore.PersistentStoreMap)
      (store-map? seed)
      (do
        (let [ds (.content seed)]
          ;; (log/debug "PersistentEntityMap.reduce ds: " ds)
          (.put ds content)
          seed))
      (= (class seed) com.google.appengine.api.datastore.DatastoreServiceImpl)
      (do
        (.put seed content)
        seed)
      (= (class seed) clojure.lang.PersistentArrayMap)
      (do
        (log/debug "seed:  PersistentArrayMap")
        )
      (= (class seed) clojure.lang.PersistentVector)
      (do
        (log/debug "seed:  clojure.lang.PersistentVector")
        )
      (= (class seed) PersistentEntityMap)
      (do
        ;; (log/debug "seed:  PersistentEntityMap")
        (let [k (.getKey content)
              e (Entity. k)]
          (.setPropertiesFrom e (.content seed))
          (.setPropertiesFrom e content)
          (PersistentEntityMap. e nil)))
      ;; f = cons, so we can just use the native into
      ;; (let [from-props (.getProperties content)
      ;;       from-coll (into {} (for [[k v] from-props]
      ;;                                (let [prop (keyword k)
      ;;                                      val (ds-to-clj v)]
      ;;                                  {prop val})))
      ;;       foo (.setPropertiesFrom (.content seed) (.content this))
      ;;       to-props (.getProperties (.content seed))
      ;;       to-coll (into {} (for [[k v] to-props]
      ;;                              (let [prop (keyword k)
      ;;                                    val (ds-to-clj v)]
      ;;                                {prop val})))
      ;;       res (with-meta (into to-coll from-coll)
      ;;             {:migae/keychain (:migae/key (meta seed))
      ;;              :type PersistentEntityMap})]
      ;;   ;; (log/debug "to-coll: " res (type res))
      ;;   seed)
      (= (class seed) clojure.lang.PersistentArrayMap$TransientArrayMap)
      (do
        (log/debug "seed:  PersistentArrayMap$TransientArrayMap")
        ;; we use a ghastly hack in order to retain metadata
        ;; FIXME: handle case where seed is a clj-emap (map with ^:PersistentEntityMap metadata)
        (let [from-props (.getProperties content)
              from-coll (into {} (for [[k v] from-props]
                                       (let [prop (keyword k)
                                             val (ds-to-clj v)]
                                         {prop val})))
              to-ent (Entity. (.getKey content))
              ;; to-coll (into {} (for [[k v] to-props]
              ;;                        (let [prop (keyword k)
              ;;                              val (ds-to-clj v)]
              ;;                          {prop val})))
              to-keychain (if (nil? (:migae/keychain seed))
                            (keychain content)
                            (:migae/keychain seed))]
          (doseq [[k v] from-coll]
            (assoc! seed k v))
          (let [p (persistent! seed)]
            (doseq [[k v] p]
              (.setProperty to-ent (subs (str k) 1) (get-val-ds v))))
          ;; (let [m1 (persistent! seed)
          ;;       m2 (with-meta m1 {:migae/keychain keychain
          ;;                         :type PersistentEntityMap})
          ;;       to-coll (transient m2)]
          ;;   (log/debug "m2: " (meta m2) m2 (class m2))
          ;;   (log/debug "to-coll: " (meta to-coll) to-coll (class to-coll))
          (PersistentEntityMap. to-ent nil)))
      :else (log/debug "PersistentEntityMap.reduce HELP" (class seed)))
      )

  ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;; clojure.lang.IReference ; extends IMeta; required to support metadata reader syntax?
    ;; IPersistentMap alterMeta(IFn alter, ISeq args) ;
  (^IPersistentMap alterMeta [this, ^IFn alter, ^ISeq args]
    (log/debug "IReference.alterMeta")
    {:foo 3})
    ;; IPersistentMap resetMeta(IPersistentMap m);
  (^IPersistentMap resetMeta [this ^IPersistentMap m]
    (log/debug "IReference.resetMeta")
    {:bar 3})

  ;; FIXME: make result of (into {} em) support Map$Entry so it behaves like an em
  ;; this doesn't work since clojure.lang.PersistentArrayMap cannot be cast to java.util.Map$Entry
  ;; NB:  IMapEntry extends java.util.Map$Entry
  ;; can we use defprotocol and extend to do this?

  ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;; clojure.lang.ITransientCollection
  ;; conj, persistent
  (conj [this args]  ; -> ITransientCollection
    (log/debug "ITransientCollection conj")
    (let [item (first args)
          k (name (key item))
          v (val item)
          val (if (number? v) v
                  (if (string? v) v
                      (edn/read-string v)))]
      ;; (log/debug "ITransientCollection conj: " args item k v)
      (.setProperty content k v)
      this))
  (persistent [this]  ; -> IPersistentCollection
    (log/debug "ITransientCollection persistent")
    ;; (try (/ 1 0) (catch Exception e (print-stack-trace e)))
    (let [props (.getProperties content)
          kch (keychain content)
          coll (into {} (for [[k v] props]
                              (let [prop (keyword k)
                                    val (ds-to-clj v)]
                                {prop val})))
          res (with-meta coll {:migae/keychain kch
                               :type PersistentEntityMap})]
      (log/debug "persistent result: " res (class res))
      res))


  ;; clojure.lang.ITransientMap
  ;; (assoc [this k v]                     ; both assoc! and assoc (?)
  ;;   (let [prop (name k)]
  ;;     (log/debug "ITransientMap assoc: setting prop " k "->" prop v)
  ;;     (.setProperty content prop v)
  ;;     this))
  ;; (without [this k]                     ; = dissoc!, return new datum with k removed
  ;;   (let [prop (name k)]
  ;;     (log/debug "without: removing prop " k "->" prop)
  ;;     (.removeProperty content prop)
  ;;     this))
  ;; (persistent [this]                    ; persistent!
  ;;     (log/debug "ITransientMap persistent")
  ;;   )

  ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;; clojure.lang.Indexed                  ; extends Counted
  ;; (count [this]                         ; Counted
  ;;   (log/debug "count"))
  (nth [this i]                         ; called by get(int index)
    (log/debug "nth"))
  ;;    (next em-iter)) ;; HACK
  (nth [this i not-found]
    )

  ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;; IHashEq
  (^int hasheq [this]
    (clojure.lang.Murmur3/hashUnordered this))

  ) ;; end deftype PersistentEntityMap

