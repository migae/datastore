(in-ns 'migae.datastore)

(clojure.core/println "loading PersistentEntityMap")
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;  PersistentEntityMap
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(deftype PersistentEntityMap [content ^IPersistentMap em-meta]

  migae.datastore.IPersistentEntityMap
  ;; java.lang.Iterable
  (iterator [this]
    (log/trace "Iterable iterator" (.content this))
    (let [props (.getProperties content) ;; java.util.Map<java.lang.String,java.lang.Object>
          entry-set (.entrySet props)
          e-iter (.iterator entry-set)
          em-iter (->PersistentEntityMapSeq e-iter) ]
      ;; (log/trace "Iterable res:" em-iter)
      em-iter))

  ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;  java.util.Map
  ;; void	clear()
  ;; boolean containsKey(Object key) => implemented by Associative, below
  ;; boolean containsValue(Object value)
  ;; Set<Map.Entry<K,V>> entrySet()
  ;; boolean equals(Object o)
  ;; V get(Object key)
  (get [_ k]
    (log/trace "PersistentEntityMap.get: " k)
    ;; (let [prop (name k)
    ;;       r    (.hasProperty content prop)]
    (get (.getProperties content) (name k)))
  ;; int hashCode()
  (hashCode [_]
    (log/trace "PersistentEntityMap.hashCode")
    (.hashCode content))
  ;; boolean isEmpty()
  (isEmpty [_]
    (log/trace "PersistentEntityMap.hashCode")
    (.isEmpty (.getProperties content)))
  ;; Set<K> keySet()
  (keySet [_]
    (log/trace "PersistentEntityMap.keySet")
    (.keySet (.getProperties content)))
  ;; V put(K key, V value)
  ;; void putAll(Map<? extends K,? extends V> m)
  ;; V remove(Object key)
  ;; int size()
  (size [_]
    (log/trace "PersistentEntityMap.SIZE ")
    (.size (.getProperties content)))
  ;; Collection<V> values()
  (values [_]
    (log/trace "PersistentEntityMap.size")
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
    (log/trace "IFn.invoke(" k ")")
    (if (= k :migae/keychain)
      (keychain content)
      (let [kw (subs (str k) 1)
            prop (.getProperty content kw)]
        (if (not (nil? prop))
          (ds-to-clj prop)
          nil))))
  ;; Object applyTo(ISeq arglist) ;
  (applyTo [_ ^ISeq arglist]  ; -> Object
    (log/trace "IFn.applyTo"))

  ;; clojure.lang.IMapEntry
  ;; key, val
  ;; (key [this]  ; -> Object
  ;;   (log/trace "PersistentEntityMap.IMapEntry key")
  ;;   (keychain content))
  ;; (val [this]  ; -> Object
  ;;   (log/trace "PersistentEntityMap.IMapEntry val")
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
    ;; (log/trace "IObj.meta" (keychain content))
    (into (some identity [em-meta {}]) {:migae/keychain (keychain content)}))
  ;;;; extends IMeta
  (^IObj withMeta [this ^IPersistentMap md]
    (log/trace "IObj withMeta" md)
    (let [em (PersistentEntityMap. (.clone content) md)]
      ;; (log/trace "entity with meta" em)
      em))

  ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;; clojure.lang.IPersistentMap ; extends Iterable, Associative, Counted
  ;; assoc, assocEx, without; containsKey, entryAt; valAt; cons, count, empty, equiv; seq
  (assoc [this k v] ; -> IPersistentMap
    (let [prop (subs (str k) 1)]
      (log/trace "IPersistentMap assoc: " k v "(" prop v ")")
      ;; (.setProperty content prop v)
      ;; this))
      (let [to-props (.getProperties content)
            to-coll (into {} (for [[k v] to-props]
                                     (let [prop (keyword k)
                                           val (ds-to-clj v)]
                                       {prop val})))
            key-chain (keychain this)
            res (assoc to-coll k v)]
      (log/trace "IPersistentMap assoc res: " res)
      (with-meta res {:migae/keychain key-chain}))))
  (assocEx [_ k v]
    (log/trace "assocEx")
    (PersistentEntityMap. (.assocEx content k v) nil))
  (without [this k]                     ; = dissoc!, return new datum with k removed
    (let [prop (name k)]
      (log/trace "without: removing prop " k "->" prop)
      (.removeProperty content prop)
      this))

  ;;;; extends Counted
  (count [_]  ; -> int
    "number of properties in container, plus one for the keychain"
    (log/trace "PersistentEntityMap.count")
    (let [props (.getProperties content)
          c (.size props)]
      c))
  ;;;; extends Associative extends IPersistentCollection, ILookup
  ;; containsKey, entryAt, assoc
  ;; (containsKey [_ k]
  ;;   (log/trace "PersistentEntityMap.containsKey: " k)
  ;;   (.containsKey (.getProperties content) k))
  (containsKey [_ k] ; -> boolean
    (let [prop (name k)
          r    (.hasProperty content prop)]
      ;; (log/trace "PersistentEntityMap.containsKey: " k r)
      r))
  (entryAt [this k] ; -> IMapEntry
    (do (log/trace "Associative.entryAt " k)
    (let [val (.getProperty content (name k))
          entry (clojure.lang.MapEntry. k val)]
      ;; (log/trace "entryAt " k val entry)
      entry)))
  ;; (assoc) -> Associative => overridden at IPersistentMap

  ;;;; extends ILookup
  ;; valAt(Object key), valAt(Object key, Object notFound)
  (valAt [_ k]  ; -> Object
    (log/trace "PersistentEntityMap.ILookup.valAt: " k)
   (if (= k :migae/keychain)
      (keychain content)
      (let [prop (ds-to-clj (subs (str k) 1))]
        (if-let [v  (.getProperty content prop)]
          (do ;;(log/trace "prop:" prop ", val:" v)
              (ds-to-clj v))
          nil))))
  (valAt [_ k not-found]  ; -> Object
    (log/trace "PersistentEntityMap.ILookup.valAt w/notfound: " k)
    ;; FIXME: is k a keyword or a string?
    (.getProperty content (str k) not-found))

  ;;;;  extends IPersistentCollection extends Seqable
  ;; cons(Object o), count(), empty(), equiv(Object o);
  (cons [this o] ; -> IPersistentCollection
    ;; this is called on:  (into em {:a }); not called on (into em1 em2)
    (log/trace "PersistentEntityMap.cons: " o (type o))
    ;; (log/trace "IPersistentCollection this: " this (type this))
    (cond
      (= (type o) PersistentEntityMap)
      (do
        ;; (log/trace "cons PersistentEntityMap to this")
        ;; (log/trace "this:" this)
        ;; (log/trace "that:" o)
        (let [props (.getProperties (.content o))
              newe (.clone content)]
          (.setPropertiesFrom newe content) ;; is this needed, or is clone enough?
          (doseq [[k v] props]
            (.setProperty newe k v))
          (PersistentEntityMap. newe em-meta)))
      ;; (= (type o) java.util.Collections$UnmodifiableMap$UnmodifiableEntrySet$UnmodifiableEntry)
      (nil? o)
      (do
        (log/trace "nil object")
        this)
      (= (type o) clojure.lang.MapEntry)
      (do
        (log/trace "cons clojure.lang.MapEntry" o " to emap" this)
        (let [new-entity (.clone content)]
          (.setProperty new-entity (subs (str (first o)) 1) (get-val-ds (second o)))
          (PersistentEntityMap. new-entity nil)))
      (= (type o) clojure.lang.PersistentArrayMap)
      (do
        (log/trace "cons PersistentArrayMap to emap")
        (let [newe (.clone content)
              newm (into {} em-meta)]
          (doseq [[k v] o]
            (.setProperty newe (subs (str k) 1) (get-val-ds v)))
          ;; (.put (datastore) content)
          (PersistentEntityMap. newe newm)))
      (= (type o) java.util.Map$Entry)
      (do
        (log/trace "cons java.util.Map$Entry to emap")
        (.setProperty content (.getKey o) (.getValue o))
        ;; (.put (datastore) content)
        this)
      :else (log/trace "cons HELP?" o (type o))))
  ;;int count();
  ;; (count  overridden by Counted
  (empty [this]  ; -> IPersistentCollection
    (log/trace "PersistentEntityMap.empty")
    (let [k (.getKey (.content this))
          e (Entity. k)]
      (PersistentEntityMap. e nil)))
  (equiv [this o]  ; -> boolean
    (log/trace "PersistentEntityMap.equiv") ;;
    ;; (throw (RuntimeException. "clojure.core/= not supported; use one of key=?, map=? or entity-map?")))
    ;; FIXME: test for equality of values?
    (if (instance? migae.datastore.IPersistentEntityMap o)
      (do
        (log/trace "comparing PersistentEntityMap")
        (if (.equals content (.content o))
          (do ;; the entities are key=
            (log/trace "(.equals content (.content o)) is true: key=")
            (log/trace "content:" content)
            (log/trace "(.content o):" (.content o))
            (let [this-map (.getProperties content)
                  that-map (.getProperties (.content o))]
              (log/trace "this-map:" this-map)
              (log/trace "that-map:" that-map)
              (log/trace "(= this-map that-map)" (= this-map that-map))
              (= this-map that-map)))
          (do (log/trace "entities have different keys")
              false)))
      ;; FIXME:  check for (instance? clojure.lang.IPersistentMap)
      (do (log/trace "comparing " (type o))
          (let [props (.getProperties content)
                pmap (into {}
                           (for [[k v] props]
                             (do
                               (let [prop (keyword k)
                                     val (ds-to-clj v)]
                                 ;; (log/trace "prop " prop " val " val)
                                 {prop val}))))]
            (= pmap o)))))

  ;;;;;;;;;;;;;;;; extends Seqable
  (^ISeq seq [this]
    ;; seq is called by: into, merge, "print", e.g. (log/trace em)
    (log/trace "PersistentEntityMap.seq:" (type this)) ;; (.hashCode this) (.getKey content))
    ;; (log/trace "    on content" content)
    (let [props (.getProperties content)
          ;; foo (log/trace "props" props)
          emap (into {}
                           (for [[k v] props]
                             (do
                               (let [prop (keyword k)
                                     val (ds-to-clj v)]
                                 ;; (log/trace "prop " prop " val " val)
                                 {prop val}))))
          k (keychain content)
          res (with-meta emap {:migae/keychain k})
          ;; res (into r {:migae/keychain k})
          ]
      (log/trace "seq result:" #_(meta res) (type res) res)
      (seq res)))
  ;;      this))

  ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;; clojure.lang.IReduce  ; extends IReduceInit
  (reduce [this ^IFn f]  ; -> Object
    (log/trace "HELP! reduce") (flush)
    this)
  (reduce [this ^IFn f ^Object to-map]  ; -> Object
    ;; called by "print" stuff
    (log/trace "IReduce.reduce:" (class to-map) (type to-map))
    (cond
      (= (class to-map) clojure.lang.PersistentArrayMap)
      (do
        (log/trace "to-map:  PersistentArrayMap")
        )
      (= (class to-map) clojure.lang.PersistentVector)
      (do
        (log/trace "to-map:  clojure.lang.PersistentVector")
        )
      (= (class to-map) PersistentEntityMap)
      (do
        ;; (log/trace "to-map:  PersistentEntityMap")
        (let [k (.getKey content)
              e (Entity. k)]
          (.setPropertiesFrom e (.content to-map))
          (.setPropertiesFrom e content)
          (PersistentEntityMap. e nil)))
      ;; f = cons, so we can just use the native into
      ;; (let [from-props (.getProperties content)
      ;;       from-coll (into {} (for [[k v] from-props]
      ;;                                (let [prop (keyword k)
      ;;                                      val (ds-to-clj v)]
      ;;                                  {prop val})))
      ;;       foo (.setPropertiesFrom (.content to-map) (.content this))
      ;;       to-props (.getProperties (.content to-map))
      ;;       to-coll (into {} (for [[k v] to-props]
      ;;                              (let [prop (keyword k)
      ;;                                    val (ds-to-clj v)]
      ;;                                {prop val})))
      ;;       res (with-meta (into to-coll from-coll)
      ;;             {:migae/keychain (:migae/key (meta to-map))
      ;;              :type PersistentEntityMap})]
      ;;   ;; (log/trace "to-coll: " res (type res))
      ;;   to-map)
      (= (class to-map) clojure.lang.PersistentArrayMap$TransientArrayMap)
      (do
        (log/trace "to-map:  PersistentArrayMap$TransientArrayMap")
        ;; we use a ghastly hack in order to retain metadata
        ;; FIXME: handle case where to-map is a clj-emap (map with ^:PersistentEntityMap metadata)
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
              to-keychain (if (nil? (:migae/keychain to-map))
                            (keychain content)
                            (:migae/keychain to-map))]
          (doseq [[k v] from-coll]
            (assoc! to-map k v))
          (let [p (persistent! to-map)]
            (doseq [[k v] p]
              (.setProperty to-ent (subs (str k) 1) (get-val-ds v))))
          ;; (let [m1 (persistent! to-map)
          ;;       m2 (with-meta m1 {:migae/keychain keychain
          ;;                         :type PersistentEntityMap})
          ;;       to-coll (transient m2)]
          ;;   (log/trace "m2: " (meta m2) m2 (class m2))
          ;;   (log/trace "to-coll: " (meta to-coll) to-coll (class to-coll))
          (PersistentEntityMap. to-ent nil)))
      :else (log/trace "HELP! reduce!" (class to-map)))
      )

  ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;; clojure.lang.IReference ; extends IMeta; required to support metadata reader syntax?
    ;; IPersistentMap alterMeta(IFn alter, ISeq args) ;
  (^IPersistentMap alterMeta [this, ^IFn alter, ^ISeq args]
    (log/trace "IReference.alterMeta")
    {:foo 3})
    ;; IPersistentMap resetMeta(IPersistentMap m);
  (^IPersistentMap resetMeta [this ^IPersistentMap m]
    (log/trace "IReference.resetMeta")
    {:bar 3})

  ;; FIXME: make result of (into {} em) support Map$Entry so it behaves like an em
  ;; this doesn't work since clojure.lang.PersistentArrayMap cannot be cast to java.util.Map$Entry
  ;; NB:  IMapEntry extends java.util.Map$Entry
  ;; can we use defprotocol and extend to do this?

  ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;; clojure.lang.ITransientCollection
  ;; conj, persistent
  (conj [this args]  ; -> ITransientCollection
    (log/trace "ITransientCollection conj")
    (let [item (first args)
          k (name (key item))
          v (val item)
          val (if (number? v) v
                  (if (string? v) v
                      (edn/read-string v)))]
      ;; (log/trace "ITransientCollection conj: " args item k v)
      (.setProperty content k v)
      this))
  (persistent [this]  ; -> IPersistentCollection
    (log/trace "ITransientCollection persistent")
    ;; (try (/ 1 0) (catch Exception e (print-stack-trace e)))
    (let [props (.getProperties content)
          kch (keychain content)
          coll (into {} (for [[k v] props]
                              (let [prop (keyword k)
                                    val (ds-to-clj v)]
                                {prop val})))
          res (with-meta coll {:migae/keychain kch
                               :type PersistentEntityMap})]
      (log/trace "persistent result: " res (class res))
      res))


  ;; clojure.lang.ITransientMap
  ;; (assoc [this k v]                     ; both assoc! and assoc (?)
  ;;   (let [prop (name k)]
  ;;     (log/trace "ITransientMap assoc: setting prop " k "->" prop v)
  ;;     (.setProperty content prop v)
  ;;     this))
  ;; (without [this k]                     ; = dissoc!, return new datum with k removed
  ;;   (let [prop (name k)]
  ;;     (log/trace "without: removing prop " k "->" prop)
  ;;     (.removeProperty content prop)
  ;;     this))
  ;; (persistent [this]                    ; persistent!
  ;;     (log/trace "ITransientMap persistent")
  ;;   )

  ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;; clojure.lang.Indexed                  ; extends Counted
  ;; (count [this]                         ; Counted
  ;;   (log/trace "count"))
  (nth [this i]                         ; called by get(int index)
    (log/trace "nth"))
;;    (next em-iter)) ;; HACK
  (nth [this i not-found]
    )
  ) ;; end deftype PersistentEntityMap
