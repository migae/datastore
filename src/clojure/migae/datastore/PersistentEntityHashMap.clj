(in-ns 'migae.datastore)

;; (declare get-val-clj get-val-ds)
;; (declare props-to-map get-next-emap-prop)

;; (declare make-embedded-entity)

(defn- get-next-emap-prop [this]
  ;; (log/trace "get-next-emap-prop" (.query this))
  (let [r (.next (.query this))]
    ;; (log/trace "next: " r)
    r))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;  PersistentEntityHashMap
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(deftype PersistentEntityHashMap [k ^clojure.lang.PersistentHashMap content, meta]

  migae.datastore.IPersistentEntityMap

  ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;; java.lang.Iterable
  (iterator [this]
    (log/trace "Iterable iterator" (.content this))
    (let [props (.getProperties content) ;; java.util.Map<java.lang.String,java.lang.Object>
          entry-set (.entrySet props)
          e-iter (.iterator entry-set)
          em-iter (PersistentEntityMapIterator. e-iter) ]
    ;; (log/trace "Iterable res:" em-iter)
    em-iter))

  ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;   java.util.Map$Entry
  ;; (getKey [this]
  ;;   ;; (log/trace "PersistentEntityHashMap getKey ")
  ;;   k)
  ;; (getValue [_]
  ;;   ;; (log/trace "HashMap getValue")
  ;;   content)
  ;; (equals [_ o]
  ;;   (log/trace "HashMap equals")
  ;;   )
  ;; (hashCode [_]
  ;;   (log/trace "HashMap hashCode")
  ;;   )
  ;; (setValue [_ v]
  ;;   (log/trace "HashMap setValue")
  ;;   )

  ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;   clojure.lang.IFn
  ;; invoke, applyTo
  (invoke [this k]  ; -> Object
    {:pre [(keyword? k)]}
    (log/trace "IFn.invoke(" k ")")
    (if (= k :migae/keychain)
      (ekey/to-keychain content)
      (let [kw (subs (str k) 1)
            prop (.getProperty content kw)]
        (if (not (nil? prop))
          (get-val-clj prop)
          nil))))
  ;; Object applyTo(ISeq arglist) ;
  (applyTo [_ ^ISeq arglist]  ; -> Object
    (log/trace "IFn.applyTo"))

  ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;   clojure.lang.IMapEntry
  ;; key, val
  ;; (clj/key [this]  ; -> Object
  ;;   (log/trace "IMapEntry key")
  ;;   )
  ;; (val [this]  ; -> Object
  ;;   (log/trace "PersistentEntityHashMap.IMapEntry val")
  ;;   content)

  ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;   clojure.lang.IObj ;; extends IMeta
  ;; withMeta; meta
  (^IPersistentMap meta [this]
    ;; (log/trace "IObj meta" (ekey/to-keychain content))
    meta)
  ;;;; extends IMeta
  (^IObj withMeta [this ^IPersistentMap md]
    ;; (log/trace "IObj withMeta" md)
    (let [em (PersistentEntityMap. (.clone content) md)]
      ;; (log/trace "entity with meta" em)
      em))

  ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;   clojure.lang.IPersistentMap ; extends Iterable, Associative, Counted
  ;; assoc, assocEx, without; containsKey, entryAt; valAt; cons, count, empty, equiv; seq
  (assoc [this k v] ; -> IPersistentMap
    (let [prop (subs (str k) 1)]
      (log/trace "IPersistentMap assoc: " k v "(" prop v ")")
      ;; (.setProperty content prop v)
      ;; this))
      (let [to-props (.getProperties content)
            to-coll (clj/into {} (for [[k v] to-props]
                                     (let [prop (keyword k)
                                           content (get-val-clj v)]
                                       {prop content})))
            key-chain (ekey/to-keychain this)
            res (clj/assoc to-coll k v)]
      (log/trace "IPersistentMap assoc res: " res)
      (with-meta res {:migae/keychain key-chain}))))
  (assocEx [_ k v]
    (log/trace "assocEx")
    (PersistentEntityMap. (.assocEx content k v) nil))
  (without [this k]                     ; = dissoc!, return new datum with k removed
    (let [prop (clj/name k)]
      (log/trace "without: removing prop " k "->" prop)
      (.removeProperty content prop)
      this))

  ;;;; extends Counted
  (count [_]  ; -> int
    "number of properties in container, plus one for the keychain"
    ;; (log/trace "count")
    (.size (+ (.getProperties content) 1)))

  ;;;; extends Associative extends IPersistentCollection, ILookup
  ;; containsKey, entryAt, assoc
  (containsKey [_ k] ; -> boolean
    (do (log/trace "Associative.containsKey " k)
    (let [prop (clj/name k)
          r    (.hasProperty content prop)]
      r)))
  (entryAt [this k] ; -> IMapEntry
    (do (log/trace "Associative.entryAt " k)
    (let [val (.getProperty content (clj/name k))
          entry (clojure.lang.MapEntry. k val)]
      ;; (log/trace "entryAt " k val entry)
      entry)))
  ;; (assoc) -> Associative => overridden at IPersistentMap

  ;;;; extends ILookup
  ;; valAt(Object key), valAt(Object key, Object notFound)
  (valAt [_ k]  ; -> Object
    (log/trace "ILookup.valAt" k)
    (if (= k :migae/keychain)
      (ekey/to-keychain content)
      (let [prop (get-val-clj (subs (str k) 1))]
        ;; (log/trace "prop:" prop)
        (if-let [v  (.getProperty content prop)]
          (get-val-clj v)
          nil))))
  (valAt [_ k not-found]  ; -> Object
    (log/trace "valAt w/notfound: " k)
    ;; FIXME: is k a keyword or a string?
    (.getProperty content (str k) not-found))

  ;;;;  extends IPersistentCollection extends Seqable
  ;; cons(Object o), count(), empty(), equiv(Object o);
  (cons [this o] ; -> IPersistentCollection
    ;; this is called on:  (into em {:a }); not called on (into em1 em2)
    (log/trace "IPersistentCollection.cons: " o (type o))
    (cond
      (nil? o)
      (do
        (log/trace "nil object")
        this)
      (= (type o) clojure.lang.MapEntry)
      (do
        ;; (log/trace "cons clojure.lang.MapEntry" o " to emap" this)
        (let [new-entity (.clone content)]
          (.setProperty new-entity (subs (str (first o)) 1) (get-val-ds (second o)))
          (PersistentEntityMap. new-entity nil)))
      (= (type o) clojure.lang.PersistentArrayMap)
      (do
        (log/trace "cons PersistentArrayMap to emap")
        o)
      ;; (do
      ;;   ;; (log/trace "cons clj map to emap")
      ;;   (doseq [[k v] o]
      ;;     (let [nm (subs (str k) 1)
      ;;           val (get-val-ds v)]
      ;;       ;; (log/trace "cons key: " nm (type nm))
      ;;       ;; (log/trace "cons val: " val (type val))
      ;;       (.setProperty entity nm val)))
      ;;   ;; (.put (datastore) val)
      ;;   this)
      (= (type o) PersistentEntityMap)
      (do
        (log/trace "cons PersistentEntityMap to emap")
        (let [props (.getProperties (.content o))]
          ;; (log/trace "cons emap to emap")
          (doseq [[k v] props]
            (.setProperty content (subs (str k) 1) (get-val-ds v)))
          ;; (.put (datastore) content)
          this))
      ;; (= (type o) java.util.Collections$UnmodifiableMap$UnmodifiableEntrySet$UnmodifiableEntry)
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
    (let [k (.getKey (.content this))
          e (Entity. k)]
      (PersistentEntityMap. e nil)))
  (equiv [this o]  ; -> boolean
    (log/trace "PersistentEntityHasmMap.equiv")
    (log/trace "    this:" this)
    (log/trace "    o:   " (type o))
    nil)
      ;; isa
    ;; (or (.equals this o)
    ;;     (clojure.lang.PersistentCollection/equiv this o)))
;;        (.equals content (.content o)))

  ;;;; extends Seqable
  ;; seq()
  (seq [this] ; -> ISeq
    ;; seq is called by: into, merge, "print", e.g. (log/trace em)
    ;; (log/trace "seq" (.hashCode this) (.getKey content))
    ;; (let [props (.getProperties content)
    ;;       kprops (clj/into {}
    ;;                        (for [[k v] props]
    ;;                          (do
    ;;                            ;; (log/trace "v: " v)
    ;;                            (let [prop (keyword k)
    ;;                                  val (get-val-clj v)]
    ;;                              ;; (log/trace "prop " prop " val " val)
    ;;                              {prop val}))))
    ;;       k (ekey/to-keychain val)
    (let [res (clj/into content {:migae/keychain k})]
      ;; (log/trace "hashmap seq result:" (type res) res)
      (seq res)))
  ;;      this))

  ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;   clojure.lang.IReduce  ; extends IReduceInit
  (reduce [this ^IFn f]  ; -> Object
    (log/trace "HELP! reduce") (flush)
    this)
  (reduce [this ^IFn f ^Object to-map]  ; -> Object
    ;; called by "print" stuff
    ;; (log/trace "IReduce.reduce:" (class to-map) (type to-map))
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
      ;; f = cons, so we can just use the native clj/into
      ;; (let [from-props (.getProperties content)
      ;;       from-coll (clj/into {} (for [[k v] from-props]
      ;;                                (let [prop (keyword k)
      ;;                                      val (get-val-clj v)]
      ;;                                  {prop val})))
      ;;       foo (.setPropertiesFrom (.content to-map) (.content this))
      ;;       to-props (.getProperties (.content to-map))
      ;;       to-coll (clj/into {} (for [[k v] to-props]
      ;;                              (let [prop (keyword k)
      ;;                                    val (get-val-clj v)]
      ;;                                {prop val})))
      ;;       res (with-meta (clj/into to-coll from-coll)
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
              from-coll (clj/into {} (for [[k v] from-props]
                                       (let [prop (keyword k)
                                             val (get-val-clj v)]
                                         {prop val})))
              to-ent (Entity. (.getKey content))
              ;; to-coll (clj/into {} (for [[k v] to-props]
              ;;                        (let [prop (keyword k)
              ;;                              val (get-val-clj v)]
              ;;                          {prop val})))
              to-keychain (if (nil? (:migae/keychain (meta to-map)))
                            (ekey/to-keychain content)
                            (:migae/keychain (meta to-map)))]
          (doseq [[k v] from-coll]
            (assoc! to-map k v))
          (let [p (persistent! to-map)]
            (doseq [[k v] p]
              (.setProperty to-ent (subs (str k) 1) (get-val-ds v))))
          ;; (let [m1 (persistent! to-map)
          ;;       m2 (with-meta m1 {:migae/keychain ekey/to-keychain
          ;;                         :type PersistentEntityMap})
          ;;       to-coll (transient m2)]
          ;;   (log/trace "m2: " (meta m2) m2 (class m2))
          ;;   (log/trace "to-coll: " (meta to-coll) to-coll (class to-coll))
          (PersistentEntityMap. to-ent nil)))
      :else (log/trace "HELP! reduce!" (class to-map)))
      )

  ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;   clojure.lang.IReference ; extends IMeta; required to support metadata reader syntax?
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

  ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;   clojure.lang.ITransientCollection
  ;; conj, persistent
  (conj [this args]  ; -> ITransientCollection
    (log/trace "ITransientCollection conj")
    (let [item (first args)
          k (clj/name (clj/key item))
          v (clj/val item)
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
          kch (ekey/to-keychain content)
          coll (clj/into {} (for [[k v] props]
                              (let [prop (keyword k)
                                    val (get-val-clj v)]
                                {prop val})))
          res (with-meta coll {:migae/keychain kch
                               :type PersistentEntityMap})]
      (log/trace "persistent result: " (meta res) res (class res))
      res))


  ;; clojure.lang.ITransientMap
  ;; (assoc [this k v]                     ; both assoc! and assoc (?)
  ;;   (let [prop (clj/name k)]
  ;;     (log/trace "ITransientMap assoc: setting prop " k "->" prop v)
  ;;     (.setProperty content prop v)
  ;;     this))
  ;; (without [this k]                     ; = dissoc!, return new datum with k removed
  ;;   (let [prop (clj/name k)]
  ;;     (log/trace "without: removing prop " k "->" prop)
  ;;     (.removeProperty content prop)
  ;;     this))
  ;; (persistent [this]                    ; persistent!
  ;;     (log/trace "ITransientMap persistent")
  ;;   )

  ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;   clojure.lang.Indexed                  ; extends Counted
  ;; (count [this]                         ; Counted
  ;;   (log/trace "count"))
  (nth [this i]                         ; called by get(int index)
    (log/trace "nth"))
;;    (next em-iter)) ;; HACK
  (nth [this i not-found]
    )
  ) ;; end deftype PersistentEntityMap

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; (deftype PersistentEntityMapCollIterator [query]
;;   java.util.Iterator
;;   (hasNext [this]
;;     (log/trace "PersistentEntityMapCollIterator hasNext")
;;     (.hasNext query))
;;   (next    [this]
;;     (log/trace "PersistentEntityMapCollIterator next")
;;     (PersistentEntityMap. (.next query) nil))
;;   ;; (remove  [this])
;;   )

;; load order matters!
;;(load "datastore/service")

;; ;; (load "datastore/hashmap/adapter")
;; ;;(load "datastore/hashmap/ctor_common")
;; (load "datastore/hashmap/predicates")
;; (load "datastore/hashmap/ctor_push")
;; (load "datastore/hashmap/query")
;; (load "datastore/hashmap/ctor_pull")
;; (load "datastore/ekey")
;; (load "datastore/hashmap/dsmap")
;; (load "datastore/hashmap/api")
