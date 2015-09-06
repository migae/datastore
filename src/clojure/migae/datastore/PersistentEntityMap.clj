(in-ns 'migae.datastore)

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
          em-iter (PersistentEntityMapIterator. e-iter) ]
    ;; (log/trace "Iterable res:" em-iter)
    em-iter))

  ;; ;; java.util.Map$Entry
  ;; (getKey [this]
  ;;   (log/trace "PersistentEntityMap.java.util.Map$Entry getKey ")
  ;;   (let [k (.getKey content)]
  ;;     (log/trace "key:" k)
  ;;     (ekey/to-keychain k)))
  ;; ;; FIXME: just do (ekey/to-keychain content)??
  ;;     ;;     kind (.getKind k)
  ;;     ;;     nm (.getName k)]
  ;;     ;; [(keyword kind (if nm nm (.getId k)))]))
  ;; (getValue [_]
  ;;   (log/trace "PersistentEntityMap.java.util.Map$Entry getValue ")
  ;;   (let [r (props-to-map (.getProperties content))]
  ;;     (log/trace "r: " r)
  ;;     r))
  ;; ;; (equals [_ o]
  ;; ;;   (log/trace "PersistentEntityMap.java.util.Map$Entry equals ")
  ;; ;;   )
  ;; ;; (hashCode [_]
  ;; ;;   (log/trace "PersistentEntityMap.java.util.Map$Entry hashCode ")
  ;; ;;   )
  ;; (setValue [_ v]
  ;;   (log/trace "PersistentEntityMap.java.util.Map$Entry setValue ")
  ;;   )

  ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;; clojure.lang.IFn
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

  ;; clojure.lang.IMapEntry
  ;; key, val
  ;; (key [this]  ; -> Object
  ;;   (log/trace "PersistentEntityMap.IMapEntry key")
  ;;   (ekey/to-keychain content))
  ;; (val [this]  ; -> Object
  ;;   (log/trace "PersistentEntityMap.IMapEntry val")
  ;;   (.printStackTrace (Exception.))
  ;;   (let [props (.getProperties content)
  ;;         coll (clj/into {} (for [[k v] props]
  ;;                             (let [prop (keyword k)
  ;;                                   val (get-val-clj v)]
  ;;                               {prop val})))
  ;;         keychain (ekey/to-keychain content)
  ;;         ;; res (clj/into coll {:migae/keychain keychain})]
  ;;         ]
  ;;     coll))

  ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;; clojure.lang.IObj ;; extends IMeta
  ;; withMeta; meta
  (^IPersistentMap meta [this]
    ;; (log/trace "IObj.meta" (ekey/to-keychain content))
    (clj/into (some identity [em-meta {}]) {:migae/keychain (ekey/to-keychain content)}))
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
            to-coll (clj/into {} (for [[k v] to-props]
                                     (let [prop (keyword k)
                                           val (get-val-clj v)]
                                       {prop val})))
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
    (log/trace "PersistentEntityMap.count")
    (let [props (.getProperties content)
          c (.size props)]
      c))
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
    ;; (log/trace "IPersistentCollection.cons: " o (type o))
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
        ;; (log/trace "cons PersistentArrayMap to emap")
        (let [newe (.clone content)
              newm (clj/into {} em-meta)]
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
    ;; (log/trace "PersistentEntityMap.equiv")
    (if (instance? migae.datastore.IPersistentEntityMap o)
      true
      ;; FIXME:  implement PropertyContainer to map logic as a function
      (let [props (.getProperties content)
            pmap (clj/into {}
                           (for [[k v] props]
                             (do
                               (let [prop (keyword k)
                                     val (get-val-clj v)]
                                 ;; (log/trace "prop " prop " val " val)
                                 {prop val}))))]
        (= pmap o))))
    ;; (.equals this o))
    ;; (.equals content (.content o)))

  ;;;; extends Seqable
  ;; seq()
  (^ISeq seq [this]
    ;; seq is called by: into, merge, "print", e.g. (log/trace em)
    ;; (log/trace "seq" (type this)) ;; (.hashCode this) (.getKey content))
    ;; (log/trace "    on content" content)
    (let [props (.getProperties content)
          ;; foo (log/trace "props" props)
          emap (clj/into {}
                           (for [[k v] props]
                             (do
                               (let [prop (keyword k)
                                     val (get-val-clj v)]
                                 ;; (log/trace "prop " prop " val " val)
                                 {prop val}))))
          k (ekey/to-keychain content)
          res (with-meta emap {:migae/keychain k})
          ]
      ;; (log/trace "seq result:" #_(meta res) (type res) res)
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
              to-keychain (if (nil? (:migae/keychain to-map))
                            (ekey/to-keychain content)
                            (:migae/keychain to-map))]
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
      (log/trace "persistent result: " res (class res))
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

  ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;; clojure.lang.Indexed                  ; extends Counted
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
(deftype PersistentEntityMapCollIterator [query]
  java.util.Iterator
  (hasNext [this]
    (log/trace "PersistentEntityMapCollIterator hasNext")
    (.hasNext query))
  (next    [this]
    (log/trace "PersistentEntityMapCollIterator next")
    (PersistentEntityMap. (.next query) nil))
  ;; (remove  [this])
  )

;; ;; (load "datastore/adapter")
;; ;; (load "datastore/ctor_common")
;; (load "datastore/predicates")
;; (load "datastore/ctor_push")
;; (load "datastore/query")
;; (load "datastore/ctor_pull")
;; ;;(load "datastore/ekey")
;; (load "datastore/dsmap")
;; (load "datastore/api")
