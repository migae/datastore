(declare Entities)
(declare wrap-entity)

(defn entity-from-entitymap
  [theEntityMap]
  {:pre [;; (do (prn "e map meta: " (meta theEntityMap))
         ;;     (prn "e map id: "   (:id (meta theEntityMap))) true),
         ;; :key not allowed in EntityMap initializers
         (not (nil? (:kind (meta theEntityMap)))),
         ;; one of :id or :name or neither
         (or (nil? (:id (meta theEntityMap)))
             (nil? (:name (meta theEntityMap)))),
         (if (not (nil? (:id (meta theEntityMap))))
           (number? (:id (meta theEntityMap)))
           true),
         (if (not (nil? (:name (meta theEntityMap))))
           (or (string?  (:name (meta theEntityMap)))
               (keyword? (:name (meta theEntityMap))))
           true)
         ;; TODO: validate :parent
         ]}
  (let [{:keys [kind id name parent]} (meta theEntityMap)
        arg2 (if id id (if name name nil))
        arg3 (if (nil? parent) nil
                 (cond
                  (= (type parent)
                     :migae.datastore/Key)
                  ;;no yet
                  nil
                  (= (type parent)
                     :migae.datastore/Entity)
                  (:key (meta parent))
                  :else  ;; type parent = EntityMap
                  (:key (meta (Entities parent)))))
        ;; OR: (ds/keys ds parent)))))
        theEntity (if (nil? parent)
                    (Entity. (clojure.core/name kind)
                             (if id id (if name name)))
                    (Entity. (clojure.core/name kind)
                             (if id id (if name name))
                             arg3))]    ; arg3 = parent Key
          (doseq [[k v] theEntityMap]
            ;; TODO: handle val types
            (.setProperty theEntity
                          (clojure.core/name k)
                          (if (number? v) v
                              (clojure.core/name v))))
          ;; TODO: wrap-entity s/b resonsible for putting if needed
          (.put (datastore) theEntity)
          (wrap-entity theEntity)))


;; QUESTION: do we want to implement a
;; :migae.datastore/Key clojo to go with our
;; :migae.datastore/Entity clojo?

(defn- wrap-entity
  ;; wrap-entity wraps an Entity in a function.  It memoizes metadata
  ;; (key, kind, id, name, etc.)  as a 'keymap' for use as clojure
  ;; metadata.  We could implement access to e.g. :kind as logic in
  ;; the function, but since this data is immutable, there is no
  ;; reason not to memoize it.  (TODO: see about using deftype for
  ;; Entities; problem is metadata)
  ;; BUT: implementing this in the closure amounts to the same thing?
  [theEntity]
  (do ;;(prn "making entity " theEntity)
      (let [theKey (.getKey theEntity)
            kind (keyword (.getKind theEntity))]
        ;; then construct function
        ^{:entity theEntity
          :parent (.getParent theEntity)
          :type ::Entity ;; :migae.datastore/Entity
          :key (.getKey theEntity)
          :kind kind ; (keyword (.getKind theEntity))
          :namespace (.getNamespace theEntity)
          :name (.getName theKey)
          :id (.getId theKey)
          :keystring (.toString theKey)
          :keystringrep (KeyFactory/keyToString theKey)}
        (fn [& kw]
          ;; the main job of the function is to lookup properties
          ;; TODO: accomodate iteration, seq-ing, etc
          ;; e.g.  (clj/into myEnt {:foo "bar"})
          ;; also conj, clj/into, etc.
          ;; e.g.  (conj myEnt {:foo "bar"})
          ;; etc.
          ;; only way I see to do this as of now is local replacement
          ;; funcs in our namespace
          ;; (cond (= kw :kind kind) ...
          (if (nil? kw)
            (let [props (.getProperties theEntity)]
              ;; efficiency?  this constructs map of all props
              ;; every time
              (clj/into {} (map (fn [item]
                              {(keyword (.getKey item))
                               (.getValue item)}) props)))
            (.getProperty theEntity (name kw)))))))


(deftest ^:keys keymap3
  (testing "keymap literals 1"
    (is (= (type (dskey/make {:_kind :Employee :_name "asalieri"}))
           com.google.appengine.api.datastore.Key))
    (is (= (type (dskey/make {:_kind :Employee :_id 99}))
           com.google.appengine.api.datastore.Key))
    ))

(deftest ^:keys ekeymap1
  (testing "entity with keymap literal 2"
    (let [em ^{:_kind :Employee, :_name "asalieri"}
          {:fname "Antonio", :lname "Salieri"}]
      (let [ent (ds/persist em)]
        (is (= (dskey/id (:_key (meta ent))) 0))
        (is (= (dskey/name (:_key (meta ent))) "asalieri"))
        (is (= (dskey/name (dse/key ent)) "asalieri"))
        (is (= (dse/name em)) "asalieri")
        (is (= (dse/name ent)) "asalieri"))
      )))

(deftest ^:keys keys3
  (testing "entitymap deftype keys child"
    (let [key (dskey/make {:_kind :Genus :_name "Felis"})
          child (dskey/child key {:_kind :Genus :_name "Felis"})]
      (is (= (type child)
             com.google.appengine.api.datastore.Key))
      (is (= (dskey/parent child)
             key))
      (should-fail (is (= (dskey/name child)
             "felis catus")))
      (println child)
      )))

;; (deftest ^:one one-off
;;   (testing "entitymap deftype keys parent"
;;     (let [parent (dskey/make {:_kind :Genus :_name "Felis"})]
;;       (log/trace (dskey/kind parent))
;;       (is (= ((dskey/kind parent) :Genus))))))

(deftest ^:keys keys4
  (testing "entitymap deftype keys parent"
    (let [parent (dskey/make {:_kind :Genus :_name "Felis"})
          foo (log/trace "parent " parent)
          child  (dskey/make {:_parent parent :_kind :Species :_name "felis catus"})
          bar (log/trace "child " child)
          ]
      (is (= (type child)
             com.google.appengine.api.datastore.Key))
      (is (= (dskey/parent child)
             parent))
      (is (= (dskey/name child)
             "felis catus"))
      )))

(deftest ^:keys keys5
  (testing "entitymap deftype keys parent"
    (let [parent (dskey/make {:_kind :Genus :_name "Felis"})
          child  (dskey/make {:_parent {:_kind :Genus :_name "Felis"}
                              :_kind :Species :_name "felis catus"})]
      (is (= (type child)
             com.google.appengine.api.datastore.Key))
      (is (= (dskey/parent child)
             parent))
      (is (= (dskey/name child)
             "felis catus"))
      )))


(defn persist
  [theMap]
  {:pre [ ]} ;; TODO: validate entitymap
  (let [{kind :_kind name :_name id :_id parent :_parent} (meta theMap)
        parentEntity (if parent
                       (let [k (:_kind parent)
                             n (:_name parent)
                             i (:_id parent)]
                         (cond name (Entity. (clojure.core/name k) n)
                               id (Entity. (clojure.core/name k) i)
                               :else (Entity. (clojure.core/name k))))
                       nil)
        parentKey (if parent (dse/key parentEntity)
                      nil)
        theEntity (if parentKey
                    (cond name (Entity. (clojure.core/name kind) name parentKey)
                          id (Entity. (clojure.core/name kind) id parentKey)
                          :else (Entity. (clojure.core/name kind) parentKey))
                    (cond name (Entity. (clojure.core/name kind) name)
                          id (Entity. (clojure.core/name kind) id)
                          :else (Entity. (clojure.core/name kind))))]
        (do
          (doseq [[k v] theMap]
            (.setProperty theEntity (clojure.core/name k) v))
          (let [key (.put (datastore) theEntity)
                kw (if (and (not id) (not name)) :_id)
                v  (if (and (not id) (not name)) (dskey/id key))
                m (clj/assoc (meta theMap)
                    kw v
                    :_key key
                    :_entity theEntity)]
            (with-meta theMap m)))))

;; TODO:  support tabular input, each row one entity
(defn persist-list
  [kind theList]
  ;; (log/trace "persist kind" kind)
  ;; (log/trace "persist list:" theList)
  (doseq [item theList]
    (let [theEntity (Entity. (clojure.core/name kind))]
      (doseq [[k v] item]
        ;; (log/trace "item" k (type v))
        (.setProperty theEntity (clojure.core/name k)
                      (cond
                       (= (type v) clojure.lang.Keyword) (clojure.core/name v)
                       :else v)))
      (.put (datastore) theEntity)))
  true)

      ;;       kw (if (and (not id) (not name)) :_id)
      ;;       v  (if (and (not id) (not name)) (dskey/id key))
      ;;       m (assoc (meta theMap)
      ;;           kw v
      ;;           :_key key
      ;;           :_ent theEntity)]
      ;; (with-meta theMap m)))))

;; ################
;; (defn fetch
;;   ([^String kind] )
;;   ([^Key key] (dss/get key))
;;   ([^String kind ^String name] (let [key ...] (ds/get key)))
;;   ([^String kind ^Long id] (let [key ...] (ds/get key)))

(defmulti fetch
  (fn [{key :_key kind :_kind name :_name id :_id parent :_parent :as args}]
    (cond
     (= (type args) com.google.appengine.api.datastore.Key) :key
     parent :parent
     key  :keymap
     (and kind name) :kindname
     (and kind id)   :kindid
     kind :keysonly
     :else :bug)))

(defmethod fetch :bug
 [{key :_key kind :_kind name :_name id :_id :as args}]
  (log/trace "fetch method :bug, " args))

(defmethod fetch :key
  [key]
  {:pre [(= (type key) com.google.appengine.api.datastore.Key)]}
  (let [ent (.get (datastore) key)
        kind (dskey/kind key)
        name (dskey/name key)
        id (dskey/id   key)
        props  (clj/into {} (.getProperties ent))] ;; java.util.Collections$UnmodifiableMap???
    (with-meta
      (clj/into {} (for [[k v] props] [(keyword k) v]))
      {:_kind kind :_name name :_key key :_entity ent})))

(defmethod fetch :keymap
  [{key :_key}]
  {:pre [(= (type key) com.google.appengine.api.datastore.Key)]}
  (let [ent (.get (datastore) key)
        kind (dskey/kind key)
        name (dskey/name key)
        id (dskey/id   key)
        props  (clj/into {} (.getProperties ent))] ;; java.util.Collections$UnmodifiableMap???
    (with-meta
      (clj/into {} (for [[k v] props] [(keyword k) v]))
      {:_kind kind :_name name :_key key :_entity ent})))

(defmethod fetch :parent
  [{key :_key kind :_kind name :_name id :_id parent :_parent :as args}]
  (let [parentKey (dskey/make {:_kind (:_kind parent) :_name (:_name parent)})
        childKey (dskey/make {:_kind kind :_name name :_parent parent})
        ;; childKey (dskey/make parentKey kind name)
        ent (.get (datastore) childKey)
        kind (dskey/kind childKey)
        name (dskey/name childKey)
        id (dskey/id   childKey)
        props  (clj/into {} (.getProperties ent))]
    (with-meta
      (clj/into {} (for [[k v] props] [(keyword k) v]))
      {:_kind kind :_name name :_key childKey :_entity ent})))

(defmethod fetch :kindname
  ;; validate kind, name
  ;; {:pre [ ]}
  [{kind :_kind name :_name}]
  (let [foo (log/debug (format "fetching kind %s name %s" kind name))
        key (try (dskey/make {:_kind kind :_name name})
                 (catch Exception e nil))]
    (if key
      (let [ent (.get (datastore) key)
            props  (clj/into {} (.getProperties ent))]
        ;; TODO: if not found, return nil
        (with-meta
          (clj/into {} (for [[k v] props] [(keyword k) v]))
          {:_kind kind :_name name :_key key :_entity ent}))
      nil)))

(defmethod fetch :kindid
  ;; {:pre [ ]}
  [{kind :_kind id :_id}]
  (let [key (dskey/make {:_kind kind :_id id})
        ent (.get (datastore) key)
        props (clj/into {} (.getProperties ent))] ;; java.util.Collections$UnmodifiableMap???
    (with-meta
      (clj/into {} (for [[k v] props] [(keyword k) v]))
      {:_kind kind :_id id :_key key :_entity ent})))

;; see also dsqry/fetch
(defmethod fetch :keysonly
  ;; {:pre [ ]}
  [{kind :_kind :as args}]
  (let [q (dsqry/keys-only kind)
        foo (log/debug "isKeysOnly? " (.isKeysOnly q))
        pq (dsqry/prepare q)
        bar (log/debug "resulting prepq: " pq)
        c (log/debug "count " (dsqry/count pq))]
    (dsqry/run pq)))

(defmulti ptest
  (fn
    ([arg1 arg2]
       (cond
        ;; :_kind :Person
        (= arg1 :_kind) (do (log/trace "dispatching on kw " arg2) :kind)
        ;; :Person '(:sex = :M)
        (list? arg2) (do (log/trace "dispatching on kind filter "
                                    (infix/infix-to-prefix arg2)) :kindfilter)
        :else :bug)
        )
    ([kw kind filter]
       (let [f (infix/infix-to-prefix filter)]
         (do (println "ptest kw " kind)
             (println "ptest kw" (type kind))
             (log/trace "kw filter: " f)
             (cond
              ;; :_kind :Person '(:age >= 18)
              (list? filter) (do (log/trace "dispatching on kw filter "
                                    (infix/infix-to-prefix filter)) :kwfilter)
              (map? kw) (do (log/trace "map " kw) :map)
              (vector? kw) (do (let [a (apply hash-map kw)]
                                  (log/trace kw) :v))
              :else :bug
              ))))
    ;; ([kw kind & args]
    ;;    (let [a (apply hash-map kw kind args)
    ;;          f (:filter a)]
    ;;      (do (println "ptest a " a)
    ;;          (println "ptest a" (type a))
    ;;          (log/trace "filter: " (infix/infix-to-prefix f))
    ;;          (cond
    ;;           (map? a) (do (log/trace "map " a) :map)
    ;;           (vector? a) (do (let [a (apply hash-map a)]
    ;;                               (log/trace a) :v))
    ;;           :else :bug
    ;;           ))))
    ([{kind :_kind :as args}]
       (do (println "ptest b" args)
           (println "ptest b" (type args))
           (cond
            ;; check for null kind
            (keyword? args) (do (log/trace "dispatching on kind " args) :kind)
            (map? args) (do (log/trace "map " args) :map)
            (vector? args) (do (let [a (apply hash-map args)]
                                 (log/trace a) :v))
            :else :bug
            )))))

(defmethod ptest :bug
  [arg & args]
  (log/trace "ptest bug: " args)
  (log/trace "ptest bug: " (type args))
  )

(defmethod ptest :map
  [arg & args]
  )

(defmethod ptest :kw
  [arg1 arg2]
  )

(defmethod ptest :kind
  [arg & args]
  )

(defmethod ptest :kindfilter
  [kind filter]
  )

(defmethod ptest :kwfilter
  [kw kind filter]
  )

(defmethod ptest :v
  [arg & args]
  )
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;; An alternative: use defProtocol

(defn dump-entity [theEntity]
  (do
    (prn "****************")
    (prn "Dumping entity: " theEntity)
    (prn "entity: " ((meta theEntity) :entity))
    (prn "keymap: "(meta theEntity))
    (prn "entitymap: " (theEntity))
    (prn "****************")
    ))


;; (defn- get-next-emap-prop [this]
;;   ;; (log/debug "get-next-emap-prop" (.query this))
;;   (let [r (.next (.query this))]
;;     ;; (log/debug "next: " r)
;;     r))

;; ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; ;;;;  utils
;; (defn print
;;   [em]
;;   (binding [*print-meta* true]
;;     (prn em)))

;; (defn print-str
;;   [em]
;;   (binding [*print-meta* true]
;;     (pr-str em)))

;; (defn println-str
;;   [em]
;;   (binding [*print-meta* true]
;;     (prn-str em)))

;; (defn println
;;   [^migae.datastore.IPersistentEntityMap em]
;;   (binding [*print-meta* true]
;;     (prn em)))

;; (defn dump
;;   [msg datum data]
;;   (binding [*print-meta* true]
;;     (log/debug msg (pr datum) (pr data))))

;; ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; ;; (deftype PersistentEntityMapCollIterator [query]
;; ;;   java.util.Iterator
;; ;;   (hasNext [this]
;; ;;     (log/debug "PersistentStoreMap hasNext")
;; ;;     (.hasNext query))
;; ;;   (next    [this]
;; ;;     (log/debug "PersistentStoreMap next")
;; ;;     (->PersistentEntityMap (.next query) nil))
;; ;;   ;; (remove  [this])
;; ;;   )

;; ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; (defn- keyword-to-ds
;;   [kw]
;;    (KeyFactory/createKey "Keyword"
;;                          ;; remove leading ':'
;;                          (subs (str kw) 1)))

;; (defn- symbol-to-ds
;;   [sym]
;;    (KeyFactory/createKey "Symbol" (str sym)))

;; (defn- ds-to-clj-coll
;;   "Type conversion: java to clojure"
;;   [coll]
;;   ;; (log/debug "ds-to-clj-coll" coll (type coll))
;;   (cond
;;     (= (type coll) java.util.ArrayList) (into '() (for [item coll]
;;                                                        (ds-to-clj item)))
;;     (= (type coll) java.util.HashSet)  (into #{} (for [item coll]
;;                                                        (ds-to-clj item)))
;;     (= (type coll) java.util.Vector)  (into [] (for [item coll]
;;                                                        (ds-to-clj item)))
;;     :else (log/debug "EXCEPTION: unhandled coll " coll)
;;     ))

;; (defn- clj-to-ds-coll
;;   "Type conversion: clojure to java.  The datastore supports a limited
;;   number of Java classes (see
;;   https://cloud.google.com/appengine/docs/java/datastore/entities#Java_Properties_and_value_types);
;;   e.g. no BigInteger, no HashMap, etc.  Before we can store a
;;   collection we have to convert its elements to acceptable types.  In
;;   particular, maps must be converted to EmbeddedEntity objects"
;;   ;; {:tag "[Ljava.lang.Object;"
;;   ;;  :added "1.0"
;;   ;;  :static true}
;;   [coll]
;;   ;; (log/debug "clj-to-ds-coll" coll (type coll))
;;   (cond
;;     (list? coll) (let [a (ArrayList.)]
;;                      (doseq [item coll]
;;                        (do
;;                          ;; (log/debug "vector item:" item (type item))
;;                          (.add a (clj-to-ds item))))
;;                      ;; (log/debug "ds converted:" coll " -> " a)
;;                      a)

;;     (map? coll) (make-embedded-entity coll)

;;     (set? coll) (let [s (java.util.HashSet.)]
;;                   (doseq [item coll]
;;                     (let [val (clj-to-ds item)]
;;                       ;; (log/debug "set item:" item (type item))
;;                       (.add s (clj-to-ds item))))
;;                   ;; (log/debug "ds converted:" coll " -> " s)
;;                   s)

;;     (vector? coll) (let [a (Vector.)]
;;                      (doseq [item coll]
;;                        (do
;;                          ;; (log/debug "vector item:" item (type item))
;;                          (.add a (clj-to-ds item))))
;;                      ;; (log/debug "ds converted:" coll " -> " a)
;;                      a)

;;     :else (do
;;             (log/debug "HELP" coll)
;;             coll))
;;     )

;; ;; this is for values to be printed (i.e. from ds to clojure)
;; (defn- ds-to-clj
;;   [v]
;;   ;; (log/debug "ds-to-clj:" v (type v) (class v))
;;   (let [val (cond (integer? v) v
;;                   (string? v) (str v)
;;                   (= (class v) java.lang.Double) v
;;                   (= (class v) java.lang.Boolean) v
;;                   (= (class v) java.util.Date) v

;;                   (= (class v) java.util.Collections$UnmodifiableMap)
;;                   (let [props v]
;;                     (into {} (for [[k v] props]
;;                                (let [prop (keyword k)
;;                                      val (ds-to-clj v)]
;;                                  {prop val}))))

;;                   (instance? java.util.Collection v) (ds-to-clj-coll v)
;;                   (= (type v) Link) (.toString v)

;;                   (= (type v) Email) (.getEmail v)
;;                   (= (type v) EmbeddedEntity) (->PersistentEntityMap v nil)
;;                   (= (type v) Key) (let [kind (.getKind v)]
;;                                      (if (= kind "Keyword")
;;                                        (keyword (.getName v))
;;                                        ;; (symbol (.getName v))))
;;                                        (str \' (.getName v))))
;;                   :else (do
;;                           ;; (log/debug "HELP: ds-to-clj else " v (type v))
;;                           (throw (RuntimeException. (str "HELP: ds-to-clj else " v (type v))))
;;                           (edn/read-string v)))]
;;     ;; (log/debug "ds-to-clj result:" v val)
;;     val))

;; ;; this is for values to be stored (i.e. from clojure to ds java types)
;; (defn clj-to-ds
;;   [v]
;;   ;; (log/debug "clj-to-ds" v (type v))
;;   (let [val (cond (integer? v) v
;;                   (string? v) (str v)
;;                   (coll? v) (clj-to-ds-coll v)
;;                   (= (type v) clojure.lang.Keyword) (keyword-to-ds v)
;;                   (= (type v) clojure.lang.Symbol) (symbol-to-ds v)
;;                   (= (type v) EmbeddedEntity) v
;;                   (= (type v) Link) v
;;                   (= (type v) Email) v
;;                   (= (type v) Key) v
;;                   (= (type v) java.lang.Double) v
;;                   (= (type v) java.lang.Long) v
;;                   (= (type v) java.lang.Boolean) v
;;                   (= (type v) java.util.Date) v
;;                   (= (type v) java.util.ArrayList) v ;; (into [] v)
;;                   :else (do
;;                           (log/debug "ELSE: get val type" v (type v))
;;                           v))]
;;     ;; (log/debug "clj-to-ds result:" v " -> " val "\n")
;;     val))

;; (defn props-to-map
;;   [props]
;;   (into {} (for [[k v] props]
;;                  (let [prop (keyword k)
;;                        val (ds-to-clj v)]
;;                    {prop val}))))

;; ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; ;;;;  predicates
;; ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; (declare keychain? keychain=? key? key=? map=? entity-map=? dogtag)

;; (defn emap? ;; OBSOLETE - use entity-map?
;;   [em]
;;   (entity-map? em))

;; (defn entity?
;;   [e]
;;   (= (type e) Entity))

;; ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; ;;;;  key stuff
;; ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; (defn keylink?
;;   [k]
;;   ;; (log/debug "keylink?" k (and (keyword k)
;;   ;;                              (not (nil? (namespace k)))))
;;   (or (and (keyword? k)
;;            (not (nil? (namespace k))))
;;       (= (type k) com.google.appengine.api.datastore.Key)))

;; (defn proper-keychain?
;;   [k]
;;   {:pre [(and (vector? k) (not (empty? k)))]}
;;   ;; (log/debug "proper-keychain?: " k)
;;   (if (every? keylink? k)
;;       true
;;       false))
;; (defn improper-keychain?
;;   [k]
;;   {:pre [(and (vector? k) (not (empty? k)))]}
;;   ;; (log/debug "improper-keychain?: " k)
;;   (if (every? keylink? (butlast k))
;;     (let [dogtag (last k)]
;;       ;; (log/debug "DOGTAG K" dogtag)
;;       (and (keyword? dogtag)
;;            (nil? (namespace dogtag))))
;;     false))

;; (defn keychain?
;;   [k]
;;   (and
;;    (vector? k)
;;    (or (proper-keychain? k) (improper-keychain? k))))
;;    ;; (and (vector? k) (not (empty? k)))

;; ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; ;; (defmulti kind class)
;; ;; (defmethod kind Key
;; ;;   [^Key k]
;; ;;   (keyword (.getKind k)))
;; ;; (defmethod kind Entity
;; ;;   [^Entity e]
;; ;;   (keyword (.getKind e)))
;; ;; (defmethod kind migae.datastore.IPersistentEntityMap
;; ;;   [^migae.datastore.PersistentEntityMap e]
;; ;;   (log/debug "IPersistentEntityMap.kind")
;; ;;   (keyword (.getKind (.content e))))
;; ;; ;; (defmethod kind migae.datastore.PersistentEntityHashMap
;; ;; ;;   [^migae.datastore.PersistentEntityMap e]
;; ;; ;;   ;; (log/debug "PersistentEntityHashMap.kind")
;; ;; ;;   (kind (.k e)))
;; ;; (defmethod kind clojure.lang.Keyword
;; ;;   [^clojure.lang.Keyword kw]
;; ;;   (when-let [k (namespace kw)]
;; ;;     (keyword k)))
;; ;;     ;; (clojure.core/name kw)))
;; ;; (defmethod kind clojure.lang.PersistentVector
;; ;;   [^clojure.lang.PersistentVector k]
;; ;;   ;; FIXME: validate keychain contains only keylinks
;; ;;   (if (keychain? k)
;; ;;     (keyword (namespace (last k)))))

;; ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; ;; (defmulti identifier class)
;; ;; (defmethod identifier Key
;; ;;   [^Key k]
;; ;;   ;; (log/debug "Key identifier" k)
;; ;;   (let [nm (.getName k)
;; ;;         id (.getId k)]
;; ;;     (if (nil? nm) id (str nm))))
;; ;; (defmethod identifier migae.datastore.PersistentEntityMap
;; ;;   [^migae.datastore.PersistentEntityMap em]
;; ;;   ;; (log/debug "PersistentEntityMap.identifier")
;; ;;   (let [k (.getKey (.content em))
;; ;;         nm (.getName k)
;; ;;         id (.getId k)]
;; ;;     (if (nil? nm) id (str nm))))
;; ;; ;; (defmethod identifier migae.datastore.PersistentEntityHashMap
;; ;; ;;   [^migae.datastore.PersistentEntityHashMap em]
;; ;; ;;   ;; (log/debug "PersistentEntityHashMap.identifier")
;; ;; ;;   (let [fob (dogtag (.k em))
;; ;; ;;         nm (read-string (name fob))]
;; ;; ;;     nm))

;; ;; (defmethod identifier clojure.lang.PersistentVector
;; ;;   [^clojure.lang.PersistentVector keychain]
;; ;;   ;; FIXME: validate vector contains only keylinks
;; ;;   (let [k (last keychain)]
;; ;;     (if-let [nm (.getName k)]
;; ;;       nm
;; ;;       (.getId k))))

;; ;; (defmulti ename class)
;; ;; (defmethod ename Entity
;; ;;   [^Entity e]
;; ;;   (.getName (.getKey e)))
;; ;; (defmethod ename clojure.lang.PersistentVector
;; ;;   [^ clojure.lang.PersistentVector keychain]
;; ;;   (name (last keychain)))
;; ;; (defmethod ename migae.datastore.PersistentEntityMap
;; ;;   [^migae.datastore.PersistentEntityMap em]
;; ;;   (.getName (.getKey (.content em))))

;; (defmulti id class)
;; (defmethod id clojure.lang.PersistentVector
;;   [ks]
;;    (let [keylink (last ks)]
;;      (.getId keylink)))
;; (defmethod id Key
;;   [^Key k]
;;   (.getId k))
;; (defmethod id Entity
;;   [^Entity e]
;;   (.getId (.getKey e)))
;; (defmethod id migae.datastore.PersistentEntityMap
;;   [^migae.datastore.PersistentEntityMap e]
;;   (.getId (.getKey (.content e))))

;; (defn ekey? [^com.google.appengine.api.datastore.Key k]
;;   (= (type k) com.google.appengine.api.datastore.Key))

;; ;; (defmulti to-keychain class)
;; ;; (defmethod to-keychain Key
;; ;;   [k]
;; ;;   (keychain k))
;; ;; (defmethod to-keychain migae.datastore.PersistentEntityMap
;; ;;   [em]
;; ;;   (keychain (.getKey (.content em))))

;; ;; ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; ;; (defmulti keychain
;; ;;  class)

;; ;; (defmethod keychain nil
;; ;;   [x]
;; ;;   nil)

;; ;; (defmethod keychain Entity
;; ;;   [^Entity e]
;; ;;   ;; (log/debug "keychain co-ctor 1: entity" e)
;; ;;   (keychain (.getKey e)))

;; ;; (defmethod keychain EmbeddedEntity
;; ;;   [^EmbeddedEntity e]
;; ;;   ;; (log/debug "keychain co-ctor 1: entity" e)
;; ;;   (keychain (.getKey e)))

;; ;; (defmethod keychain Key
;; ;;   [^Key k]
;; ;;   {:pre [(not (nil? k))]}
;; ;;   ;; (log/debug "keychain co-ctor 2: key" k)
;; ;;   (let [kind (.getKind k)
;; ;;         nm (.getName k)
;; ;;         id (str (.getId k))
;; ;;         dogtag (keyword kind (if nm nm id))
;; ;;         res (if (.getParent k)
;; ;;               (conj (list dogtag) (keychain (.getParent k)))
;; ;;               (list dogtag))]
;; ;;     ;; (log/debug "kind" kind "nm " nm " id " id " parent " (.getParent k))
;; ;;     ;; (log/debug "res: " res)
;; ;;     ;; (log/debug "res2: " (vec (flatten res)))
;; ;;     (vec (flatten res))))

;; ;; (defmethod keychain migae.datastore.PersistentEntityMap
;; ;;   [^PersistentEntityMap e]
;; ;;   (log/debug "keychain IPersistentEntityMap: " e)
;; ;;   (keychain (.getKey (.content e))))

;; ;; ;; (defmethod keychain migae.datastore.PersistentEntityHashMap
;; ;; ;;   [^PersistentEntityHashMap e]
;; ;; ;;   (log/debug "to-keychain IPersistentEntityMap: " e)
;; ;; ;;   (.k e))

;; ;; (defmethod keychain clojure.lang.PersistentVector
;; ;;   [keychain]
;; ;;   (if (keychain? keychain)
;; ;;     keychain
;; ;;     (throw (IllegalArgumentException.
;; ;;             (str "Invalid keychain: " keychain)))))

;; ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; (declare keyword-to-key)
;; ;; (defn add-child-keylink
;; ;;   [^KeyFactory$Builder builder chain]
;; ;;   ;; (log/debug "add-child-keylink builder:" builder)
;; ;;   ;; (log/debug "add-child-keylink chain:" chain (type chain) (type (first chain)))
;; ;;   (doseq [kw chain]
;; ;;     (if (nil? kw)
;; ;;       nil
;; ;;       (if (keylink? kw)
;; ;;         (let [k (keyword-to-key kw)]
;; ;;           (.addChild builder
;; ;;                      (.getKind k)
;; ;;                      (ds/identifier k)))
;; ;;         (throw (IllegalArgumentException.
;; ;;                 (str "not a clojure.lang.Keyword: " kw)))))))
;; ;; (throw (RuntimeException. (str "Bad child keylink (not a clojure.lang.Keyword): " kw)))))))

;; ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; ;; FIXME: restrict keychain-to-key to proper keychains
;; ;; ;; (defmethod keychain-to-key clojure.lang.PersistentVector
;; ;; (defn keychain-to-key
;; ;;   ;; FIXME: validate keychain only keylinks
;; ;;   ([keychain]
;; ;;   ;; (log/debug "keychain-to-key: " keychain (type keychain) " : " (vector? keychain))
;; ;;    (if (proper-keychain? keychain)
;; ;;      (let [k (keyword-to-key (first keychain))
;; ;;            root (KeyFactory$Builder. k)]
;; ;;        (.getKey (doto root (add-child-keylink (rest keychain)))))
;; ;;      (throw (IllegalArgumentException.
;; ;;              (str "Invalid keychain: " keychain))))))
;; ;; (throw (RuntimeException. (str "Bad keychain (not a vector of keywords): " keychain))))))


;; ;; FIXME: make this an internal helper method
;; ;; (defmethod keychain-to-key clojure.lang.Keyword
;; (defn keyword-to-key
;;   [^clojure.lang.Keyword k]
;;    "map single keyword to key."
;; ;;     {:pre [(= (type k) clojure.lang.Keyword)]}
;;      ;; (log/debug "keyword-to-key:" k (type k))
;;      (if (not (= (type k) clojure.lang.Keyword))
;;         (throw (IllegalArgumentException.
;;                 (str "not a clojure.lang.Keyword: " k))))
;;        ;; (throw (RuntimeException. (str "Bad keylink (not a clojure.lang.Keyword): " k))))
;;      (let [kind (clojure.core/namespace k)
;;            ident (edn/read-string (clojure.core/name k))]
;;        ;; (log/debug (format "keychain-to-key 1: kind=%s, ident=%s" kind ident))
;;        (cond
;;         (nil? kind)
;;         ;;(throw (RuntimeException. (str "Improper keylink (missing namespace): " k)))
;;         (throw (IllegalArgumentException.
;;                 (str "missing namespace: " k)))
;;         (integer? ident)
;;         (KeyFactory/createKey kind ident)
;;         (symbol? ident)                  ;; edn reader makes symbols
;;         (let [s (str ident)]
;;           (cond
;;            (= (first s) \d)
;;            (let [id (edn/read-string (apply str (rest s)))]
;;              (if (= (type id) java.lang.Long)
;;                (KeyFactory/createKey kind id)
;;                (KeyFactory/createKey kind s)))
;;            (= (first s) \x)
;;            (if (= (count s) 1)
;;              (KeyFactory/createKey kind s)
;;              (let [id (edn/read-string (str "0" s))]
;;                (if (= (type id) java.lang.Long)
;;                  (KeyFactory/createKey kind id)
;;                  (KeyFactory/createKey kind s))))
;;            :else
;;            (KeyFactory/createKey kind s)))
;;         :else
;;         (throw (IllegalArgumentException.
;;                 (str k))))))
;;      ;; (throw (RuntimeException. (str "Bad keylink: " k))))))

;; ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; (defmulti dogtag class)
;; (defmethod dogtag Key
;;   [k]
;;   (let [keychain (keychain k)]
;;     (last keychain)))
;; (defmethod dogtag migae.datastore.PersistentEntityMap
;;   [em]
;;   (let [keychain (keychain (.getKey (.content em)))]
;;     (last keychain)))
;; (defmethod dogtag clojure.lang.PersistentVector
;;   [keychain]
;;   ;; FIXME: validate vector contains only keylinks
;;   (if (every? keylink? keychain)
;;     (last keychain)))

;; ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; (defmulti entity-key class)
;; (defmethod entity-key Key
;;   [^Key k]
;;   k)
;; (defmethod entity-key migae.datastore.PersistentEntityMap
;;   [^migae.datastore.PersistentEntityMap e]
;;   (.getKey (.content e)))
;; (defmethod entity-key com.google.appengine.api.datastore.Entity
;;   [^Entity e]
;;   (.getKey e))
;; (defmethod entity-key clojure.lang.Keyword
;;   [^clojure.lang.Keyword k]
;;   (keychain-to-key [k]))
;; (defmethod entity-key clojure.lang.PersistentVector
;;   [kchain]
;;   ;; FIXME: validate vector contains only keylinks
;;   (keychain-to-key kchain))

;; ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; (defn keykind?
;;   [k]
;;   (log/debug "keykind?" k (and (keyword k)
;;                                (not (nil? (namespace k)))))
;;   (and (keyword? k) (nil? (namespace k))))

;; (load "datastore/ctor_local")
;; (load "datastore/query")
;; (load "datastore/ctor_push")
;; (load "datastore/ctor_pull")
;;(load "datastore/impls")
