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

