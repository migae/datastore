(in-ns 'migae.datastore)

(defn get-kinded-emap
  [keychain & data]
  ;; precon: improper keychain is validated
  ;; (log/trace "get-kinded-emap " keychain)
  (if (> (count keychain) 1)
    ;; kinded descendant query
    (let [ancestor-key (ekey/keychain-to-key (vec (butlast keychain)))
          kind (clj/name (last keychain))
          q (Query. kind ancestor-key)
          prepared-query (.prepare (ds/datastore) q)
          iterator (.asIterator prepared-query)
          seq (iterator-seq iterator)
          res (PersistentEntityMapIterator. seq)]
      ;; (log/trace "seq1: " (type seq))
      res)
    ;; kinded query
    (let [kind (clj/name (last keychain))
          q (Query. kind)
          prepared-query (.prepare (ds/datastore) q)
          iterator (.asIterator prepared-query)
           seq (iterator-seq iterator)
          res (PersistentEntityMapIterator. seq)]
      ;; (log/trace "seq2: " (type seq))
      res)))



  ;; (log/trace "keychain" keychain)
  ;; (log/trace "data" data)
  ;; (let [em (first data)
  ;;       kind (clj/name (last keychain))
  ;;       ;; foo (log/trace "kind" kind)
  ;;       parent-keychain (vec (butlast keychain))
  ;;       e (if (nil? parent-keychain)
  ;;           (Entity. kind)
  ;;           (do #_(log/trace "parent-keychain" parent-keychain)
  ;;               #_(log/trace "parent-key" (ekey/keychain-to-key parent-keychain))
  ;;               (Entity. kind  (ekey/keychain-to-key parent-keychain))
  ;;               ))]
  ;;   (when (not (empty? em))
  ;;     (doseq [[k v] (seq em)]
  ;;       (.setProperty e (subs (str k) 1) (get-val-ds v))))
  ;;   (.put (ds/datastore) e)
  ;;   (PersistentEntityMap. e nil)))

(defn get-prefix-matches
  [keychain]
  ;; precon: keychain has already been validated
  ;; (log/trace "get-prefix-matches" keychain)
  (let [prefix (apply ekey/keychain-to-key keychain)
        q (.setAncestor (Query.) prefix)
        prepared-query (.prepare (ds/datastore) q)
        iterator (.asIterator prepared-query (FetchOptions$Builder/withDefaults))
        seq  (iterator-seq iterator)
        res (PersistentEntityMapIterator. seq)]
    res))

;;   (let [k (ekey/keychain-to-key keychain)
;;         e (.get (ds/datastore) k)]
;; ;;               (catch EntityNotFoundException e nil))]
;;     (PersistentEntityMap. e nil)))

(defn get-proper-emap
  [keychain & data]
  ;; precon: keychain has already been validated
  (log/trace "get-proper-emap" keychain)
  (let [k (ekey/keychain-to-key keychain)
        e (.get (ds/datastore) k)]
;;               (catch EntityNotFoundException e nil))]
    (PersistentEntityMap. e nil)))

;; getter
(defn get-ds
  [keychain & arg2]
  ;; (log/trace "get-ds " keychain arg2)
   (cond
     (= :prefix keychain)
     (if (apply ekey/proper-keychain? arg2)
       (get-prefix-matches arg2))
     (empty? keychain)
     (pull-all)
     (ekey/improper-keychain? keychain)
     (get-kinded-emap keychain)
     (ekey/proper-keychain? keychain)
     (get-proper-emap keychain)
     :else
      (throw (IllegalArgumentException. (str "Invalid keychain" keychain)))))

   ;; (if (clj/empty? keychain)
   ;;   (throw (IllegalArgumentException. "keychain vector must not be empty"))
   ;;   (let [k (ekey/keychain-to-key keychain)
   ;;         e (.get (ds/datastore) k)]
   ;;     (log/trace "key: " k)
   ;;     (log/trace "e: " e)
   ;;     ;; throw  EntityNotFoundException
   ;;                ;; (catch EntityNotFoundException e nil)
   ;;                ;; (catch DatastoreFailureException e (throw e))
   ;;                ;; (catch java.lang.IllegalArgumentException e (throw e)))]
   ;;     (PersistentEntityMap. e nil))))

;; pull constructor
;; (entity-map* [:A/B]) -- identity match
;; (entity-map* :ancestor [:A/B]) -- ancestor filter
;; (entity-map* :ancestor [:A/B :C]) -- ancestor filter, kinded
;;      Query q = new Query("Person").setAncestor(ancestorKey);
;;      https://cloud.google.com/appengine/docs/java/datastore/queries?hl=en#Java_Ancestor_filters,
;; (entity-map* k m) -- match homomorphisms
;; (entity-map* :iso k m) -- match isomorphisms
;; (defmulti entity-map*
;;   "Pull constructor.  Retrieve matching entities or throw NotFound exception."
;;   ;; FIXME:  implement map matching
;;   ;; FIXME:  throw notfound exception
;;     (fn [keychain & valmap]
;;       ;; (log/trace "entity-map*")
;;       ;; (log/trace "keychain: " keychain)
;;       ;; (log/trace "type keychain: " (type keychain))
;;       ;; (log/trace "valmap: " valmap)
;;       ;; (log/trace "type valmap: " (type valmap))
;;       ;; (log/trace "first valmap: " (first valmap))
;;       ;; (log/trace "type first valmap: " (type (first valmap)))
;;       ;; (flush)
;;       [(type keychain) (type (first valmap))]))


(defn entity-map*
  ;; (defmethod entity-map* [clojure.lang.PersistentVector nil]
  ([keychain]
   ;; (log/trace "entity-map* PersistentEntityMap")
   (get-ds keychain))

  ;; (defmethod entity-map* [clojure.lang.Keyword clojure.lang.PersistentVector]
  ([arg1 arg2]
   ;; (log/trace "entity-map* 1" arg1 arg2)
   (if (keyword? arg1)
     ;; mode keyword:  :prefix, :iso, etc.
     (do ;; (log/trace "mode " arg1 " keychain: " arg2)
         (get-ds arg1 arg2))
     ;; else keychain
     (if (ekey/keychain? arg1)
       (get-ds arg1 arg2))))
  ;; modal keychain + propmap filters
  ([arg1 arg2 arg3]
   (log/trace "entity-map* 3" arg1 arg2 arg3)
   (if (keyword? arg1)
     ;; mode keyword:  :prefix, :iso, etc.
     (log/trace "mode " arg1 " keychain: " arg2)
     (throw (RuntimeException. "bad args")))))

;; ;; (defmethod entity-map* [clojure.lang.PersistentVector clojure.lang.PersistentArrayMap]
;;   ([keychain & valmap]
;;   ;; (log/trace "entity-map* PersistentVector")
;;   (get-ds keychain))

(defn emap??                            ;FIXME: use a multimethod?
  "return matching emaps"
  [keylinks & filter-map]  ; keychain and predicate-map
  ;; {:pre []} ;; check types
  (log/trace "emap?? keylinks" keylinks (type keylinks))
  (log/trace "emap?? filter-map" filter-map (type filter-map))
;; )
  (if (clj/empty? keylinks)
    (do
      (log/trace "emap?? predicate-map filter" filter-map (type filter-map))
      )
      ;; (let [ks (clj/keylinks filter-map)
      ;;       vs (vals filter-map)
      ;;       k  (subs (str (first ks)) 1)
      ;;       v (last vs)
      ;;       f (Query$FilterPredicate. k Query$FilterOperator/EQUAL v)]
      ;;   (log/trace (format "key: %s, val: %s" k v))))
    (let [k (if (coll? keylinks)
              (apply ekey/keychain-to-key keylinks)
              (apply ekey/keychain-to-key [keylinks]))
          ;; foo (log/trace "emap?? kw keylinks: " k)
          e (try (.get (ds/datastore) k)
                 (catch EntityNotFoundException e (throw e))
                 (catch DatastoreFailureException e (throw e))
                 (catch java.lang.IllegalArgumentException e (throw e)))]
      (PersistentEntityMap. e nil))))

(declare find-definite-necessarily find-indefinite-necessarily
         throw-bad-keylinks)

;; FIXME:  remove this. instead use entity-map! :force, etc
(defn entity-map?!
  "Find, necessarily.  If keychain is improper (ends in a Kind
  keyword), will create new entity; i.e., ?! means 'find indefinite -
  find _some_ entity of this kind'.  If keychain is proper (all
  keylinks), then ?! means 'find exactly _this_ entity' gets the
  entity; if found, returns it, otherwise creates and returns new
  entity."
  [keylinks & propmap]
  (log/trace "emap?!" keylinks propmap)
  (if (clj/empty? keylinks)
    (throw (IllegalArgumentException. "key vector must not be empty"))
    (if (keyword? (last keylinks))
      (if (nil? (namespace (last keylinks)))
        (apply find-indefinite-necessarily keylinks propmap)
        (apply find-definite-necessarily keylinks propmap))
    (throw-bad-keylinks keylinks))))

(defn emap?!
  [keylinks & propmap]
  (log/trace "emap?!" keylinks propmap)
  (entity-map?! keylinks propmap))


     ;; (let [k (apply ekey/keychain-to-key keychain)
     ;;       e (try (.get (ds/datastore) k)
     ;;              (catch EntityNotFoundException e
     ;;                ;;(log/trace (.getMessage e))
     ;;                e)
     ;;              (catch DatastoreFailureException e
     ;;                ;;(log/trace (.getMessage e))
     ;;                nil)
     ;;              (catch java.lang.IllegalArgumentException e
     ;;                ;;(log/trace (.getMessage e))
     ;;                nil))
     ;;       ]
     ;;   ;; (log/trace "emap! got e: " e)
     ;;   (if (nil? e)
     ;;     (let [e (Entity. k)]
     ;;       (.put (ds/datastore) e)
     ;;       (PersistentEntityMap. e nil))
     ;;     (PersistentEntityMap. e nil))))))


;; (defn emap??
;;   "Find, possibly.  If entity found, return without change, otherwise
;;   throw exception."
;;   ;; [keylinks & propmap]
;;   [keylinks]
;;    (log/trace "emap??" keylinks)
;;    (if (clj/empty? keylinks)
;;      (throw (IllegalArgumentException. "keychain vector must not be empty"))
;;      (if (every? keylink? keylinks)
;;        (let [k (apply keychainer keylinks)
;;              e ;;(try
;;                  (.get (ds/datastore) k)]
;;                     ;; (catch EntityNotFoundException ex (throw ex))
;;                     ;; (catch DatastoreFailureException ex (throw ex))
;;                     ;; (catch java.lang.IllegalArgumentException ex (throw ex)))]
;;          (PersistentEntityMap. e nil))
;;        (throw-bad-keylinks keylinks))))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; (defmulti ancestors
;;   (fn [& {kind :kind key :key name :name id :id}]
;;     (cond
;;      name :kindname
;;      id   :kindid
;;      kind :kind
;;      key  :key
;;      ;; (= (type s) java.lang.String) :kind
;;      ;; (= (type rest) com.google.appengine.api.datastore.Key) :key
;;      :else :kindless)))

;; (defmethod ancestors :key
;;   [& {kind :kind key :key name :name id :id}]
;;   (Query. key)
;;   )

;; (defmethod ancestors :kindname
;;   [& {kind :kind key :key name :name id :id}]
;;   (let [k (dskey/make {:_kind kind :_name name})]
;;         (Query. k))
;;   )

;; (defmethod ancestors :kindid
;;   [& {kind :kind id :id}]
;;   (let [k (dskey/make {:_kind kind :_id id})]
;;         (Query. k))
;;   )
