(in-ns 'migae.datastore)

(declare get-ds)

(defn entity-map*
  ;; (defmethod entity-map* [clojure.lang.PersistentVector nil]
  ([keychain]
   (log/debug "entity-map* k" keychain)
   (let [r (get-ds keychain)]
     (log/debug "entity-map* result: " r)
     r))
  ;; (defmethod entity-map* [clojure.lang.Keyword clojure.lang.PersistentVector]
  ([arg1 arg2]
   (log/debug "entity-map* 1" arg1 arg2)
   (if (keyword? arg1)
     ;; mode keyword:  :prefix, :iso, etc.
     (do ;; (log/debug "mode " arg1 " keychain: " arg2)
         (get-ds arg1 arg2))
     ;; else keychain
     (if (improper-keychain? arg1)
       (get-ds arg1 arg2))))
  ;; modal keychain + propmap filters
  ([arg1 arg2 arg3]
   (log/debug "entity-map* 3" arg1 arg2 arg3)
   (if (keyword? arg1)
     ;; mode keyword:  :prefix, :iso, etc.
     (log/debug "mode " arg1 " keychain: " arg2)
     (throw (RuntimeException. "bad args")))))

;; ;; (defmethod entity-map* [clojure.lang.PersistentVector clojure.lang.PersistentArrayMap]
;;   ([keychain & valmap]
;;   ;; (log/debug "entity-map* PersistentVector")
;;   (get-ds keychain))

;;   ;; (log/debug "keychain" keychain)
;;   ;; (log/debug "data" data)
;;   ;; (let [em (first data)
;;   ;;       kind (name (last keychain))
;;   ;;       ;; foo (log/debug "kind" kind)
;;   ;;       parent-keychain (vec (butlast keychain))
;;   ;;       e (if (nil? parent-keychain)
;;   ;;           (Entity. kind)
;;   ;;           (do #_(log/debug "parent-keychain" parent-keychain)
;;   ;;               #_(log/debug "parent-key" (keychain-to-key parent-keychain))
;;   ;;               (Entity. kind  (keychain-to-key parent-keychain))
;;   ;;               ))]
;;   ;;   (when (not (empty? em))
;;   ;;     (doseq [[k v] (seq em)]
;;   ;;       (.setProperty e (subs (str k) 1) (get-val-ds v))))
;;   ;;   (.put store-map e)
;;   ;;   (PersistentEntityMap. e nil)))

;; (defn get-prefix-matches
;;   [keychain]
;;   ;; precon: keychain has already been validated
;;   (log/debug "get-prefix-matches" keychain)
;;   (let [prefix (apply keychain-to-key keychain)
;;         q (.setAncestor (Query.) prefix)
;;         prepared-query (.prepare store-map q)
;;         iterator (.asIterator prepared-query (FetchOptions$Builder/withDefaults))
;;         seq  (iterator-seq iterator)
;;         res (PersistentEntityMapSeq. seq)]
;;     res))

;; ;;   (let [k (keychain-to-key keychain)
;; ;;         e (.get (.content store-map) k)]
;; ;; ;;               (catch EntityNotFoundException e nil))]
;; ;;     (PersistentEntityMap. e nil)))

;; ;; getter
;; (defn get-ds
;;   [keychain & args]
;;   (log/debug "get-ds " keychain args)
;;    (cond
;;      (= :prefix keychain)
;;      (if (proper-keychain? args)
;;        (get-prefix-matches args))
;;      (empty? keychain)
;;      (pull-all)
;;      (improper-keychain? keychain)
;;      (do
;;        (log/debug "improper-keychain: " keychain)
;;        (let [r (get-kinded-emap keychain)]
;;          (log/debug "get-kinded-emap res: " r)))
;;      (proper-keychain? keychain)
;;      (do
;;        (log/debug "proper keychain: " keychain)
;;        (let [r (get-proper-emap keychain)]
;;          r))
;;      :else
;;       (throw (IllegalArgumentException. (str "Invalid keychain" keychain)))))

;;    ;; (if (empty? keychain)
;;    ;;   (throw (IllegalArgumentException. "keychain vector must not be empty"))
;;    ;;   (let [k (keychain-to-key keychain)
;;    ;;         e (.get (.content store-map) k)]
;;    ;;     (log/debug "key: " k)
;;    ;;     (log/debug "e: " e)
;;    ;;     ;; throw  EntityNotFoundException
;;    ;;                ;; (catch EntityNotFoundException e nil)
;;    ;;                ;; (catch DatastoreFailureException e (throw e))
;;    ;;                ;; (catch java.lang.IllegalArgumentException e (throw e)))]
;;    ;;     (PersistentEntityMap. e nil))))

;; (defn emap??                            ;FIXME: use a multimethod?
;;   "return matching emaps"
;;   [keylinks & filter-map]  ; keychain and predicate-map
;;   ;; {:pre []} ;; check types
;;   (log/debug "emap?? keylinks" keylinks (type keylinks))
;;   (log/debug "emap?? filter-map" filter-map (type filter-map))
;; ;; )
;;   (if (empty? keylinks)
;;     (do
;;       (log/debug "emap?? predicate-map filter" filter-map (type filter-map))
;;       )
;;       ;; (let [ks (keylinks filter-map)
;;       ;;       vs (vals filter-map)
;;       ;;       k  (subs (str (first ks)) 1)
;;       ;;       v (last vs)
;;       ;;       f (Query$FilterPredicate. k Query$FilterOperator/EQUAL v)]
;;       ;;   (log/debug (format "key: %s, val: %s" k v))))
;;     (let [k (if (coll? keylinks)
;;               (apply keychain-to-key keylinks)
;;               (apply keychain-to-key [keylinks]))
;;           ;; foo (log/debug "emap?? kw keylinks: " k)
;;           e (try (.get (.content store-map) k)
;;                  (catch EntityNotFoundException e (throw e))
;;                  (catch DatastoreFailureException e (throw e))
;;                  (catch java.lang.IllegalArgumentException e (throw e)))]
;;       (PersistentEntityMap. e nil))))

;; (declare find-definite-necessarily find-indefinite-necessarily
;;          throw-bad-keylinks)

;; ;; FIXME:  remove this. instead use entity-map! :force, etc
;; (defn entity-map?!
;;   "Find, necessarily.  If keychain is improper (ends in a Kind
;;   keyword), will create new entity; i.e., ?! means 'find indefinite -
;;   find _some_ entity of this kind'.  If keychain is proper (all
;;   keylinks), then ?! means 'find exactly _this_ entity' gets the
;;   entity; if found, returns it, otherwise creates and returns new
;;   entity."
;;   [keylinks & propmap]
;;   (log/debug "emap?!" keylinks propmap)
;;   (if (empty? keylinks)
;;     (throw (IllegalArgumentException. "key vector must not be empty"))
;;     (if (keyword? (last keylinks))
;;       (if (nil? (namespace (last keylinks)))
;;         (apply find-indefinite-necessarily keylinks propmap)
;;         (apply find-definite-necessarily keylinks propmap))
;;     (throw-bad-keylinks keylinks))))

;; ;;   emap?! = find necessarily - return if found (ignoring props arg)
;; ;;   else create, i.e. either find what's already there or "find"
;; ;;   what's passed as props arg.
;; (defn emap?!
;;   [keylinks & propmap]
;;   (log/debug "emap?!" keylinks propmap)
;;   (entity-map?! keylinks propmap))


;;      ;; (let [k (apply keychain-to-key keychain)
;;      ;;       e (try (.get (.content store-map) k)
;;      ;;              (catch EntityNotFoundException e
;;      ;;                ;;(log/debug (.getMessage e))
;;      ;;                e)
;;      ;;              (catch DatastoreFailureException e
;;      ;;                ;;(log/debug (.getMessage e))
;;      ;;                nil)
;;      ;;              (catch java.lang.IllegalArgumentException e
;;      ;;                ;;(log/debug (.getMessage e))
;;      ;;                nil))
;;      ;;       ]
;;      ;;   ;; (log/debug "emap! got e: " e)
;;      ;;   (if (nil? e)
;;      ;;     (let [e (Entity. k)]
;;      ;;       (.put store-map e)
;;      ;;       (PersistentEntityMap. e nil))
;;      ;;     (PersistentEntityMap. e nil))))))


;; ;; (defn emap??
;; ;;   "Find, possibly.  If entity found, return without change, otherwise
;; ;;   throw exception."
;; ;;   ;; [keylinks & propmap]
;; ;;   [keylinks]
;; ;;    (log/debug "emap??" keylinks)
;; ;;    (if (empty? keylinks)
;; ;;      (throw (IllegalArgumentException. "keychain vector must not be empty"))
;; ;;      (if (every? keylink? keylinks)
;; ;;        (let [k (apply keychainer keylinks)
;; ;;              e ;;(try
;; ;;                  (.get (.content store-map) k)]
;; ;;                     ;; (catch EntityNotFoundException ex (throw ex))
;; ;;                     ;; (catch DatastoreFailureException ex (throw ex))
;; ;;                     ;; (catch java.lang.IllegalArgumentException ex (throw ex)))]
;; ;;          (PersistentEntityMap. e nil))
;; ;;        (throw-bad-keylinks keylinks))))


;; ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; ;; (defmulti ancestors
;; ;;   (fn [& {kind :kind key :key name :name id :id}]
;; ;;     (cond
;; ;;      name :kindname
;; ;;      id   :kindid
;; ;;      kind :kind
;; ;;      key  :key
;; ;;      ;; (= (type s) java.lang.String) :kind
;; ;;      ;; (= (type rest) com.google.appengine.api.datastore.Key) :key
;; ;;      :else :kindless)))

;; ;; (defmethod ancestors :key
;; ;;   [& {kind :kind key :key name :name id :id}]
;; ;;   (Query. key)
;; ;;   )

;; ;; (defmethod ancestors :kindname
;; ;;   [& {kind :kind key :key name :name id :id}]
;; ;;   (let [k (dskey/make {:_kind kind :_name name})]
;; ;;         (Query. k))
;; ;;   )

;; ;; (defmethod ancestors :kindid
;; ;;   [& {kind :kind id :id}]
;; ;;   (let [k (dskey/make {:_kind kind :_id id})]
;; ;;         (Query. k))
;; ;;   )
