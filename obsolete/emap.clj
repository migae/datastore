

(defprotocol GAEDS
  (get-entity-with-fields [keymap])
  ;; (meta? [theEntity])
  (ds [this])
  (Keys [keymap])
  (Entities [e]))
;;  (merge [theEntity entitymap])) ;; update Entity map

(extend-protocol GAEDS
  ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
  com.google.appengine.api.datastore.Key
  (ds [theKey]
    (do ;; (prn "ds applied to Key" theKey)
        (let [theEntity (.get (datastore) theKey)]
          ;; (prn "ds applied to map")
          ;; (prn (str "made key: " theKey))
          ;; (prn (str "fetched entity: " theEntity))
          (wrap-entity theEntity))))
  (Entities [theKey]
    ;; if entity already exists return it as ds/Entity else create it
    (let [theEntity
          (try (.get (datastore) theKey)
               (catch EntityNotFoundException e1 ) ;; (prn "NOT FOUND"))
               (catch IllegalArgumentException e2 (prn "ILLEGAL ARG TO GET"))
               (catch DatastoreFailureException e3
                 (prn "DatastoreFailureException")))]
      (if theEntity
        (do (prn "FOUND")
            (wrap-entity theEntity))
        (do (prn "NOT FOUND")
            ;; TODO: make new only if body non-empty
            ;; otherwise return NOTFOUND
            ;; (but what if user wants to create empty entity?)
            ;; answer: use a metadatum to indicate what to do
            (let [theEntity (Entity. theKey)]
              (do (.put (datastore) theEntity)
                  (wrap-entity theEntity)))))))


  ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
  com.google.appengine.api.datastore.Entity
  (ds [theEntity] (throw (Exception. "ds applied to Entity")))
  (Entities [theEntity] (throw (Exception. "Entities applied to Entity")))

  ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
  clojure.lang.PersistentArrayMap       ; e.g.  {:a 100 :b 200}
    (get-entity-with-fields
    [{:keys [kind name id] :as keymap}]
;;    [{:keys [kind id theKey] :as keymap}]
    ;; TODO: validate keymap
    ;; TODO: handle both string and nbr ids
    (do ;;(prn "get-entity-with-fields applied to keymap" keymap)
        (let [theKey (KeyFactory/createKey
                      (clojure.core/name kind)
                      (if id id name))
              theEntity (.get (datastore) theKey)
              props (.getProperties theEntity)]
          ;; props = java.util.Collections$UnmodifiableMap
          ;; prop = java.util.Collections
          ;;		$UnmodifiableMap$UnmodifiableEntrySet$UnmodifiableEntry
          (clj/into {} (map (fn [item]
                          {(keyword (.getKey item))
                           (.getValue item)}) props)))))
    (Keys [{:keys [kind name id] :as keymap}]
       (do (prn "Keys applied to keymap" keymap)
           (let [theKey  (KeyFactory/createKey (clojure.core/name kind)
                                      (if id id
                                          name))]
             theKey)))
    (ds [{:keys [kind name id] :as keymap}]
      ;; TODO: handle bad keymap
      ;; use :pre ?
      (do ;;(prn "ds applied to keymap")
        (let [theKey (KeyFactory/createKey
                      (clojure.core/name kind)
                      (if id id name))
              theEntity (.get (datastore) theKey)]
          (wrap-entity theEntity))))
    (Entities
      [theEntityMap]
      (let [{:keys [kind name id]} (meta theEntityMap)]
        (do ;; (prn "ds/Entities applied to EntityMap" theEntityMap)
            (if (nil? kind)
              (throw (Exception. "EntityMap must be metadata containing :kind")))
            (if id (if (not (number? id))
                     (throw
                      (Exception. ":id must be numeric"))))
            (entity-from-entitymap theEntityMap))))
        ;; (doseq [[k v] theAugment]
        ;;   (.setProperty (:theEntity theEntity)
        ;;                 ;; todo: deal with val types
        ;;                 (name k) (if (number? v)
        ;;                            v
        ;;                            (name v))))))

  ;; clojure.lang.PersistentHashMap        ; e.g.  #{:a 100 :b 200}

  clojure.lang.IFn
  (ds [theEntity] (throw (Exception. "ds applied to fn")))
  (Entities [theEntity]
    ;; TODO: validate theEntity
    {:pre [;; (do (prn "e map meta: " (meta theEntity))
           ;;     (prn "e map id: "   (:id (meta theEntity))) true),
           (if (nil? ( :kind (meta theEntity)))
             (and (not (nil? ((meta theEntity) :key)))
                  (and (nil? ((meta theEntity) :id))
                       (nil? ((meta theEntity) :name))))
             (and (nil? (:key (meta theEntity)))
                  (or (nil? (:id (meta theEntity)))
                      (nil? (:name (meta theEntity)))))),
           (if (not (nil? (:id (meta theEntity))))
             (number? (:id (meta theEntity)))
             true),
           (if (not (nil? (:name (meta theEntity))))
             (or (string?  (:name (meta theEntity)))
                 (keyword? (:name (meta theEntity))))
             true)
           ;; TODO: validate :parent
           ]}
    (do (prn "Entities applied to fn" (meta theEntity))
        (let [{:keys [key kind id name parent]} (meta theEntity)
;;              arg1 (if (nil? kind) key
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
              theEntity (if (nil? kind) (Entity. key)
                            (if (nil? parent)
                              (Entity. (clojure.core/name kind)
                                       (if id id (if name name)))
                              (Entity. (clojure.core/name kind)
                                       (if id id (if name name))
                                       ;; stipulate: arg3 = :Key
                                       arg3)))]
          (doseq [[k v] theEntity]
            ;; TODO: handle val types
            (.setProperty theEntity
                          (clojure.core/name k)
                          (clojure.core/name v)))
          ;; TODO: wrap-entity s/b resonsible for putting if needed
          (.put (datastore) theEntity)
          (wrap-entity theEntity))))
          ;; {:theKey (.put (datastore) theEntity)
          ;;  :theEntity theEntity})))
  ) ;; extend-protocol


