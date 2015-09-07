(in-ns 'migae.datastore)

;; FIXME: rename:  co-ctors-key?

;;(load "emap")

(declare keychain? keychain= dogtag)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defmulti kind class)
(defmethod kind Key
  [^Key k]
  (keyword (.getKind k)))
(defmethod kind Entity
  [^Entity e]
  (keyword (.getKind e)))
;; (defmethod kind migae.datastore.IPersistentEntityMap
;;   [^migae.datastore.PersistentEntityMap e]
;;   (log/trace "IPersistentEntityMap.kind")
;;   (keyword (.getKind (.content e))))
(defmethod kind migae.datastore.PersistentEntityMap
  [^migae.datastore.PersistentEntityMap e]
  ;; (log/trace "PersistentEntityMap.kind")
  (keyword (.getKind (.content e))))
(defmethod kind migae.datastore.PersistentEntityHashMap
  [^migae.datastore.PersistentEntityHashMap e]
  (keyword (clj/namespace (last (.k e)))))
(defmethod kind clojure.lang.Keyword
  [^clojure.lang.Keyword kw]
  (when-let [k (namespace kw)]
    (keyword k)))
    ;; (clojure.core/name kw)))
(defmethod kind clojure.lang.PersistentVector
  [^clojure.lang.PersistentVector k]
  ;; FIXME: validate keychain contains only keylinks
  (if (keychain? k)
    (keyword (namespace (last k)))))

(defmulti identifier class)
(defmethod identifier Key
  [^Key k]
  ;; (log/trace "Key identifier" k)
  (let [nm (.getName k)
        id (.getId k)]
    (if (nil? nm) id (str nm))))
(defmethod identifier migae.datastore.PersistentEntityMap
  [^migae.datastore.PersistentEntityMap em]
  ;; (log/trace "PersistentEntityMap.identifier")
  (let [k (.getKey (.content em))
        nm (.getName k)
        id (.getId k)]
    (if (nil? nm) id (str nm))))
(defmethod identifier migae.datastore.PersistentEntityHashMap
  [^migae.datastore.PersistentEntityHashMap em]
  ;; (log/trace "PersistentEntityHashMap.identifier")
  (let [fob (dogtag (.k em))
        nm (read-string (clj/name fob))]
    nm))
(defmethod identifier clojure.lang.PersistentVector
  [^clojure.lang.PersistentVector keychain]
  ;; FIXME: validate vector contains only keylinks
  (let [k (last keychain)]
    (if-let [nm (.getName k)]
      nm
      (.getId k))))

(defmulti name class)
(defmethod name Entity
  [^Entity e]
  (.getName (.getKey e)))
(defmethod name clojure.lang.PersistentVector
  [^ clojure.lang.PersistentVector keychain]
  (clj/name (last keychain)))
(defmethod name migae.datastore.PersistentEntityMap
  [^migae.datastore.PersistentEntityMap em]
  (.getName (.getKey (.content em))))

(defmulti id class)
(defmethod id clojure.lang.PersistentVector
  [ks]
   (let [keylink (last ks)]
     (.getId keylink)))
(defmethod id Key
  [^Key k]
  (.getId k))
(defmethod id Entity
  [^Entity e]
  (.getId (.getKey e)))
(defmethod id migae.datastore.PersistentEntityMap
  [^migae.datastore.PersistentEntityMap e]
  (.getId (.getKey (.content e))))

(defn ekey? [^com.google.appengine.api.datastore.Key k]
  (= (type k) com.google.appengine.api.datastore.Key))

(defn keylink?
  [k]
  ;; (log/trace "keylink?" k (and (keyword k)
  ;;                              (not (nil? (clj/namespace k)))))
  (and (keyword k)
       (not (nil? (clj/namespace k)))))


(defmulti to-keychain class)
(defmethod to-keychain Key
  [k]
  (ekey/to-keychain k))
(defmethod to-keychain migae.datastore.PersistentEntityMap
  [em]
  (ekey/to-keychain (.getKey (.content em))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defmulti keychain class)
(defmethod keychain Key
  [k]
  (let [keychain (ekey/to-keychain k)]
    keychain))
(defmethod keychain migae.datastore.PersistentEntityMap
  [em]
  (let [keychain (ekey/to-keychain (.getKey (.content em)))]
    keychain))
(defmethod keychain clojure.lang.PersistentVector
  [keychain]
  keychain)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defmulti dogtag class)
(defmethod dogtag Key
  [k]
  (let [keychain (ekey/to-keychain k)]
    (last keychain)))
(defmethod dogtag migae.datastore.PersistentEntityMap
  [em]
  (let [keychain (ekey/to-keychain (.getKey (.content em)))]
    (last keychain)))
(defmethod dogtag clojure.lang.PersistentVector
  [keychain]
  ;; FIXME: validate vector contains only keylinks
  (if (every? keylink? keychain)
    (last keychain)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defmulti entity-key class)
(defmethod entity-key Key
  [^Key k]
  k)
(defmethod entity-key migae.datastore.PersistentEntityMap
  [^migae.datastore.PersistentEntityMap e]
  (.getKey (.content e)))
(defmethod entity-key com.google.appengine.api.datastore.Entity
  [^Entity e]
  (.getKey e))
(defmethod entity-key clojure.lang.Keyword
  [^clojure.lang.Keyword k]
  (ekey/keychain-to-key [k]))
(defmethod entity-key clojure.lang.PersistentVector
  [kchain]
  ;; FIXME: validate vector contains only keylinks
  (ekey/keychain-to-key kchain))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn key=
  [em1 em2]
  (if (emap? em1)
    (if (emap? em2)
      (.equals (.content em1) (.content em2))
      (keychain= em1 em2))
    (if (map? em1)
      (keychain= em1 em2)
      (log/trace "EXCEPTION: key= applies only to maps and emaps"))))

(defn keykind?
  [k]
  ;; (log/trace "keykind?" k (and (keyword k)
  ;;                              (not (nil? (clj/namespace k)))))
  (and (keyword? k) (nil? (clj/namespace k))))

(defn keychain? [k]
  ;; k is vector of DS Keys and clojure keywords
  (every? #(or (keyword? %) (= (type %) Key)) k))

(defn keychain=
  [k1 k2]
  (let [kch1 (if (emap? k1)
               ;; recur with .getParent
               (if (map? k1)
                 (:migae/key (meta k1))))
        kch2 (if (emap? k2)
               ;; recur with .getParent
               (if (map? k2)
                 (:migae/key (meta k2))))]
    ))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; (let [kind (edn/read-string fst)
;;       name (edn/read-string )]
;;   (KeyFactory/createKey (str kind) ident))))

;; ([^String k ^Long ident]
;;  (let [kind (edn/read-string k)
;;        name (edn/read-string ident)]
;;    (KeyFactory/createKey (str kind) ident))))

;; (if (= (type name) java.lang.Long)
;;       (KeyFactory/createKey ns n)))
;;   (KeyFactory/createKey ns n)))))

;; (log/trace "ns " ns " n: " n ", first n: " (first n))

