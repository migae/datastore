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
  (keyword (namespace (last (.k e)))))
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
        nm (read-string (name fob))]
    nm))
(defmethod identifier clojure.lang.PersistentVector
  [^clojure.lang.PersistentVector keychain]
  ;; FIXME: validate vector contains only keylinks
  (let [k (last keychain)]
    (if-let [nm (.getName k)]
      nm
      (.getId k))))

(defn ekey? [^com.google.appengine.api.datastore.Key k]
  (= (type k) com.google.appengine.api.datastore.Key))

(defn keylink?
  [k]
  ;; (log/trace "keylink?" k (and (keyword k)
  ;;                              (not (nil? (namespace k)))))
  (or (and (keyword k)
           (not (nil? (namespace k))))
      (= (type k) com.google.appengine.api.datastore.Key)))



;; (defmulti to-keychain class)
;; (defmethod to-keychain Key
;;   [k]
;;   (keychain k))
;; (defmethod to-keychain migae.datastore.PersistentEntityMap
;;   [em]
;;   (keychain (.getKey (.content em))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; (defmulti keychain class)
;; (defmethod keychain Key
;;   [k]
;;   (let [keychain (keychain k)]
;;     keychain))
;; (defmethod keychain migae.datastore.PersistentEntityMap
;;   [em]
;;   (let [keychain (keychain (.getKey (.content em)))]
;;     keychain))
;; (defmethod keychain clojure.lang.PersistentVector
;;   [keychain]
;;   keychain)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defmulti dogtag class)
(defmethod dogtag Key
  [k]
  (let [keychain (keychain k)]
    (last keychain)))
(defmethod dogtag migae.datastore.PersistentEntityMap
  [em]
  (let [keychain (keychain (.getKey (.content em)))]
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
  (keychain-to-key [k]))
(defmethod entity-key clojure.lang.PersistentVector
  [kchain]
  ;; FIXME: validate vector contains only keylinks
  (keychain-to-key kchain))

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
  ;;                              (not (nil? (namespace k)))))
  (and (keyword? k) (nil? (namespace k))))

;; NB: this dups what's in the keychain namespace
;; to make it available as ds/keychain?
(defn keychain? [k]
  (keychain? k))

;; (defn keychain
;;   [arg]
;;   (log/debug "KEYCHAIN: " arg)
;;   (keychain arg))

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

