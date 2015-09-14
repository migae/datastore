(in-ns 'migae.datastore)

(defn print
  [em]
  (binding [*print-meta* true]
    (prn em)))

(defn print-str
  [em]
  (binding [*print-meta* true]
    (clj/pr-str em)))

(defn println
  [^migae.datastore.IPersistentEntityMap em]
  (binding [*print-meta* true]
    (prn em)))

(defn dump
  [msg datum data]
  (binding [*print-meta* true]
    (log/trace msg (pr datum) (pr data))))
