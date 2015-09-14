(ns IPersistentEntityMap)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;  IPersistentEntityMap
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(gen-interface
 :name migae.datastore.IPersistentEntityMap
 :extends [clojure.lang.Indexed                  ; extends Counted
           clojure.lang.IFn
           ;; clojure.lang.IMapEntry
           clojure.lang.IObj ;; extends IMeta
           clojure.lang.IPersistentMap
           ;; < Iterable, (Associative < (IPersistentCollection < Seqable, ILookup), Counted
           clojure.lang.IReduce  ; extends IReduceInit
           clojure.lang.IReference ; extends IMeta; required to support metadata reader syntax?
           clojure.lang.ITransientCollection]
 ;; :methods []
 ) ;; end gen-interface IPersistentEntityMap
