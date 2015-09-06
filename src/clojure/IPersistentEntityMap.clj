(ns IPersistentEntityMap
  (:import
   [clojure.lang
    Indexed
    IFn
    IMapEntry
    IObj
    IPersistentMap
    IReduce
    IReference
    ITransientCollection]
   )
  ;; (:require [clojure.core :as clj]
  ;;           [clojure.walk :as walk]
  ;;           [clojure.stacktrace :refer [print-stack-trace]]
  ;;           [clojure.tools.reader.edn :as edn]
  ;;           ;; [migae.datastore :refer :all]
  ;;           [migae.datastore.service :as ds]
  ;;           [migae.datastore.keychain :as ekey]
  ;;           ;; [migae.datastore.dsmap :as dsm]
  ;;           ;; [migae.datastore.emap :as emap]
  ;;           ;; [migae.datastore.entity :as dse]
  ;;           ;; [migae.datastore.key :as dskey]
  ;;           ;; [migae.datastore.query :as dsqry]
  ;;           [migae.infix :as infix]
  ;;           [clojure.tools.logging :as log :only [trace debug info]]))
  )
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;  IPersistentEntityMap
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(gen-interface
 :name migae.datastore.IPersistentEntityMap
 :extends [;; java.util.Map$Entry
           clojure.lang.Indexed                  ; extends Counted
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
