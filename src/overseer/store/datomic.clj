(ns ^:no-doc overseer.store.datomic
  "Implementation of overseer.core/Store in Datomic"
  (:require [datomic.api :as d]
            [overseer.core :as overseer]
            [clojure.set :as set]
            (loom graph derived)))

(defn transitive-dependents [db job-id]
  "Returns a set of job IDs that transitively depend upon given job ID."
  (let [rules '[[(dependent ?j1 ?j0)
                 [?j1 :job/dep ?j0]]
                [(dependent ?j2 ?j0)
                 (dependent ?j2 ?j1)
                 (dependent ?j1 ?j0)]]]
    (set (d/q '[:find [?dep-jid ...]
                :in $ % ?jid
                :where [?j0 :job/id ?jid]
                       [dependent ?j1 ?j0]
                       [?j1 :job/id ?dep-jid]]
              db
              rules
              job-id))))

(defn jobs-ready'
  "Find all job IDs that are ready to run, i.e. :unstarted
  and not blocked (i.e. dependent on an unfinished job)"
  [db]
  (let [rules '[[(blocked ?j)
                 [?j :job/dep ?dep]
                 [?dep :job/status ?js]
                 [(not= :finished ?js)]]]]
    (->> (d/q '[:find [?jid ...]
                :in $ %
                :where [?j :job/status :unstarted]
                       (not [blocked ?j])
                       [?j :job/id ?jid]]
              db
              rules)
         set)))

(defn jobs-dead'
  "Find all jobs "
  [db thresh]
  (d/q '[:find [?jid ...]
         :in $ ?thresh
         :where [?e :job/status :started]
                [?e :job/id ?jid]
                [?e :job/heartbeat ?h]
                [(< ?h ?thresh)]]
       db
       thresh))

(defn loom-graph->datomic-txn
  "Given a Loom job graph, return a Datomic transaction representing it."
  [graph]
  (let [graph-with-tempids
        (loom.derived/mapped-by #(assoc % :db/id (d/tempid :db.part/user)) graph)

        dep-edge
        (fn [[job0 job1]]
          [:db/add (:db/id job0) :job/dep (:db/id job1)]) ]
    (concat
      (loom.graph/nodes graph-with-tempids)
      (map dep-edge (loom.graph/edges graph-with-tempids)))))

(defn heartbeat []
  (quot (System/currentTimeMillis) 1000))

(defn cas-failed?
  "Return whether an exception is specifically a Datomic check-and-set failure"
  [ex]
  (= :db.error/cas-failed
     (->> ex
          Throwable->map
          :data
          :db/error)))

(defmacro with-ignore-cas [& body]
  `(try
    ~@body
    (catch java.util.concurrent.ExecutionException ex#
      (when-not (cas-failed? ex#)
        (throw ex#)))))

(defn heartbeat-assertion [job-id]
  [:db/add [:job/id job-id] :job/heartbeat (heartbeat)])

(defrecord DatomicStore [conn]
  overseer/Store
  (reserve-job [this job-id]
    (with-ignore-cas
      @(d/transact conn [[:db.fn/cas [:job/id job-id] :job/status :unstarted :started]
                         (heartbeat-assertion job-id)])))

  (finish-job [this job-id]
    @(d/transact conn [[:db.fn/cas [:job/id job-id] :job/status :started :finished]]))

  (fail-job [this job-id failure]
    @(d/transact conn [[:db.fn/cas [:job/id job-id] :job/status :started :failed]
                       [:db/add [:job/id job-id] :job/failure (pr-str failure)]]))

  (heartbeat-job [this job-id]
    @(d/transact conn [(heartbeat-assertion job-id)]))

  (abort-job [this job-id]
    (let [job-ids (cons job-id (transitive-dependents (d/db conn) job-id))
          abort (fn [jid] [:db/add [:job/id jid] :job/status :aborted])]
      @(d/transact conn (map abort job-ids))))

  (reset-job [this job-id]
    (let [old-heartbeat (:job/heartbeat (d/pull (d/db conn) [:job/heartbeat] [:job/id job-id]))]
      (with-ignore-cas
        @(d/transact conn [[:db.fn/cas [:job/id job-id] :job/heartbeat old-heartbeat (heartbeat)]
                           [:db.fn/cas [:job/id job-id] :job/status :started :unstarted]]))))

  (job-info [this job-id]
    (d/pull (d/db conn) [:*] [:job/id job-id]))

  (transact-graph [this graph]
    (->> graph
         loom-graph->datomic-txn
         (d/transact conn)
         deref))

  (jobs-ready [this]
    (jobs-ready' (d/db conn)))

  (jobs-dead [this thresh]
    (jobs-dead' (d/db conn) thresh)))

(defn store [datomic-uri]
  (->DatomicStore (d/connect datomic-uri)))
