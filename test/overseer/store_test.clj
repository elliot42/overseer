(ns overseer.store-test
  "Generic tests for implementations of the Store protocol.

  In your store's test namespace, call these, passing
  them a store implementation.

  (deftest my-test
    (overseer.store-test/test-fn my-store)"
 (:require [clojure.test :refer :all]
           [datomic.api :as d]
           (overseer
             [core :as core]
             [test-utils :as test-utils])
           [overseer.store.datomic :as store]
           [taoensso.timbre :as timbre]
           [clj-time.core :as tcore]
           [framed.std.time :as std.time]
           loom.graph))

(defn simple-graph [& jobs]
  (apply loom.graph/add-nodes (loom.graph/digraph) jobs))

(def ->job test-utils/job)

(defn- unix<-millis-ago [now millis-ago]
  (->> (tcore/minus now (tcore/millis millis-ago))
       std.time/datetime->unix))

(defn test-updating-jobs [store]
  (testing "reservation"
    (let [{job-id :job/id :as job} (test-utils/job {:job/type :foo})]
      (core/transact-graph store (simple-graph job))
      (is (= :unstarted (:job/status (core/job-info store job-id))))
      (core/reserve-job store job-id)
      (is (= :started (:job/status (core/job-info store job-id))))
      (is (nil? (core/reserve-job store job-id))
          "It returns nil if it can't reserve an already started job")))

  (testing "finishing"
    (let [{job-id :job/id :as job} (test-utils/job {:job/type :foo})]
      (core/transact-graph store (simple-graph job))
      (is (= :unstarted (:job/status (core/job-info store job-id))))
      (core/reserve-job store job-id)
      (is (= :started (:job/status (core/job-info store job-id))))
      (core/finish-job store job-id)
      (is (= :finished (:job/status (core/job-info store job-id))))))

  (testing "failing"
    (let [{job-id :job/id :as job} (test-utils/job {:job/type :foo})
          failure {:thing :went-wrong}]
      (core/transact-graph store (simple-graph job))
      (is (= :unstarted (:job/status (core/job-info store job-id))))
      (core/reserve-job store job-id)
      (is (= :started (:job/status (core/job-info store job-id))))
      (core/fail-job store job-id failure)
      (is (= :failed (:job/status (core/job-info store job-id))))
      (is (= (pr-str failure) (:job/failure (core/job-info store job-id)))))))


(defn test-jobs-ready [store]
  (let [j0 (->job)
        j1 (->job)
        graph (loom.graph/digraph
                {j0 []
                 j1 [j0]})
        _ (core/transact-graph store graph)
        ready (core/jobs-ready store)]
    (is (contains? ready (:job/id j0)))
    (is (not (contains? ready (:job/id j1))))))

(defn test-jobs-dead [store]
  (timbre/with-log-level :report
    (let [now (tcore/now)

          j1 (->job {:job/status :started
                     :job/heartbeat (unix<-millis-ago now 1000)})
          j2 (->job {:job/status :started
                     :job/heartbeat (unix<-millis-ago now 50000)}) ; Dead
          j3 (->job {:job/status :started
                     :job/heartbeat (unix<-millis-ago now 500)})
          thresh (unix<-millis-ago now 3000)]
      (core/transact-graph store (simple-graph j1 j2 j3))
      (is (= [(:job/id j2)] (core/jobs-dead store thresh))))))

(defn test-protocol [store]
  (test-updating-jobs store)
  (test-jobs-ready store)
  (test-jobs-dead store))
