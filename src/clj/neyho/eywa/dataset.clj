(ns neyho.eywa.dataset
  (:require
    [cheshire.generate :as cgen]
    [clojure.core.async :as async]
    [clojure.java.io :as io]
    clojure.pprint
    clojure.set
    clojure.string
    [clojure.tools.logging :as log]
    [com.walmartlabs.lacinia.executor :as executor]
    [com.walmartlabs.lacinia.resolve :as resolve]
    [com.walmartlabs.lacinia.selection :as selection]
    neyho.eywa
   ; [neyho.eywa.transit :refer [<-transit]]
    [neyho.eywa.data]
    [neyho.eywa.dataset.core :as dataset]
    [neyho.eywa.dataset.uuids :as du]
    [neyho.eywa.db :as db :refer [*db*]]
    [neyho.eywa.iam.uuids :as au]
    [neyho.eywa.lacinia :as lacinia])
  (:import [org.postgresql.util PGobject]))

(defonce ^:dynamic *model* (ref nil))

(defn deployed-model [] @*model*)

(defn deployed-entity [id]
  (dataset/get-entity (deployed-model) id))

(defn deployed-relation [id]
  (dataset/get-relation (deployed-model) id))

(defn save-model [new-model]
  (dosync (ref-set *model* new-model)))

(defn list-entity-ids []
  (sort
    (map
      (juxt :euuid :name)
      (dataset/get-entities (deployed-model)))))

(defonce subscription (async/chan 100))

(defonce publisher (async/pub subscription :topic))

(def datasets #uuid "a800516e-9cfa-4414-9874-60f2285ec330")

; (defn wrap-resolver-request
;   "Function should take handle apply "
;   [handler resolver]
;   (fn [ctx args value]
;     (let [[ctx args value] (handler ctx args value)]
;       (resolver ctx args value))))

; (defn wrap-context
;   [handler resolver]
;   (fn [ctx args value]
;     (let [[ctx args value] (handler ctx args value)]
;       [ctx args (resolver ctx args value)])))

(defn wrap-hooks
  "Function will wrap all hooks that were defined for some
  field definition and sort those hooks by :metric attribute.
  
  Every time field is required by selection hook chain will be
  called.
  
  Hooks accept args [ctx args value] and should return 'modified'
  [ctx args value]"
  [hooks resolver]
  (if (not-empty hooks)
    (let [hooks (keep
                  (fn [definition]
                    (let [{resolver :fn
                           :as hook} (selection/arguments definition)]
                      (if-some [resolved (try
                                           (resolve (symbol resolver))
                                           (catch Throwable e
                                             (log/errorf e "Couldn't resolve symbol %s" resolver)
                                             nil))]
                        (assoc hook :fn resolved)
                        (assoc hook :fn (fn [ctx args v]
                                          (log/error "Couldn't resolve '%s'" resolver)
                                          [ctx args v])))))
                  hooks)
          ;;
          {:keys [pre post]}
          (group-by
            #(cond
               (neg? (:metric % 1)) :pre
               (pos? (:metric % 1)) :post
               :else :resolver)
            hooks)
          ;;
          steps
          (cond-> (or (some-> (not-empty (map :fn pre)) vec) [])
            ;;
            (some? resolver)
            (conj (fn wrapped-resolver [ctx args value]
                    (let [new-value (resolver ctx args value)]
                      [ctx args new-value])))
            ;;
            (not-empty post)
            (into (map :fn post)))]
      ;; FINAL RESOLVER
      (fn wrapped-hooks-resolver [ctx args value]
        ; (log/infof "RESOLVING: %s" resolver)
        (let [[_ _ v]
              (reduce
                (fn [[ctx args value] f]
                  (f ctx args value))
                [ctx args value]
                steps)]
          ; (log/infof "RETURNING FINAL RESULT: %s" v)
          v)))
    resolver))

;; WRAPPERS
(defn sync-entity [entity-id data] (db/sync-entity *db* entity-id data))
(defn stack-entity [entity-id data] (db/stack-entity *db* entity-id data))
(defn slice-entity [entity-id args selection] (db/slice-entity *db* entity-id args selection))
(defn get-entity [entity-id args selection] (db/get-entity *db* entity-id args selection))
(defn get-entity-tree [entity-id root on selection] (db/get-entity-tree *db* entity-id root on selection))
(defn search-entity [entity-id args selection] (db/search-entity *db* entity-id args selection))
(defn search-entity-tree [entity-id on args selection] (db/search-entity-tree *db* entity-id on args selection))
(defn purge-entity [entity-id args selection] (db/purge-entity *db* entity-id args selection))
(defn aggregate-entity [entity-id args selection] (db/aggregate-entity *db* entity-id args selection))
(defn delete-entity [entity-id data] (db/delete-entity *db* entity-id data))

(defn deploy! [model] (dataset/deploy! *db* model))
(defn reload [] (dataset/reload *db*))

(defn bind-service-user
  [variable]
  (let [args (select-keys (var-get variable) [:euuid])
        data (get-entity
               au/user args
               {:_eid nil
                :euuid nil
                :name nil
                :avatar nil
                :active nil
                :type nil})]
    (log/debugf "Initializing %s\n%s" variable data)
    (alter-var-root variable (constantly data))))

(comment
  (binding [neyho.eywa.iam.access.context/*roles*
            #{#uuid "28895548-9fe6-4a5d-93d7-14468c2b2b51"}
            neyho.eywa.iam.access.context/*user* #uuid "0a6e2c0e-fed8-45e4-9ec7-9beca0c29531"]
    (let [m (protect-dataset (dataset/get-model *db*))]
      {:entities
       (map :name (dataset/get-entities m))
       :relations
       (map (juxt :from-label :to-label) (dataset/get-relations m))})))

(defn get-version
  [euuid]
  (get-entity
    du/dataset-version
    {:euuid euuid}
    {:euuid nil
     :dataset [{:selections
                {:euuid nil
                 :name nil}}]
     :model nil
     :name nil}))

(defn get-version-model
  [euuid]
  (:model (get-version euuid)))

(comment
  (def entity-uuid #uuid "5338693b-9dbc-4434-b598-b15175da04c3")
  (def entity (dataset/get-entity (deployed-model) entity-uuid))
  (def relations (dataset/get-entity-relations (deployed-model) entity))
  (doseq [{relation :euuid} relations]
    (delete-entity du/dataset-relation {:euuid relation}))
  (delete-entity du/dataset-entity {:euuid entity-uuid})
  (def version
    (get-entity
      du/dataset-version
      {:euuid #uuid "1b14b5c9-44ab-4280-8f8a-37c2d419068a"}
      {:euuid nil
       :dataset [{:selections
                  {:euuid nil
                   :name nil}}]
       :model nil
       :name nil}))
  (def version)
  (def username "rgersak")
  (def user nil))

;; @resolve
(defn deploy-dataset
  ([model] (deploy-dataset nil model nil))
  ([context model] (deploy-dataset context model nil))
  ([{:keys [username]
     :as context}
    {version :version}
    _]
   (let [selection (executor/selections-tree context)]
     (log/infof
       "User %s deploying dataset %s@%s"
       username (-> version :dataset :name) (:name version))
     (try
       ; (let [{:keys [euuid]} (dataset/deploy! connector version)]
       ;; TODO - Rethink this. Should we use teard-down and setup instead
       ;; of plain deploy! recall!
       (let [{:keys [euuid]} (dataset/deploy! *db* version)]
         (async/put!
           subscription
           {:topic :refreshedGlobalDataset
            :data {:name "Global"
                   :model (dataset/get-model *db*)}})
         (log/infof
           "User %s deployed version %s@%s"
           username (-> version :dataset :name) (:name version))
         (when (not-empty selection)
           ; (log/tracef
           ;   "Returning data for selection:\n%s"
           ;   (with-out-str (clojure.pprint/pprint selection)))
           (db/get-entity *db* du/dataset-version {:euuid euuid} selection)))
       (catch clojure.lang.ExceptionInfo e
         (log/errorf
           e "Couldn't deploy dataset version %s@%s"
           (-> version :dataset :name) (:name version))
         (resolve/with-error
           nil
           (assoc (ex-data e) :message (ex-message e))))
       (catch Throwable e
         (log/errorf
           e "Couldn't deploy dataset version %s@%s"
           (-> version :dataset :name) (:name version))
         ;; TODO - Decide if upon unsuccessfull deploy do we delete old table (commented)
         ; (let [{versions :versions} 
         ;       (graphql/get-entity
         ;         connector
         ;         datasets-uuid
         ;         {:euuid euuid}
         ;         {:name nil
         ;          :versions [{:args {:_order_by [{:modified_on :desc}]}
         ;                      :selections {:name nil
         ;                                   :euuid nil
         ;                                   :model nil}}]})]
         ;   (when (empty? versions)
         ;     (dataset/tear-down-module connector version)))
         ; (dataset/unmount connector version)
         #_(server/restart account)
         (resolve/with-error
           nil
           {:message (.getMessage e)
            :type ::dataset/error-unknown}))))))

;; @resolve
(defn import-dataset
  ([context {dataset :dataset} _]
   (deploy-dataset context {:version dataset} nil)))

;; @hook
(defn prepare-deletion-context
  [ctx {:keys [euuid]
        :as args} v]
  [(if (some? euuid)
     (if-let [dataset (db/get-entity
                        *db*
                        datasets
                        {:euuid euuid}
                        {:name nil
                         :euuid nil
                         :versions [{:args {:_order_by [{:modified_on :asc}]
                                            :_where {:deployed {:_boolean :TRUE}}}
                                     :selections {:name nil
                                                  :euuid nil
                                                  :model nil}}]})]
       (do
         (log/infof "Preparing deletion context for dataset: %s" (:name dataset))
         (cond-> ctx
           (and (some? euuid) dataset)
           (assoc ::destroy dataset)))
       ctx)
     ctx)
   args
   v])

;; @hook
(defn destroy-linked-versions
  [{username :eywa/username
    destroy ::destroy
    :as ctx} args v]
  (when destroy
    (log/infof "User %s destroying dataset %s" username (:name destroy))
    (dataset/destroy! *db* destroy)
    (comment
      (dataset/get-relations (-> destroy :versions first :model)))
    (async/put! subscription
                {:topic :refreshedGlobalDataset
                 :data {:name "Global"
                        :model (dataset/get-model *db*)}}))
  [ctx args v])

(defn is-supported?
  [db]
  (when-not (some #(instance? % db) [neyho.eywa.Postgres])
    (throw
      (ex-info
        "Database is not supported"
        (if (map? db) db {:db db})))))

(defn setup
  "Function will setup initial dataset models that are required for EYWA
  datasets to work. That includes aaa.edm and dataset.edm models"
  ([db]
   (is-supported? db)
   db))

(comment
  (def db *db*)
  (def global-model (dataset/get-last-deployed db))
  (deployed-model)
  (def euuid #uuid "ae3e0f7f-dd0a-468c-9885-caac4141a5c3")
  (dataset/get-relation (deployed-model) #uuid "ae3e0f7f-dd0a-468c-9885-caac4141a5c3")
  (lacinia/generate-lacinia-schema db)
  (def dataset-euuid #uuid "5bd79e74-8ada-4b51-9692-f402d3008e93")
  (do
    (def file "./first_ai_test.edm")
    (def model (neyho.eywa.transit/<-trasit (slurp file)))
    (def model (:model (neyho.eywa.transit/<-transit (slurp "/Users/robi/Downloads/Authentication_Authorization_&_Access_0_75.edm"))))
    (type global-model) (type model)
    (def projection (dataset/project model global-model))
    (filter
      dataset/entity-changed?
      (dataset/get-entities projection)))
  (:version
    (:model
      (get-entity
        du/dataset-version
        {:euuid #uuid "55f666c1-f631-4a69-a84a-5746ed04ba4e"}
        {:model nil}))))

(defn latest-deployed-versions
  []
  (map
    (fn [{[{version :name
            :keys [model]}] :versions
          :keys [euuid name]}]
      {:euuid euuid
       :name name
       :version version
       :model model})
    (search-entity
      du/dataset
      nil
      {:euuid nil
       :name nil
       :versions
       [{:selections {:name nil
                      :model nil}
         :args {:_where {:deployed {:_boolean :TRUE}}
                :_limit 1
                :_order_by {:modified_on :desc}}}]})))

(defn latest-deployed-version
  [dataset-euuid]
  (let [{[version] :versions}
        (get-entity
          du/dataset
          {:euuid dataset-euuid}
          {:versions
           [{:selections {:euuid nil
                          :name nil
                          :model nil}
             :args {:_where {:deployed {:_boolean :TRUE}}
                    :_limit 1
                    :_order_by {:modified_on :desc}}}]})]
    version))

(defn latest-deployed-model
  [dataset-euuid]
  (:model (latest-deployed-version dataset-euuid)))

(defn init-delta-pipe
  []
  (let [delta-client (async/chan 1000)
        delta-publisher (async/pub
                          delta-client
                          (fn [{:keys [element]}]
                            element))]
    (alter-var-root #'neyho.eywa.dataset.core/*delta-client* (fn [_] delta-client))
    (alter-var-root #'neyho.eywa.dataset.core/*delta-publisher* (fn [_] delta-publisher))))

(defn start
  "Function initializes EYWA datasets by loading last deployed model."
  ([] (start *db*))
  ([db]
   (log/info "Initializing Datasets...")
   (comment
     (def db *db*))
   (try
     (init-delta-pipe)
     (cgen/add-encoder
       PGobject
       (fn [^PGobject pgobj json-gen]
         ;; .getValue returns the textual representation (for json/jsonb it's a JSON string)
         (.writeString json-gen (.getValue pgobj))))
     (dataset/reload db {:model (dataset/get-last-deployed db 0)})
     (lacinia/add-directive :hook wrap-hooks)
     (lacinia/add-shard ::dataset-directives (slurp (io/resource "dataset_directives.graphql")))
     (lacinia/add-shard ::datasets (slurp (io/resource "datasets.graphql")))
     (catch Throwable e (log/errorf e "Couldn't initialize Datasets...")))
   ; (alter-var-root #'*datasets* (fn [_] db))
   (dataset/reload db)
   (binding [dataset/*return-type* :edn]
     (bind-service-user #'neyho.eywa.data/*EYWA*))
   nil))

(defn stop
  []
  (dosync
    (ref-set *model* nil)))
