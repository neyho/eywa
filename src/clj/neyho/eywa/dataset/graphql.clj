(ns neyho.eywa.dataset.graphql
  (:require
   [camel-snake-kebab.core :as csk]
   [clojure.core.async :as async]
   [clojure.string :as str]
   [clojure.tools.logging :as log]
   [neyho.eywa.dataset
    :refer [publisher
            deployed-model]]
   [neyho.eywa.dataset.core :as dataset]
   [neyho.eywa.dataset.sql.naming :refer [normalize-name]]
   [neyho.eywa.db :refer [*db*]]
   [neyho.eywa.iam.access :as access]))

(defn- protect-dataset
  [model]
  (as-> model m
    (reduce
     (fn [m entity]
       (if (access/entity-allows? (:euuid entity) #{:read :write})
         m
         (dataset/remove-entity m entity)))
     m
     (dataset/get-entities m))
    (reduce
     (fn [m {{from :euuid} :from
             {to :euuid} :to
             :as relation}]
       (if (or
            (access/relation-allows?
             (:euuid relation)
             [from to]
             #{:read :write})
            (access/relation-allows?
             (:euuid relation)
             [to from]
             #{:read :write}))
         m
         (dataset/remove-relation m relation)))
     m
     (dataset/get-relations m))))

(defn get-deployed-model [_ _ _]
  (protect-dataset (deployed-model)))

(defn on-deploy
  [{:keys [username]} _ upstream]
  (let [sub (async/chan)]
    (async/sub publisher :refreshedGlobalDataset sub)
    (async/go-loop [{:keys [data]
                     :as published} (async/<! sub)]
      (when published
        (log/tracef "Sending update of global model to user %s" username)
        (upstream data)
        (recur (async/<! sub))))
    (let [model (dataset/get-model *db*)
          protected-model (protect-dataset model)]
      (upstream
       {:name "Global"
        :model protected-model}))
    (fn []
      (async/unsub publisher :refreshedGlobalDataset sub)
      (async/close! sub))))

(defn on-delta
  [{:keys [username]} {:keys [elements]} upstream]
  (let [sub (async/chan)]
    (doseq [element elements]
      (log/debugf "[DELTA SUBSCRIPTION::%s] Subscribing to dataset delta channel for: %s" username element)
      (async/sub dataset/*delta-publisher* element sub))
    ;; Start idle service that will listen on delta changes
    (async/go-loop
     [{:keys [element]
       :as data} (async/<! sub)]
      (log/debugf "[DELTA SUBSCRIPTION::%s] Received something at delta channel" username)
      (when data
        (when (or (access/entity-allows? element #{:read :write})
                  (access/relation-allows? element #{:read :write}))
          (upstream data))
        (recur (async/<! sub))))
    (fn []
      (doseq [element elements]
        (log/debugf "[DELTA SUBSCRIPTION::%s] Removing subscription to dataset delta channel for: %s" username element)
        (async/unsub dataset/*delta-publisher* element sub))
      (async/close! sub))))

(defn- constraint-glyph [constraint]
  (case constraint
    "optional" "o"
    "mandatory" "*"
    "unique" "#"
    "o"))

(defn- attribute-type-str
  [{:keys [type configuration]}]
  (str type
       (when (= type "enum")
         (str \{
              (str/join ","
                        (keep (fn [{:keys [active name]}] (when active name))
                              (:values configuration)))
              \}))))

;; ---------------------------------------------------------------------------
;; describeEntity — Markdown view for a single entity. Designed for LLM agents
;; via the CLI, which combines this with GraphQL `__type` introspection for
;; exact input shapes.
;; ---------------------------------------------------------------------------

(defn- entity->graphql-prefix
  "Canonical PascalCase prefix Lacinia uses for an entity. Same conversion
   as `neyho.eywa.dataset.lacinia/entity->gql-object` so worked examples
   match the actual generated schema."
  [entity-name]
  (csk/->PascalCase (or entity-name "")))

(defn- example-attributes
  "Pick attributes safe to use in worked examples: skip security types and
   complex container types. Returns up to `n` attributes."
  [attributes n]
  (let [skip? #{"hashed" "encrypted" "json" "transit" "avatar"}]
    (->> attributes
         (remove (comp skip? :type))
         (take n))))

(defn- example-search
  [op-prefix attributes]
  (let [picked (example-attributes attributes 3)
        field-list (->> picked
                        (map (comp normalize-name :name))
                        (cons "euuid")
                        (str/join "\n    "))]
    (str "```graphql\nquery {\n  search" op-prefix "(_limit: 10) {\n    "
         field-list
         "\n  }\n}\n```")))

(defn- example-get
  [op-prefix attributes relations]
  (let [picked (example-attributes attributes 2)
        rel (first (filter :to-label relations))
        rel-block (when rel
                    (str "\n    " (normalize-name (:to-label rel))
                         " {\n      euuid\n    }"))]
    (str "```graphql\nquery {\n  get" op-prefix "(euuid: \"...\") {\n    "
         (str/join "\n    " (cons "euuid" (map (comp normalize-name :name) picked)))
         (or rel-block "")
         "\n  }\n}\n```")))

(defn- example-sync
  [op-prefix attributes]
  (let [picked (example-attributes attributes 2)
        fields (->> picked
                    (map (fn [{:keys [name type]}]
                           (str (normalize-name name) ": "
                                (case type
                                  "string" "\"...\""
                                  "boolean" "true"
                                  ("int" "float" "currency") "0"
                                  "timestamp" "\"2026-01-01T00:00:00Z\""
                                  "enum" "VALUE"
                                  "...")))))]
    (str "```graphql\nmutation {\n  sync" op-prefix "(data: {\n    "
         (str/join ",\n    " fields)
         "\n  }) {\n    euuid\n  }\n}\n```")))

(defn- format-entity-md
  "Markdown view of a single entity for `describeEntity`."
  [model {:keys [attributes configuration] entity-name :name entity-euuid :euuid :as entity}]
  (let [relations (dataset/focus-entity-relations model entity)
        op-prefix (entity->graphql-prefix entity-name)
        related-names (->> relations
                           (mapcat (juxt :from :to))
                           (map :name)
                           (remove #{entity-name})
                           distinct)]
    (str/join
     "\n"
     (keep identity
           [(str "## Entity: " entity-name)
            ""
            (str "**EUUID**: `" entity-euuid "`  ")
            (str "**GraphQL prefix**: `" op-prefix "` (used by `search" op-prefix "`, `get" op-prefix "`, `sync" op-prefix "`, ...)")
            (when-let [desc (some-> configuration :description not-empty)]
              (str "\n" desc))
            ""
            "### Attributes"
            ""
            "| Constraint | Name | Field | Type | Comment |"
            "|------------|------|-------|------|---------|"
            (str/join "\n"
                      (map (fn [{:keys [constraint name comment] :as attr}]
                             (format "| `%s` | %s | `%s` | `%s` | %s |"
                                     (constraint-glyph constraint)
                                     (or name "")
                                     (normalize-name (or name ""))
                                     (attribute-type-str attr)
                                     (or comment "")))
                           attributes))
            ""
            (when (seq relations)
              (str/join "\n"
                        (cons "### Relations"
                              (cons ""
                                    (cons "| From | Field on this entity | Cardinality | To | Field on target |"
                                          (cons "|------|---------------------|-------------|-----|-----------------|"
                                                (keep (fn [{:keys [from to from-label to-label cardinality]}]
                                                        (when (or from-label to-label)
                                                          (let [self? (= entity-euuid (:euuid from))
                                                                here (some-> (if self? to-label from-label) normalize-name)
                                                                there (some-> (if self? from-label to-label) normalize-name)]
                                                            (format "| %s | %s | `%s` | %s | %s |"
                                                                    (:name from)
                                                                    (if here (str "`" here "`") "—")
                                                                    cardinality
                                                                    (:name to)
                                                                    (if there (str "`" there "`") "—")))))
                                                      relations)))))))
            (when (seq relations) "")
            "### GraphQL operations"
            ""
            (format "- **Queries**: `search%s`, `get%s`" op-prefix op-prefix)
            (format "- **Mutations**: `sync%s` / `sync%sList`, `stack%s` / `stack%sList`, `slice%s`, `delete%s`, `purge%s`"
                    op-prefix op-prefix op-prefix op-prefix op-prefix op-prefix op-prefix)
            ""
            "### Worked examples"
            ""
            "Search:"
            (example-search op-prefix attributes)
            ""
            "Get one record:"
            (example-get op-prefix attributes relations)
            ""
            "Upsert (create or update):"
            (example-sync op-prefix attributes)
            ""
            (when (seq related-names)
              (str "### See also\n\n"
                   (str/join "\n" (map (fn [n] (str "- `eywa describe entity \"" n "\"`")) related-names))))
            ""
            (str "_For exact input-type field shapes (e.g. `" op-prefix "Input`, `search"
                 op-prefix "Operator`, `orderBy" op-prefix "Operator`),"
                 " run a GraphQL `__type` introspection query — the CLI does this"
                 " automatically. For filter operators and conventions see"
                 " `eywa docs query-patterns`._")]))))

(defn describe-entity
  "Resolver for `describeEntity(name: String!)`. Returns Markdown for a
   single entity. Examples are derived from the model only — never sample
   data — to respect `protect-dataset` permission semantics."
  [_ {:keys [name]} _]
  (let [model (protect-dataset (deployed-model))
        entity (->> (dataset/get-entities model)
                    (filter #(= name (:name %)))
                    first)]
    (if entity
      (format-entity-md model entity)
      (let [available (->> (dataset/get-entities model)
                           (map :name)
                           sort
                           (str/join ", "))]
        (str "Entity `" name "` not found or not visible to you.\n\n"
             "Available entities: " available "\n\n"
             "Run `eywa entities` to see the list.")))))

(defn list-entities
  "Resolver for `listEntities`. Returns the names of entities visible
   to the caller, sorted alphabetically."
  [_ _ _]
  (->> (dataset/get-entities (protect-dataset (deployed-model)))
       (map :name)
       sort
       vec))

(comment
  (def model (deployed-model))
  (def entity (dataset/get-entity model #uuid "63b2e70a-2162-423a-be36-4909d7831605"))
  (println (describe-entity nil {:name "User"} nil))
  (list-entities nil nil nil))
