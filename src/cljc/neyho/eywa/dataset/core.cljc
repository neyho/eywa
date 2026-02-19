(ns neyho.eywa.dataset.core
  (:require
   ;; DEPRECATED - only for version 1
    #?(:cljs
       [helix.core :refer [create-context]])
    clojure.data
    clojure.set))

;; DEPRECATED - only for version 1
#?(:cljs (defonce ^:dynamic *dataset* (create-context)))

(defn deep-merge
  "Recursively merges maps."
  [& maps]
  (letfn [(m [& xs]
            (if (some #(and (map? %) (not (record? %))) xs)
              (apply merge-with m xs)
              (last xs)))]
    (reduce m maps)))

(defn- not-initialized [msg]
  (throw (ex-info "Delta client not initialized" {:message msg})))

(defonce ^:dynamic *return-type* :graphql)
(defonce ^:dynamic *delta-client* not-initialized)
(defonce ^:dynamic *delta-publisher* not-initialized)

(defn generate-uuid []
  #?(:clj (java.util.UUID/randomUUID)
     :cljs (random-uuid)))

;;; Type Conversion Validation System (Shared Frontend/Backend)

(def type-families
  "Groups of types that share the same underlying database storage type.
   Used to determine safe type conversions."
  {:text #{"string" "avatar" "transit" "hashed"}
   :json #{"json" "encrypted" "currency" "timeperiod"}
   :numeric #{"int" "float"}
   :reference #{"user" "group" "role"}
   :temporal #{"timestamp"}
   :boolean #{"boolean"}
   :enum #{"enum"}})

(defn get-type-family
  "Returns the family keyword for a given type, or nil if not in a family"
  [type]
  (some (fn [[family types]]
          (when (contains? types type)
            family))
        type-families))

(defn same-family?
  "Check if two types are in the same family (safe conversion)"
  [from-type to-type]
  (when-let [from-family (get-type-family from-type)]
    (= from-family (get-type-family to-type))))

(defn validate-type-conversion
  "Validates a type conversion and returns:
   - {:safe true} if conversion is always safe
   - {:warning \"message\"} if conversion might lose data
   - {:error \"message\" :type ::error-type :suggestion \"hint\"} if conversion is forbidden

   This function is shared between frontend and backend to ensure consistent validation."
  [from-type to-type]
  (cond
    ;; Same type - no conversion needed
    (= from-type to-type)
    {:safe true}

    ;; SPECIFIC WARNINGS - These must come BEFORE general family checks

    ;; float → int (precision loss warning)
    (and (= from-type "float") (= to-type "int"))
    {:warning "Converting float to int will truncate decimal values. Precision loss may occur."}

    ;; Within reference family (risky - EIDs might not exist)
    (and (contains? (:reference type-families) from-type)
         (contains? (:reference type-families) to-type))
    {:warning (str "Converting " from-type " to " to-type " assumes all entity IDs exist in the target table. Invalid references will violate foreign key constraints.")}

    ;; timestamp → string (losing temporal semantics)
    (and (= from-type "timestamp") (= to-type "string"))
    {:warning "Converting timestamp to string will lose temporal semantics and indexing capabilities. Consider carefully if this is necessary."}

    ;; encrypted → non-json (data is encrypted)
    (and (= from-type "encrypted")
         (not (contains? (:json type-families) to-type))
         (not= to-type "string"))
    {:error "Cannot convert encrypted data to non-JSON/string type: Data is encrypted and cannot be directly converted."
     :type ::forbidden-conversion
     :suggestion "Decrypt data first or keep as encrypted/json type."}

    ;; GENERAL SAFE CONVERSIONS

    ;; Any type can be converted to string (after specific checks above)
    (= to-type "string")
    {:safe true}

    ;; Within same family - safe (same DB type) - after specific warnings
    (same-family? from-type to-type)
    {:safe true}

    ;; int → float (widening)
    (and (= from-type "int") (= to-type "float"))
    {:safe true}

    ;; enum → string (enum values are strings)
    (and (= from-type "enum") (= to-type "string"))
    {:safe true}

    ;; RISKY CONVERSIONS (data-dependent)

    ;; string → numeric (risky - depends on data)
    (and (= from-type "string") (contains? #{"int" "float"} to-type))
    {:warning (str "Converting string to " to-type " requires all values to be valid numbers. Invalid values will cause the conversion to fail.")}

    ;; string → boolean (risky - depends on data)
    (and (= from-type "string") (= to-type "boolean"))
    {:warning "Converting string to boolean requires all values to be 't', 'f', 'true', 'false', 'yes', 'no', '1', '0'. Invalid values will cause the conversion to fail."}

    ;; string → timestamp (risky - depends on data)
    (and (= from-type "string") (= to-type "timestamp"))
    {:warning "Converting string to timestamp requires all values to be valid timestamp formats. Invalid values will cause the conversion to fail."}

    ;; string → json (lossy - invalid JSON becomes NULL)
    (and (= from-type "string") (= to-type "json"))
    {:warning "Converting string to json will set non-JSON values to NULL. This may result in data loss."}

    ;; string → enum (risky - values must be in enum set)
    (and (= from-type "string") (= to-type "enum"))
    {:warning "Converting string to enum requires all values to be valid enum values. Invalid values will cause the conversion to fail."}

    ;; FORBIDDEN: avatar → json (current implementation nulls everything)
    (and (= from-type "avatar") (= to-type "json"))
    {:error "Cannot convert avatar to json: Avatar URLs/data cannot be meaningfully converted to JSON."
     :type ::forbidden-conversion
     :suggestion "Convert to string first if you need to preserve the data, then manually transform to valid JSON if needed."}

    ;; FORBIDDEN: json → numeric
    (and (contains? (:json type-families) from-type)
         (contains? #{"int" "float"} to-type))
    {:error (str "Cannot convert " from-type " to " to-type ": No meaningful automatic conversion exists.")
     :type ::forbidden-conversion
     :suggestion "Extract numeric fields from JSON manually before converting."}

    ;; FORBIDDEN: json → boolean
    (and (contains? (:json type-families) from-type)
         (= to-type "boolean"))
    {:error (str "Cannot convert " from-type " to boolean: No meaningful automatic conversion exists.")
     :type ::forbidden-conversion
     :suggestion "Extract boolean fields from JSON manually before converting."}

    ;; FORBIDDEN: timestamp → int (semantic mismatch)
    (and (= from-type "timestamp") (= to-type "int"))
    {:error "Cannot convert timestamp to int: Use explicit epoch conversion if needed."
     :type ::forbidden-conversion
     :suggestion "Create a new attribute and populate it with epoch timestamps explicitly."}

    ;; FORBIDDEN: boolean → numeric
    (and (= from-type "boolean") (contains? #{"int" "float"} to-type))
    {:error (str "Cannot convert boolean to " to-type ": Semantic mismatch.")
     :type ::forbidden-conversion
     :suggestion "Convert to string first if you need '0'/'1' representation, or create explicit mapping logic."}

    ;; FORBIDDEN: reference → non-reference (losing referential integrity)
    (and (contains? (:reference type-families) from-type)
         (not (contains? (:reference type-families) to-type))
         (not= to-type "string"))
    {:error (str "Cannot convert " from-type " to " to-type ": This would lose referential integrity.")
     :type ::forbidden-conversion
     :suggestion "Convert to string first if you need to preserve entity IDs."}

    ;; Default: Unknown/unsupported conversion
    :else
    {:error (str "Unsupported type conversion from " from-type " to " to-type ".")
     :type ::unsupported-conversion
     :suggestion "This conversion path has not been validated. Please review the type compatibility matrix."}))

(defn can-convert-type?
  "Returns true if the type conversion is allowed (safe or warning), false if forbidden.
   Use this for quick yes/no checks. For detailed info, use validate-type-conversion."
  [from-type to-type]
  (let [result (validate-type-conversion from-type to-type)]
    (not (:error result))))

(defn get-conversion-level
  "Returns the risk level of a type conversion: :safe, :warning, or :error"
  [from-type to-type]
  (let [result (validate-type-conversion from-type to-type)]
    (cond
      (:error result) :error
      (:warning result) :warning
      :else :safe)))

(def all-types
  "All available attribute types in the system"
  ["string" "avatar" "transit" "hashed" ;; text family
   "json" "encrypted" "currency" "timeperiod" ;; json family
   "int" "float" ;; numeric family
   "user" "group" "role" ;; reference family
   "timestamp" ;; temporal
   "boolean" ;; boolean
   "enum"]) ;; enum

(defn get-allowed-conversions
  "Returns a map of all possible target types grouped by safety level.

   Returns:
   {:safe [types that are safe to convert to]
    :warning [types that might work but are risky]
    :forbidden [types that are blocked]}

   Useful for populating UI dropdowns with visual indicators.

   Example:
   (get-allowed-conversions \"avatar\")
   => {:safe [\"string\" \"transit\" \"hashed\" \"avatar\"]
       :warning []
       :forbidden [\"json\" \"int\" \"float\" ...]}"
  [from-type]
  (reduce
    (fn [acc to-type]
      (let [level (get-conversion-level from-type to-type)]
        (update acc level (fnil conj []) to-type)))
    {:safe []
     :warning []
     :forbidden []}
    all-types))

(defn get-convertible-types
  "Returns only the types that CAN be converted to (safe or warning, but not forbidden).
   This is useful for filtering dropdown options to show only valid choices.

   Options:
   - :include-warnings? true (default) - includes both safe and risky conversions
   - :include-warnings? false - only safe conversions

   Example:
   (get-convertible-types \"avatar\")
   => [\"string\" \"transit\" \"hashed\" \"avatar\"]

   (get-convertible-types \"avatar\" :include-warnings? false)
   => [\"string\" \"transit\" \"hashed\" \"avatar\"]"
  ([from-type]
   (get-convertible-types from-type {:include-warnings? true}))
  ([from-type {:keys [include-warnings?]
               :or {include-warnings? true}}]
   (let [allowed (get-allowed-conversions from-type)]
     (if include-warnings?
       (concat (:safe allowed) (:warning allowed))
       (:safe allowed)))))

(defn get-type-conversion-info
  "Returns detailed information about a type conversion for UI display.

   Returns:
   {:level :safe|:warning|:error
    :allowed? true|false
    :badge-color \"green\"|\"yellow\"|\"red\"
    :icon \"✓\"|\"⚠\"|\"✗\"
    :message \"Human readable message\"
    :warning \"Warning message\" (if level is :warning)
    :error \"Error message\" (if level is :error)
    :suggestion \"Suggestion for forbidden conversions\" (if level is :error)}

   Example:
   (get-type-conversion-info \"string\" \"int\")
   => {:level :warning
       :allowed? true
       :badge-color \"yellow\"
       :icon \"⚠\"
       :warning \"Converting string to int requires all values...\"
       :message \"Converting string to int requires all values...\"}

   (get-type-conversion-info \"avatar\" \"json\")
   => {:level :error
       :allowed? false
       :badge-color \"red\"
       :icon \"✗\"
       :error \"Cannot convert avatar to json...\"
       :message \"Cannot convert avatar to json...\"
       :suggestion \"Convert to string first...\"}"
  [from-type to-type]
  (let [validation (validate-type-conversion from-type to-type)
        level (get-conversion-level from-type to-type)]
    (case level
      :safe
      {:level :safe
       :allowed? true
       :badge-color "green"
       :icon "✓"
       :message (str "Safe conversion from " from-type " to " to-type)}

      :warning
      {:level :warning
       :allowed? true
       :badge-color "yellow"
       :icon "⚠"
       :warning (:warning validation)
       :message (:warning validation)}

      :error
      {:level :error
       :allowed? false
       :badge-color "red"
       :icon "✗"
       :error (:error validation)
       :message (:error validation)
       :suggestion (:suggestion validation)})))

;;; End Type Conversion Validation System

(defprotocol EntityConstraintProtocol
  (set-entity-unique-constraints [this constraints])
  (update-entity-unique-constraints [this function])
  (get-entity-unique-constraints [this]))

(defprotocol AuditConfigurationProtocol
  (set-who-field [this name])
  (get-who-field [this])
  (set-when-field [this name])
  (get-when-field [this]))

(defprotocol ERDEntityAttributeProtocol
  (generate-attribute-id [this])
  (add-attribute [this attribute])
  (set-attribute [this attribute])
  (get-attribute [this euuid])
  (update-attribute [this euuid f])
  (remove-attribute [this attribute]))

(defrecord ERDRelation [euuid from to from-label to-label cardinality path active claimed-by])
(defrecord NewERDRelation [euuid entity type])
(defrecord ERDEntityAttribute [euuid seq name constraint type configuration active])

(defn cloned? [{:keys [clone]}] clone)
(defn original [{:keys [original]}] original)

(defrecord ERDEntity [euuid position width height name attributes type configuration clone original active claimed-by]
  ;;
  EntityConstraintProtocol
  (set-entity-unique-constraints [this constraints]
    (assoc-in this [:configuration :constraints :unique] constraints))
  (update-entity-unique-constraints [this f]
    (update-in this [:configuration :constraints :unique] f))
  (get-entity-unique-constraints [this]
    (let [active-attributes (set (map :euuid (filter :active (:attributes this))))]
      (reduce
        (fn [r constraint-group]
          (if-some [filtered-group (not-empty (filter active-attributes constraint-group))]
            (conj r (vec filtered-group))
            r))
        []
        (get-in this [:configuration :constraints :unique]))))
  ;;
  ERDEntityAttributeProtocol
  (generate-attribute-id [_]
    (generate-uuid))
  (add-attribute [{:keys [attributes]
                   :as this} {:keys [euuid]
                              :as attribute}]
    {:pre [(instance? ERDEntityAttribute attribute)]}
    (let [attribute (map->ERDEntityAttribute attribute)
          euuid (if (some? euuid)
                  euuid
                  (generate-attribute-id this))
          entity (update this :attributes (fnil conj [])
                         (assoc attribute
                           :euuid euuid
                           :seq (count attributes)))]
      (if (= "unique" (:constraint attribute))
        (update-entity-unique-constraints
          entity
          (fnil
            (fn [current]
              (update current 0 (comp distinct conj) euuid))
            [[]]))
        entity)))
  (get-attribute [{:keys [attributes]} euuid]
    (if-let [attribute (some #(when (= euuid (:euuid %)) %) attributes)]
      attribute
      (throw
        (ex-info
          (str "Couldn't find attribute with euuid " euuid)
          {:euuid euuid
           :euuids (map :euuid attributes)}))))
  (set-attribute [{:keys [attributes]
                   :as this}
                  {ct :constraint
                   :as attribute
                   euuid :euuid}]
    (let [p (.indexOf (mapv :euuid attributes) (:euuid attribute))]
      (if (neg? p)
        (throw
          (ex-info
            "Attribute not found"
            {:attribute attribute
             :attributes attributes}))
        (let [{pt :constraint} (get attributes p)
              entity (assoc-in this [:attributes p] attribute)]
          (cond
            ;; If once was unique and currently isn't
            (and (= "unique" pt) (not= "unique" ct))
            (update-entity-unique-constraints
              entity
              (fn [constraints]
                (mapv #(vec (remove #{euuid} %)) constraints)))
            ;; If now is unique and previously wasn't
            (and (= "unique" ct) (not= "unique" pt))
            (update-entity-unique-constraints
              entity
              (fnil #(update % 0 conj euuid) [[]]))
            ;; Otherwise return changed entity
            :else entity)))))
  (update-attribute [{:keys [attributes]
                      :as this} euuid f]
    (if-let [{pt :constraint
              :as attribute} (some #(when (= euuid (:euuid %)) %) attributes)]
      (let [{ct :constraint
             :as attribute'} (f attribute)
            entity (set-attribute this attribute')]
        (cond
          ;; If once was unique and currently isn't
          (and (= "unique" pt) (not= "unique" ct))
          (update-entity-unique-constraints
            entity
            (fn [constraints]
              (mapv #(vec (remove #{euuid} %)) constraints)))
          ;; If now is unique and previously wasn't
          (and (= "unique" ct) (not= "unique" pt))
          (update-entity-unique-constraints
            entity
            (fnil #(update % 0 conj euuid) [[]]))
          ;; Otherwise return changed entity
          :else entity))
      (throw (ex-info (str "Couldn't find attribute with euuid " euuid)
                      {:euuid euuid
                       :euuids (map :euuid attributes)}))))
  (remove-attribute [{:keys [attributes]
                      :as this} {euuid :euuid}]
    (->
      this
      (assoc :attributes
        (vec
          (keep-indexed
            (fn [idx a] (assoc a :seq idx))
            (remove #(= euuid (:euuid %)) attributes))))
      (update update-entity-unique-constraints
              (fn [unique-bindings]
                (reduce
                  (fn [r group]
                    (let [group' (vec
                                   (remove
                                     (some-fn
                                       #{euuid}
                                       string?)
                                     group))]
                      (if (empty? group') r (conj r group'))))
                  []
                  unique-bindings))))))

(defprotocol ERDModelActions
  (generate-entity-id [this] "Returns unique id")
  (generate-relation-id [this] "Returns unique id")
  (get-entity [this] [this euuid] "Returns node in model with name if provided, otherwise it returns last entity")
  (get-entities [this] "Returns vector of entities")
  (add-entity [this entity] "Adds new entity to model")
  (set-entity [this entity] "Sets entity in model ignoring previous state")
  (update-entity [this euuid function] "Sets entity in model ignoring previous state")
  (remove-entity [this entity] "Removes node from model")
  (replace-entity
    [this entity replacement]
    "Repaces entity in model with replacement and reconects all previous connections")
  (get-entity-relations
    [this entity]
    "Returns all relations for given entity where relations
    are returned in such maner that input entity is always in :from field")
  (get-relation [this euuid] "Returns relation between entities")
  (get-relations [this] "Returns vector of relations")
  (get-relations-between [this entity1 entity2] "Returns all found relations that exist between entity1 entity2")
  (add-relation [this relation])
  (create-relation [this from to] [this from to type] [this from to type path] [euuid this from to type path] "Creates relation from entity to entity")
  (set-relation [this relation] "Sets relation in model ignoring previous values")
  (update-relation [this euuid function] "Updates relation in model by merging new values upon old ones")
  (remove-relation [this relation] "Removes relation between entities"))

(defprotocol ERDModelReconciliationProtocol
  (reconcile
    [this model]
    "Function reconciles this with that. Starting point should be reconcilation
    of some 'this' with ERDModel, and that might lead to reconiliation of relations
    and entities with 'this'. Therefore reconcile this with that"))

(defrecord ERDModel [entities relations configuration clones version]
  AuditConfigurationProtocol
  (set-who-field
    [this name]
    (assoc-in this [:configuration :audit :who] name))
  (get-who-field [this]
    (get-in this [:configuration :audit :who]))
  (set-when-field
    [this name]
    (assoc-in this [:configuration :audit :when] name))
  (get-when-field [this]
    (get-in this [:configuration :audit :when])))

(extend-protocol ERDModelActions
  nil
  (get-entities [_] nil)
  (get-entity [_ _] nil)
  (get-relations [_] nil)
  (get-relation [_ _] nil)
  (get-entity-relations [_ _] nil)
  (add-entity [_ _] nil)
  (remove-entity [_ _] nil)
  (replace-entity [_ _ _] nil))

(defprotocol DatasetProtocol
  (deploy!
    [this version]
    "Deploys dataset version")
  (recall!
    [this version]
    "Deletes a specific dataset version by {:euuid version-uuid}. Only works on deployed versions.
     If it's the only deployed version, cleans up and returns.
     If it's the most recent (but not only), rolls back to previous version.
     Otherwise just deletes it.")
  (destroy!
    [this dataset]
    "Nuclear delete: removes ALL dataset versions and all dataset data. Affects DB as well. All is gone")
  (get-model
    [this]
    "Returns all entities and relations for given account")
  (mount
    [this module]
    "Mounts module in EYWA by storing its dataset and special handlers")
  (reload
    [this]
    [this module]
    "Reloads module. If module is not specified, than whole dataset is reloaded")
  (unmount
    [this module]
    "Removes module from EYWA by removing all data for that module")
  (get-last-deployed
    [this] [this offset]
    "Returns last deployed model")
  (setup
    [this]
    [this options]
    "Setup dataset for given DB target")
  (tear-down
    [this]
    [this options]
    "Remove dataset from given DB target")
  (backup
    [this options]
    "Backups dataset for given target based on provided options")
  ; (create-db
  ;   [this]
  ;   "Creates database instance")
  ; (drop-db
  ;   [this]
  ;   "Drops database instance")
  ; (backup-db
  ;   [this options]
  ;   "Creates database backup")
  (create-deploy-history
    [this]
    "Prepares db/storage for deploy history")
  (add-to-deploy-history
    [this] [this model]
    "Stacks current global model to deploy history"))

(defn invert-relation [relation]
  (with-meta
    (-> relation
        (clojure.set/rename-keys
          {:from :to
           :from-label :to-label
           :to :from
           :to-label :from-label})
        (assoc :cardinality
          (case (:cardinality relation)
            "o2m" "m2o"
            "o2o" "o2o"
            "m2m" "m2m"
            "m2o" "o2m"
            relation))
        map->ERDRelation)
    (merge
      (meta relation)
      {:dataset.relation/inverted? true})))

(defn inverted-relation? [relation] (:dataset.relation/inverted? (meta relation)))

(defn normalize-relation
  [relation]
  (if (inverted-relation? relation)
    (with-meta
      (invert-relation relation)
      (dissoc (meta relation) :dataset.relation/inverted?))
    relation))

(defn direct-relation-from
  [{:keys [euuid]} {:keys [from to to-label]
                    :as relation}]
  (if (= from to)
    (if (not-empty to-label)
      relation
      (invert-relation relation))
    (if (= euuid (:euuid from)) relation
        (invert-relation relation))))

(defn direct-relations-from
  [entity relations]
  (map #(direct-relation-from entity %) relations))

(defn focus-entity-relations
  "Function returns entity rel focused on entity, inverting
  all relations that are not outgoing from input entity"
  ([model entity]
   (direct-relations-from entity (get-entity-relations model entity)))
  ([model entity entity']
   (direct-relations-from entity (get-relations-between model entity entity'))))

(defn align-relations
  "Function aligns two relations. By comparing source and
  target node. If needed second relation will be inverted"
  [relation1 relation2]
  (if (= (:euuid relation1) (:euuid relation2))
    (if (= (get-in relation1 [:from :euuid])
           (get-in relation2 [:from :euuid]))
      [relation1 relation2]
      (if (= (get-in relation1 [:from :euuid])
             (get-in relation2 [:to :euuid]))
        [relation1 (invert-relation relation2)]
        (throw
          (ex-info
            "Cannot align relations that connect different entities"
            {:relations [relation1 relation2]}))))
    (throw
      (ex-info
        "Cannot align different relations"
        {:relations [relation1 relation2]}))))

(defn same-relations?
  "Function returns true if two relations are the same, by comparing
  relation1 to relation2 and inverted version of relation2"
  [relation1 relation2]
  (if (= (:euuid relation1) (:euuid relation2))
    (let [[relation1' relation2' relation2'']
          (map
            #(->
               %
               (select-keys [:to-label :from-label :cardinality :to :from])
               (update :to :euuid)
               (update :from :euuid))
            [relation1 relation2 (invert-relation relation2)])
          same? (boolean
                  (or
                    (= relation1' relation2')
                    (= relation1' relation2'')))]
      same?)
    false))

(defn- merge-entity-attributes
  "Merges attributes from two entities, accumulating all historical attributes.
   Attributes in entity2 are marked :active true, attributes only in entity1 are marked :active false.
   This implements 'last deployed wins' at the entity level for attribute active flags."
  [entity1 entity2]
  (let [attrs1 (or (:attributes entity1) [])
        attrs2 (or (:attributes entity2) [])
        ;; Build maps by attribute UUID for fast lookup
        attrs1-by-id (into {} (map (juxt :euuid identity) attrs1))
        attrs2-by-id (into {} (map (juxt :euuid identity) attrs2))
        ;; Get all unique attribute UUIDs
        all-attr-uuids (clojure.set/union (set (keys attrs1-by-id))
                                          (set (keys attrs2-by-id)))
        ;; Merge attributes: model2 wins for properties, but accumulate all
        merged-attrs (vec
                       (for [attr-uuid all-attr-uuids]
                         (if-let [attr2 (get attrs2-by-id attr-uuid)]
                          ;; Attribute in model2: use it with :active true
                           (assoc attr2 :active true)
                          ;; Attribute only in model1: keep it with :active false
                           (assoc (get attrs1-by-id attr-uuid) :active false))))]
    ;; Return entity2 as base with merged attributes
    (assoc entity2 :attributes merged-attrs)))

(defn join-models [model1 model2]
  (->
    model1
   ;; Handled by ensure active attributes
    (update :configuration deep-merge (:configuration model2))
    (update :clones deep-merge (:clones model2))
   ;; Ensure active attributes
    (as-> joined-model
         ;; Merge entities: handle both claimed-by AND attributes
          (reduce
            (fn [m {:keys [euuid]
                    :as entity}]
              (let [entity1 (get-entity model1 euuid)
                    entity2 (get-entity model2 euuid)
                    claims-1 (get entity1 :claimed-by #{})
                    claims-2 (get entity2 :claimed-by #{})
                    claims (clojure.set/union claims-1 claims-2)
                  ;; Entity is active if present in model2 (last deployment wins)
                    entity-active? (some? entity2)
                  ;; Merge attributes if both entities exist
                    merged-entity (if (and entity1 entity2)
                                    (merge-entity-attributes entity1 entity2)
                                    entity)]
                (set-entity m (assoc merged-entity
                                :claimed-by claims
                                :active entity-active?))))
            joined-model
            (mapcat get-entities [model1 model2]))
     ;; Merge relations: handle claimed-by AND active
      (reduce
        (fn [m {:keys [euuid]
                :as relation}]
          (let [relation1 (get-relation model1 euuid)
                relation2 (get-relation model2 euuid)
                claims-1 (get relation1 :claimed-by #{})
                claims-2 (get relation2 :claimed-by #{})
                claims (clojure.set/union claims-1 claims-2)
              ;; Relation is active if present in model2 (last deployment wins)
                relation-active? (some? relation2)]
            (set-relation m (assoc relation
                              :claimed-by claims
                              :active relation-active?))))
        joined-model
        (mapcat get-relations [model1 model2])))))


(defn activate-model
  [model deployed-versions]
  (as-> model m
    (reduce
      (fn [m entity]
        (set-entity m (assoc entity :active
                             (boolean
                               (not-empty
                                 (clojure.set/intersection
                                   (:claimed-by entity)
                                   deployed-versions))))))
      m
      (get-entities m))
    (reduce
      (fn [m relation]
        (set-relation m (assoc relation :active
                               (boolean
                                 (not-empty
                                   (clojure.set/intersection
                                     (:claimed-by relation)
                                     deployed-versions))))))
      m
      (get-relations m))))

(defn disjoin-model [model1 model2]
  (reduce
    (fn [final entity]
      (remove-entity final entity))
    model1
    (get-entities model2)))

;; Ownership tracking functions
(defn add-claim
  "Adds version-uuid to the :claimed-by set of an entity or relation.
   Does NOT set :active flag - that's determined by join-models based on 'last deployment wins'."
  [model entity-or-relation-uuid version-uuid]
  (cond
    ;; Check if it's an entity
    (get-in model [:entities entity-or-relation-uuid])
    (update-in model [:entities entity-or-relation-uuid :claimed-by]
               (fnil conj #{}) version-uuid)
    ;; Check if it's a relation
    (get-in model [:relations entity-or-relation-uuid])
    (update-in model [:relations entity-or-relation-uuid :claimed-by]
               (fnil conj #{}) version-uuid)
    ;; Not found
    :else model))

(defn add-claims
  "Adds version-uuid as a claim to all entities and relations in the provided model"
  ([model version-uuid] (add-claims model model version-uuid))
  ([global-model new-model version-uuid]
   (as-> global-model gm
     (reduce
       (fn [gm entity]
         (add-claim gm (:euuid entity) version-uuid))
       gm
       (get-entities new-model))
     (reduce
       (fn [gm relation]
         (add-claim gm (:euuid relation) version-uuid))
       gm
       (get-relations new-model)))))

(defn remove-claim
  "Removes version-uuid from the :claimed-by set of an entity or relation"
  [model entity-or-relation-uuid version-uuid]
  (cond
    ;; Check if it's an entity
    (get-in model [:entities entity-or-relation-uuid])
    (update-in model [:entities entity-or-relation-uuid]
               (fn [entity]
                 (let [entity' (update entity :claimed-by (fnil disj #{}) version-uuid)]
                   (if (empty? (:claimed-by entity'))
                     (assoc entity' :active false)
                     entity'))))
    ;; Check if it's a relation
    (get-in model [:relations entity-or-relation-uuid])
    (update-in model [:relations entity-or-relation-uuid]
               (fn [relation]
                 (let [relation' (update relation :claimed-by (fnil disj #{}) version-uuid)]
                   (if (empty? (:claimed-by relation'))
                     (assoc relation' :active false)
                     relation'))))
    ;; Not found
    :else model))

(defn remove-claims
  "Removes all version-uuids from claims in the model, updates :active flag"
  [model version-uuids]
  (let [;; Remove claims from entities
        updated-entities
        (reduce-kv
          (fn [entities entity-uuid entity]
            (let [remaining-claims (clojure.set/difference
                                     (get entity :claimed-by #{})
                                     version-uuids)
                  active? (not-empty remaining-claims)]
              (assoc entities entity-uuid
                     (assoc entity
                       :claimed-by remaining-claims
                       :active active?))))
          {}
          (:entities model))
        ;; Remove claims from relations
        updated-relations
        (reduce-kv
          (fn [relations relation-uuid relation]
            (let [remaining-claims (clojure.set/difference
                                     (get relation :claimed-by #{})
                                     version-uuids)
                  active? (not-empty remaining-claims)]
              (assoc relations relation-uuid
                     (assoc relation
                       :claimed-by remaining-claims
                       :active active?))))
          {}
          (:relations model))]
    (assoc model
      :entities updated-entities
      :relations updated-relations)))

(defn find-exclusive-entities
  "Returns entities that are ONLY claimed by the provided version-uuids"
  [model version-uuids]
  (let [version-set (set version-uuids)]
    (filter
      (fn [entity]
        (let [claims (get entity :claimed-by #{})]
          ;; Skip entities without claims (legacy system entities)
          ;; Exclusive if all claims are within version-uuids
          (and (not-empty claims)
               (empty? (clojure.set/difference claims version-set)))))
      (get-entities model))))

(defn find-exclusive-relations
  "Returns relations that are ONLY claimed by the provided version-uuids"
  [model version-uuids]
  (let [version-set (set version-uuids)]
    (filter
      (fn [relation]
        (let [claims (get relation :claimed-by #{})]
          ;; Skip relations without claims (legacy system relations)
          ;; Exclusive if all claims are within version-uuids
          (and (not-empty claims)
               (empty? (clojure.set/difference claims version-set)))))
      (get-relations model))))

(defprotocol ERDModelProjectionProtocol
  (added? [this] "Returns true if this is added or false otherwise")
  (removed? [this] "Returns true if this is removed or false otherwise")
  (diff? [this] "Returns true if this has diff or false otherwise")
  (diff [this] "Returns diff content")
  (mark-added [this] "Marks this ass added")
  (mark-removed [this] "Marks this as removed")
  (mark-diff [this diff] "Adds diff content")
  (suppress [this] "Returns this before projection")
  (project
    [this that]
    "Returns projection of this on that updating each value in nested structure with keys:
    * added?
    * removed?
    * diff
    * active")
  (clean-projection-meta [this] "Returns "))

(defn projection-data [x] (:dataset/projection (meta x)))

(defn attribute-has-diff?
  [attribute]
  (boolean (not-empty (:diff (projection-data attribute)))))

(defn new-attribute? [attribute] (boolean (:added? (projection-data attribute))))

(defn removed-attribute? [attribute] (boolean (:removed? (projection-data attribute))))

(def attribute-changed? (some-fn new-attribute? removed-attribute? attribute-has-diff?))
(def attribute-not-changed? (complement attribute-changed?))

(defn entity-has-diff?
  [{:keys [attributes]
    :as entity}]
  (let [{:keys [diff added?]} (projection-data entity)]
    (and
      (not added?)
      (or
        (not-empty (dissoc diff :width :height))
        (some attribute-changed? attributes)))))

(defn new-entity? [e] (boolean (:added? (projection-data e))))
(defn strong-entity? [{:keys [type]}] (= "STRONG" type))
(defn weak-entity? [{:keys [type]}] (= "WEAK" type))

(def entity-changed? (some-fn new-entity? entity-has-diff?))
(def entity-not-changed? (complement entity-changed?))

(defn new-relation? [r] (boolean (:added? (projection-data r))))
(defn relation-has-diff? [r] (some? (:diff (projection-data r))))

(def relation-changed? (some-fn new-relation? relation-has-diff?))
(def relation-not-changed? (complement relation-changed?))

(defn recursive-relation? [relation]
  (boolean (#{"tree"} (:cardinality relation))))

(extend-protocol ERDModelProjectionProtocol
  ;; ENTITY ATTRIBUTE
  #?(:clj neyho.eywa.dataset.core.ERDEntityAttribute
     :cljs neyho.eywa.dataset.core/ERDEntityAttribute)
  (mark-added [this] (vary-meta (assoc this :active true) assoc-in [:dataset/projection :added?] true))
  (mark-removed [this] (vary-meta (assoc this :active false) assoc-in [:dataset/projection :removed?] true))
  (mark-diff [this diff] (vary-meta this assoc-in [:dataset/projection :diff] diff))
  (added? [this] (boolean (:added? (projection-data this))))
  (removed? [this] (boolean (:removed? (projection-data this))))
  (diff? [this] (boolean (not-empty (:diff (projection-data this)))))
  (diff [this] (:diff (projection-data this)))
  (clean-projection-meta [this] (vary-meta this dissoc :dataset/projection))
  (suppress [this]
    (when-let [this' (cond
                       (added? this) nil
                       (diff? this) (merge this (diff this))
                       :else this)]
      (with-meta this' nil)))
  (project
    [{this-id :euuid
      :as this}
     {that-id :euuid
      :as that}]
    {:pre [(or
             (nil? that)
             (and
               (instance? ERDEntityAttribute that)
               (= this-id that-id)))]}
    ;; FIXME configuration should also implement this protocol or
    ;; at least some multimethod that would return configuration diff
    ;; based on attribute type
    (if (some? that)
      (letfn [(focus-attribute [attribute]
                (select-keys attribute [:euuid :name :type :constraint :active :configuration]))]
        (let [[{config :configuration} n _]
              (clojure.data/diff
                (focus-attribute that)
                (focus-attribute this))]
          ;; 1. Check configuration has been extended and that contains more
          ;;    information than this
          ;; 2. Check if some existing attribute changes were made
          (if (or (some? n) (not-empty config))
            ;; READ FIXME - this is dirty fix
            (mark-diff that (or n config))
            that)))
      (mark-removed this)))
  ;; ENTITY
  #?(:clj neyho.eywa.dataset.core.ERDEntity
     :cljs neyho.eywa.dataset.core/ERDEntity)
  (mark-added [this]
    (vary-meta
      (update this :attributes #(mapv mark-added %))
      assoc-in [:dataset/projection :added?] true))
  (mark-removed [this]
    (vary-meta
      (update this :attributes #(mapv mark-removed %))
      assoc-in [:dataset/projection :removed?] true))
  (mark-diff [this diff] (vary-meta this assoc-in [:dataset/projection :diff] diff))
  (added? [this] (boolean (:added? (projection-data this))))
  (removed? [this] (boolean (:removed? (projection-data this))))
  (diff? [this]
    (let [{:keys [diff added?]} (projection-data this)]
      (and
        (not added?)
        (or
          (not-empty (dissoc diff :width :height))
          (some attribute-changed? (:attributes this))))))
  (diff [this] (:diff (projection-data this)))
  (clean-projection-meta [this] (vary-meta this dissoc :dataset/projection))
  (suppress [this]
    (when-let [this'
               (cond
                 (added? this) nil
                 ;;
                 (diff? this)
                 (->
                   this
                   (merge this (dissoc (diff this) :attributes))
                   (update :attributes
                           (fn [as]
                             (vec
                               (remove nil? (map suppress as))))))
                 #_(let [cso (get-in (diff this) [:configuration :constraints :unique])]
                     (cond->

                       (some? cso) (assoc-in [:configuration :constraints :unique] cso)))
                 :else this)]
      (with-meta this' nil)))
  (project
    [{this-id :euuid
      :as this}
     {that-id :euuid
      :as that}]
    {:pre [(or
             (nil? that)
             (and
               (instance? ERDEntity that)
               (= this-id that-id)))]}
    ;; If that exists
    (if (some? that)
      ;; 
      (let [that-ids (set (map :euuid (:attributes that)))
            this-ids (set (map :euuid (:attributes this)))
            ;; Separate new ids from old and same ids
            [oid nid sid] (clojure.data/diff this-ids that-ids)
            removed-attributes (when (not-empty oid)
                                 (map
                                   mark-removed
                                   ;; Filter from this attributes
                                   ;; all attributes that are not in that model 
                                   (filter
                                     (every-pred
                                       :active
                                       (comp oid :euuid))
                                     (:attributes this))))
            attributes' (into
                          (reduce
                            ;; Reduce attributes
                            (fn [as {:keys [euuid]
                                     :as attribute}]
                              (conj
                                as
                                (cond-> attribute
                                  (and
                                    (not-empty nid)
                                    (nid euuid))
                                  mark-added
                                  ;;
                                  (and
                                    (set? sid)
                                    (sid euuid))
                                  (as-> a
                                        (project (get-attribute this euuid) a)))))
                            []
                            (:attributes that))
                          ;; at last conj removed attributes with marked :removed? keyword 
                          removed-attributes)
            [o _ _] (when (and this that)
                      (clojure.data/diff
                        (select-keys this [:name :width :height])
                        (select-keys that [:name :width :height])))
            cso (get-in this [:configuration :constraints :unique])
            csn (get-in that [:configuration :constraints :unique])
            changed-attributes (vec (filter attribute-changed? attributes'))]
        (cond->
          (assoc that :attributes attributes')
          ;;
          (some? o)
          (vary-meta assoc-in [:dataset/projection :diff] o)
          ;;
          (not= cso csn)
          (vary-meta assoc-in [:dataset/projection :diff :configuration :constraints :unique] cso)
          ;;
          (not-empty changed-attributes)
          (vary-meta assoc-in [:dataset/projection :diff :attributes] changed-attributes)))
      ;; If that doesn't exist return this with :removed? metadata
      (mark-removed this)))
  #?(:clj neyho.eywa.dataset.core.ERDRelation
     :cljs neyho.eywa.dataset.core/ERDRelation)
  (mark-added [this] (vary-meta this assoc-in [:dataset/projection :added?] true))
  (mark-removed [this] (vary-meta this assoc-in [:dataset/projection :removed?] true))
  (mark-diff [this diff] (vary-meta this assoc-in [:dataset/projection :diff] diff))
  (added? [this] (boolean (:added? (projection-data this))))
  (removed? [this] (boolean (:removed? (projection-data this))))
  (diff? [this] (boolean (not-empty (:diff (projection-data this)))))
  (diff [this] (:diff (projection-data this)))
  (clean-projection-meta [this] (vary-meta this dissoc :dataset/projection))
  (suppress [this]
    (when-let [this'
               (cond
                 (added? this) nil
                 (diff? this) (->
                                this
                                (merge (dissoc (diff this) :from :to))
                                (update :from suppress)
                                (update :to suppress)
                                (with-meta nil))
                 :else this)]
      (with-meta this' nil)))
  (project
    [this that]
    {:pre [(or
             (nil? that)
             (and
               (instance? ERDRelation that)
               (= (:euuid this) (:euuid that))))]}
    ;; If that exists
    (if (some? that)
      ;; Check if relations are the same
      (let [ks [:from-label :to-label :cardinality]
            this (normalize-relation this)
            that (normalize-relation that)
            ;; Compute difference between this and that
            [o _] (clojure.data/diff
                    (select-keys this ks)
                    (select-keys that ks))
            ;; Check only entity names since that might
            ;; affect relation
            from-projection (when (not=
                                    (:name (:from this))
                                    (:name (:from that)))
                              {:name (:name (:from that))})
            to-projection (when (not=
                                  (:name (:to this))
                                  (:name (:to that)))
                            {:name (:name (:to that))})
            o' (cond-> o
                 from-projection (assoc :from from-projection)
                 to-projection (assoc :to to-projection))]
        ;; And if there is some difference than
        (if (some? o')
          ;; return that with projected difference
          (mark-diff that o')
          ;; otherwise return that
          that))
      ;; If that does't exist than return this with projected removed metadata
      (mark-removed this)))
  #?(:clj neyho.eywa.dataset.core.ERDModel
     :cljs neyho.eywa.dataset.core/ERDModel)
  (clean-projection-meta [this] (vary-meta this dissoc :dataset/projection))
  (suppress [this]
    (with-meta
      (reduce
        (fn [m r]
          (->
            m
            (set-relation (suppress r))
            (with-meta nil)))
        (reduce
          (fn [m e]
            (->
              m
              (set-entity (suppress e))
              (with-meta nil)))
          this
          (get-entities this))
        (get-relations this))
      nil))
  (project
    [this that]
    (as-> that projection
      (reduce
        (fn [m {id :euuid
                :as e}]
          (set-entity m (project (get-entity this id) e)))
        projection
        (get-entities projection))
      (reduce
        (fn [m {id :euuid
                :as r}]
          (set-relation m (project (get-relation this id) r)))
        projection
        (get-relations projection))
      ;; Take into account relations that are missing
      ;; in that and entites have been changed in that
      (let [that-relations (get-relations projection)
            this-relations (distinct
                             (mapcat
                               (comp normalize-relation #(focus-entity-relations this %))
                               (filter entity-changed? (get-entities projection))))
            that-relation-euuids (set (map :euuid that-relations))
            target-relations (remove
                               (comp that-relation-euuids :euuid)
                               this-relations)]
        (reduce
          (fn [m {id :euuid
                  {from-euuid :euuid} :from
                  {to-euuid :euuid} :to
                  :as r}]
            (let [from (get-entity projection from-euuid)
                  to (get-entity projection to-euuid)]
              (if (and from to)
                (set-relation m
                              (project (get-relation this id)
                                       (-> r
                                           (assoc :from from)
                                           (assoc :to to))))
                m)))
          projection
          target-relations))))
  nil
  (mark-removed [_] nil)
  (mark-added [_] nil)
  (mark-diff [_ _] nil)
  ; (suppress [_ _])
  (project [_ that] (when that (mark-added that))))


;;; RLS Guards - Row Level Security

;; Path labels - auto-generated A, B, C...
(def ^:private path-labels "ABCDEFGHIJKLMNOPQRSTUVWXYZ")

(defn get-path-label
  "Get letter label for path index (0 -> A, 1 -> B, etc.)"
  [idx]
  (str (nth path-labels (mod idx 26))))

(defn iam-entity-type
  "Returns the IAM entity type keyword for a given entity UUID.
   Requires iam-uuids map with :user, :group, :role keys."
  [euuid iam-uuids]
  (cond
    (= euuid (:user iam-uuids)) :user
    (= euuid (:group iam-uuids)) :group
    (= euuid (:role iam-uuids)) :role
    :else nil))

(defn- discover-ref-paths
  "Discover ref attributes (type user/group/role) as direct paths to IAM entities.
   Returns vector of ref paths with :type :ref"
  [entity iam-uuids start-idx]
  (let [ref-types #{"user" "group" "role"}
        attributes (or (:attributes entity) [])]
    (->> attributes
         (filter (fn [attr]
                   (and (:active attr)
                        (contains? ref-types (:type attr)))))
         (map-indexed
           (fn [idx attr]
             (let [attr-type (:type attr)
                   target (keyword attr-type)  ;; "user" -> :user
                   target-euuid (get iam-uuids target)]
               {:id (get-path-label (+ start-idx idx))
                :type :ref
                :target target
                :target-name (case target
                               :user "User"
                               :group "UserGroup"
                               :role "UserRole")
                :target-euuid target-euuid
                :attribute-euuid (:euuid attr)
                :attribute-name (:name attr)
                :depth 0})))
         vec)))


(defn- discover-relation-paths
  "Discover relation paths to IAM entities using BFS.
   Returns vector of paths with :type :relation or :type :hybrid.

   :relation - path ends at an IAM entity via relation
   :hybrid - path traverses relations then ends at a ref attribute (user/group/role type)"
  [model entity iam-uuids max-depth start-idx]
  (let [entity-euuid (:euuid entity)
        iam-entity-uuids (set (vals iam-uuids))]
    (loop [queue [{:entity entity
                   :steps []
                   :visited #{entity-euuid}}]
           paths []
           path-idx start-idx]
      (if (empty? queue)
        paths
        (let [{:keys [entity steps visited]} (first queue)
              remaining (rest queue)
              current-depth (count steps)]
          (if (>= current-depth max-depth)
            ;; Max depth reached, continue with remaining queue
            (recur remaining paths path-idx)
            ;; Explore relations from current entity
            (let [relations (focus-entity-relations model entity)
                  ;; Process each relation
                  new-items
                  (reduce
                    (fn [acc relation]
                      (let [target-entity (:to relation)
                            target-euuid (:euuid target-entity)
                            relation-label (or (:to-label relation) (:from-label relation) "")]
                        (if (contains? visited target-euuid)
                          acc ;; Skip already visited
                          (let [new-step {:relation-euuid (:euuid relation)
                                          :label relation-label
                                          :entity-euuid target-euuid
                                          :entity-name (:name target-entity)}
                                new-steps (conj steps new-step)
                                new-visited (conj visited target-euuid)]
                            (if (contains? iam-entity-uuids target-euuid)
                              ;; Found IAM entity via relation - add :relation path
                              (update acc :paths conj
                                      {:id (get-path-label (+ path-idx (count (:paths acc))))
                                       :type :relation
                                       :target (iam-entity-type target-euuid iam-uuids)
                                       :target-name (:name target-entity)
                                       :target-euuid target-euuid
                                       :steps new-steps
                                       :depth (count new-steps)})
                              ;; Not IAM - check for ref attributes AND continue BFS
                              (let [;; Find ref attributes on target entity that point to IAM
                                    ref-types #{"user" "group" "role"}
                                    target-refs (->> (:attributes target-entity)
                                                     (filter #(and (:active %)
                                                                   (contains? ref-types (:type %)))))
                                    ;; Create hybrid paths for each ref attribute
                                    hybrid-paths (map-indexed
                                                   (fn [idx attr]
                                                     (let [attr-target (keyword (:type attr))]
                                                       {:id (get-path-label (+ path-idx (count (:paths acc)) idx))
                                                        :type :hybrid
                                                        :target attr-target
                                                        :target-name (case attr-target
                                                                       :user "User"
                                                                       :group "UserGroup"
                                                                       :role "UserRole")
                                                        :target-euuid (get iam-uuids attr-target)
                                                        :steps new-steps
                                                        :attribute-euuid (:euuid attr)
                                                        :attribute-name (:name attr)
                                                        :depth (count new-steps)}))
                                                   target-refs)]
                                (-> acc
                                    ;; Add hybrid paths
                                    (update :paths into hybrid-paths)
                                    ;; Continue BFS for relations
                                    (update :queue conj
                                            {:entity target-entity
                                             :steps new-steps
                                             :visited new-visited}))))))))
                    {:paths []
                     :queue []}
                    relations)]
              (recur (into (vec remaining) (:queue new-items))
                     (into paths (:paths new-items))
                     (+ path-idx (count (:paths new-items)))))))))))


(defn discover-paths-to-iam
  "Discovers all paths from entity to IAM entities (User, UserGroup, UserRole).
   Finds:
   - Ref attributes (type user/group/role) as direct paths (:type :ref)
   - Relation paths via BFS traversal (:type :relation)
   - Hybrid paths: relations ending at a ref attribute (:type :hybrid)

   Arguments:
   - model: The ERD model
   - entity: The source entity to start from
   - iam-uuids: Map with :user, :group, :role keys containing entity UUIDs
   - max-depth: Maximum number of hops for relation paths (default 3)

   Returns a vector of paths:

   Ref path (direct attribute on entity):
   {:id \"A\"
    :type :ref
    :target :user | :group | :role
    :target-name \"User\" | \"UserGroup\" | \"UserRole\"
    :target-euuid #uuid \"...\"
    :attribute-euuid #uuid \"...\"
    :attribute-name \"created_by\"
    :depth 0}

   Relation path (ends at IAM entity via relation):
   {:id \"B\"
    :type :relation
    :target :user | :group | :role
    :target-name \"User\" | \"UserGroup\" | \"UserRole\"
    :target-euuid #uuid \"...\"
    :steps [{:relation-euuid #uuid \"...\"
             :label \"assigned_to\"
             :entity-euuid #uuid \"...\"
             :entity-name \"Entity\"}]
    :depth 1}

   Hybrid path (relations then ref attribute):
   {:id \"C\"
    :type :hybrid
    :target :user | :group | :role
    :target-name \"User\" | \"UserGroup\" | \"UserRole\"
    :target-euuid #uuid \"...\"
    :steps [{:relation-euuid #uuid \"...\"
             :label \"task\"
             :entity-euuid #uuid \"...\"
             :entity-name \"Task\"}]
    :attribute-euuid #uuid \"...\"
    :attribute-name \"created_by\"
    :depth 1}"
  ([model entity iam-uuids]
   (discover-paths-to-iam model entity iam-uuids 3))
  ([model entity iam-uuids max-depth]
   (when (and model entity)
     ;; First discover ref paths (direct attributes)
     (let [ref-paths (discover-ref-paths entity iam-uuids 0)
           ref-count (count ref-paths)
           ;; Then discover relation paths (BFS)
           relation-paths (discover-relation-paths model entity iam-uuids max-depth ref-count)]
       ;; Combine: refs first (depth 0), then relations (depth 1+)
       (into ref-paths relation-paths)))))


;; RLS Configuration Helpers

(defn get-rls-config
  "Get RLS configuration from entity"
  [entity]
  (get-in entity [:configuration :rls]))

(defn set-rls-config
  "Set RLS configuration on entity"
  [entity rls-config]
  (assoc-in entity [:configuration :rls] rls-config))

(defn rls-enabled?
  "Check if RLS is enabled for entity"
  [entity]
  (get-in entity [:configuration :rls :enabled] false))

(defn set-rls-enabled
  "Enable or disable RLS for entity"
  [entity enabled]
  (assoc-in entity [:configuration :rls :enabled] enabled))

(defn get-rls-guards
  "Get RLS guards from entity"
  [entity]
  (get-in entity [:configuration :rls :guards] []))

(defn set-rls-guards
  "Set RLS guards on entity"
  [entity guards]
  (assoc-in entity [:configuration :rls :guards] guards))

(defn add-rls-guard
  "Add a new RLS guard to entity"
  [entity guard]
  (update-in entity [:configuration :rls :guards]
             (fnil conj [])
             guard))

(defn remove-rls-guard
  "Remove an RLS guard by id"
  [entity guard-id]
  (update-in entity [:configuration :rls :guards]
             (fn [guards]
               (vec (remove #(= (:id %) guard-id) guards)))))

(defn find-guard-index
  "Find the index of a guard by id"
  [guards guard-id]
  (first (keep-indexed
          (fn [idx g] (when (= (:id g) guard-id) idx))
          guards)))

(defn toggle-rls-operation
  "Toggle a Read/Write operation on a guard"
  [entity guard-id operation]
  (let [guards (get-rls-guards entity)
        guard-idx (find-guard-index guards guard-id)]
    (if guard-idx
      (let [current-ops (get-in guards [guard-idx :operation] #{})
            new-ops (if (contains? current-ops operation)
                      (disj current-ops operation)
                      (conj current-ops operation))]
        (assoc-in entity [:configuration :rls :guards guard-idx :operation] new-ops))
      entity)))

(defn- path->condition
  "Convert a discovered path to a minimal condition for storage.
   Only stores UUIDs - no names that can go stale.

   Stored structure:
   - :ref      {:type :ref :attribute <uuid>}
   - :relation {:type :relation :steps [{:relation <uuid> :entity <uuid>}]}
   - :hybrid   {:type :hybrid :steps [{:relation <uuid> :entity <uuid>}] :attribute <uuid>}"
  [path]
  (case (:type path)
    :ref
    {:type :ref
     :attribute (:attribute-euuid path)}

    :relation
    {:type :relation
     :steps (mapv #(select-keys % [:relation-euuid :entity-euuid])
                  (:steps path))}

    :hybrid
    {:type :hybrid
     :steps (mapv #(select-keys % [:relation-euuid :entity-euuid])
                  (:steps path))
     :attribute (:attribute-euuid path)}))


(defn condition-matches-path?
  "Check if a stored condition matches a discovered path by comparing UUIDs.
   This is stable across model changes that don't affect the actual path structure."
  [condition path]
  (case (:type condition)
    :ref
    (= (:attribute condition) (:attribute-euuid path))

    :relation
    (let [condition-steps (mapv :relation-euuid (:steps condition))
          path-steps (mapv :relation-euuid (:steps path))]
      (= condition-steps path-steps))

    :hybrid
    (and (= (:attribute condition) (:attribute-euuid path))
         (let [condition-steps (mapv :relation-euuid (:steps condition))
               path-steps (mapv :relation-euuid (:steps path))]
           (= condition-steps path-steps)))

    ;; Legacy: fallback to path-id for old configs
    (= (:path-id condition) (:id path))))


(defn toggle-rls-condition
  "Toggle a path condition on a guard. If guard doesn't exist, creates a new one.
   Auto-removes guard if all conditions are removed.
   Matches conditions by structure (UUIDs), not ephemeral path-id."
  [entity guard-id path]
  (let [guards (get-rls-guards entity)
        guard-idx (find-guard-index guards guard-id)]
    (if guard-idx
      ;; Toggle condition on existing guard
      (let [conditions (get-in guards [guard-idx :conditions] [])
            matching-condition (some #(when (condition-matches-path? % path) %) conditions)
            new-conditions (if matching-condition
                             (vec (remove #(condition-matches-path? % path) conditions))
                             (conj conditions (path->condition path)))]
        ;; Auto-remove guard if no conditions left
        (if (empty? new-conditions)
          (remove-rls-guard entity guard-id)
          (assoc-in entity [:configuration :rls :guards guard-idx :conditions] new-conditions)))
      ;; Guard not found - create new guard with this condition
      (let [new-guard {:id (generate-uuid)
                       :operation #{}
                       :conditions [(path->condition path)]}]
        (add-rls-guard entity new-guard)))))
