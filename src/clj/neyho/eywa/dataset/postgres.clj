(ns neyho.eywa.dataset.postgres
  (:require
    clojure.data
    [clojure.java.io :as io]
    clojure.set
    clojure.string
    [clojure.tools.logging :as log]
    [next.jdbc :as jdbc]
   ; [clojure.pprint :refer [pprint]]
    next.jdbc.date-time
    [neyho.eywa.data :refer [*EYWA*]]
    [neyho.eywa.dataset :as dataset]
    [neyho.eywa.dataset.core :as core]
    [neyho.eywa.dataset.lacinia
     :refer [normalized-enum-value]]
    [neyho.eywa.dataset.postgres.query :as query]
    [neyho.eywa.dataset.sql.naming
     :refer [normalize-name
             column-name
             relation->table-name
             entity->relation-field
             entity->table-name]]
    [neyho.eywa.dataset.uuids :as du]
    [neyho.eywa.db
     :refer [*db*
             sync-entity
             delete-entity]]
    [neyho.eywa.db.postgres :as postgres]
    [neyho.eywa.db.postgres.next :as n
     :refer [execute! execute-one!]]
    [neyho.eywa.iam :as iam]
    [neyho.eywa.iam.access.context :refer [*user*]]
    [neyho.eywa.iam.util
     :refer [import-role
             import-api
             import-app]]
    [neyho.eywa.iam.uuids :as iu]
    [neyho.eywa.lacinia :as lacinia]
    [neyho.eywa.transit
     :refer [<-transit ->transit]]))

;; TODO - remove this... probably not necessary
(defonce ^:dynamic *model* nil)

(defn user-table []
  (if-some [e (core/get-entity *model* iu/user)]
    (entity->table-name e)
    (throw (Exception. "Coulnd't find user entity"))))

(defn group-table []
  (if-some [e (core/get-entity *model* iu/user-group)]
    (entity->table-name e)
    (throw (Exception. "Coulnd't find group entity"))))

(defn role-table []
  (if-some [e (core/get-entity *model* iu/user-role)]
    (entity->table-name e)
    (throw (Exception. "Coulnd't find role entity"))))

;;; Type Conversion Validation System

(def type-families
  "Groups of types that share the same underlying PostgreSQL storage type"
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
   - {:error \"message\" :type ::error-type :suggestion \"hint\"} if conversion is forbidden"
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
    {:warning (format "Converting %s to %s assumes all entity IDs exist in the target table. Invalid references will violate foreign key constraints." from-type to-type)}

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
    {:warning (format "Converting string to %s requires all values to be valid numbers. Invalid values will cause the conversion to fail." to-type)}

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
    {:error (format "Cannot convert %s to %s: No meaningful automatic conversion exists." from-type to-type)
     :type ::forbidden-conversion
     :suggestion "Extract numeric fields from JSON manually before converting."}

    ;; FORBIDDEN: json → boolean
    (and (contains? (:json type-families) from-type)
         (= to-type "boolean"))
    {:error (format "Cannot convert %s to boolean: No meaningful automatic conversion exists." from-type)
     :type ::forbidden-conversion
     :suggestion "Extract boolean fields from JSON manually before converting."}

    ;; FORBIDDEN: timestamp → int (semantic mismatch)
    (and (= from-type "timestamp") (= to-type "int"))
    {:error "Cannot convert timestamp to int: Use explicit epoch conversion if needed."
     :type ::forbidden-conversion
     :suggestion "Create a new attribute and populate it with epoch timestamps explicitly."}

    ;; FORBIDDEN: boolean → numeric
    (and (= from-type "boolean") (contains? #{"int" "float"} to-type))
    {:error (format "Cannot convert boolean to %s: Semantic mismatch." to-type)
     :type ::forbidden-conversion
     :suggestion "Convert to string first if you need '0'/'1' representation, or create explicit mapping logic."}

    ;; FORBIDDEN: reference → non-reference (losing referential integrity)
    (and (contains? (:reference type-families) from-type)
         (not (contains? (:reference type-families) to-type))
         (not= to-type "string"))
    {:error (format "Cannot convert %s to %s: This would lose referential integrity." from-type to-type)
     :type ::forbidden-conversion
     :suggestion "Convert to string first if you need to preserve entity IDs."}

    ;; Default: Unknown/unsupported conversion
    :else
    {:error (format "Unsupported type conversion from %s to %s." from-type to-type)
     :type ::unsupported-conversion
     :suggestion "This conversion path has not been validated. Please review the type compatibility matrix."}))

(defn check-type-conversion!
  "Validates type conversion and throws exception if forbidden, logs warning if risky.
   Returns true if conversion can proceed."
  [entity attribute old-type new-type]
  (let [validation (validate-type-conversion old-type new-type)]
    (cond
      (:safe validation)
      true

      (:warning validation)
      (do
        (log/warnf "Type conversion warning for %s.%s (%s → %s): %s"
                   (:name entity)
                   (:name attribute)
                   old-type
                   new-type
                   (:warning validation))
        true)

      (:error validation)
      (throw
        (ex-info
          (format "Forbidden type conversion for %s.%s: %s → %s\n%s"
                  (:name entity)
                  (:name attribute)
                  old-type
                  new-type
                  (:error validation))
          {:type (or (:type validation) ::forbidden-conversion)
           :entity (:name entity)
           :attribute (:name attribute)
           :from-type old-type
           :to-type new-type
           :suggestion (:suggestion validation)}))

      :else
      (throw
        (ex-info
          (format "Unknown validation result for %s.%s: %s → %s"
                  (:name entity)
                  (:name attribute)
                  old-type
                  new-type)
          {:validation validation})))))

;;; End Type Conversion Validation System

(defn type->ddl
  "Converts type to DDL syntax"
  [t]
  (case t
    "currency" "jsonb"
    ("avatar" "string" "hashed") "text"
    "timestamp" "timestamp"
    ("json" "encrypted" "timeperiod") "jsonb"
    "transit" "text"
    "user" (str "bigint references \"" (user-table) "\"(_eid) on delete set null")
    "group" (str "bigint references \"" (group-table) "\"(_eid) on delete set null")
    "role" (str "bigint references \"" (role-table) "\"(_eid) on delete set null")
    "int" "bigint"
    t))

(defn attribute->ddl
  "Function converts attribute to DDL syntax"
  [entity {n :name
           t :type}]
  (case t
    "enum"
    (let [table (entity->table-name entity)
          enum-name (normalize-name (str table \space n))]
      (str (column-name n) \space enum-name))
    ;;
    (clojure.string/join
      " "
      (remove
        empty?
        [(column-name n)
         (type->ddl t)]))))

; (defn generate-enum-type-ddl [enum-name values]
;   (format
;     "do $$\nbegin\nif not exists ( select 1 from pg_type where typname='%s') then create type \"%s\" as enum%s;\nend if;\nend\n$$;"
;     enum-name
;     enum-name 
;     (when (not-empty values)
;       (str " (" 
;            (clojure.string/join 
;              ", "
;              (map (comp #(str \' % \') normalized-enum-value :name) values))
;            \)))))

(defn generate-enum-type-ddl [enum-name values]
  (format
    "create type \"%s\" as enum%s;"
    enum-name
    (when (not-empty values)
      (str " ("
           (clojure.string/join
             ", "
             (map (comp #(str \' % \') normalized-enum-value :name) values))
           \)))))

(defn generate-entity-attribute-enum-ddl
  [table {n :name
          t :type
          {values :values} :configuration}]
  (let [enum-name (normalize-name (str table \space n))]
    (when (= t "enum")
      (generate-enum-type-ddl enum-name values))))

(defn generate-entity-enums-ddl
  [{as :attributes
    :as entity}]
  (let [table (entity->table-name entity)]
    (clojure.string/join
      ";"
      (keep (partial generate-entity-attribute-enum-ddl table) as))))

(defn drop-entity-enums-ddl
  [{as :attributes
    :as entity}]
  (let [table (entity->table-name entity)]
    (clojure.string/join
      ";"
      (keep
        (fn [{n :name
              t :type}]
          (when (= t "enum")
            (str "drop type if exists " (normalize-name (str table \space n)))))
        as))))

(defn generate-entity-ddl
  "For given model and entity returns entity table DDL"
  [{n :name
    as :attributes
    {cs :constraints} :configuration
    :as entity}]
  (let [table (entity->table-name entity)
        as' (keep #(attribute->ddl entity %) as)
        pk ["_eid bigserial not null primary key"
            "euuid uuid not null unique default uuid_generate_v1()"]
        cs' (keep-indexed
              (fn [idx ids]
                (when (not-empty ids)
                  (format
                    "constraint \"%s\" unique(%s)"
                    (str table "_eucg_" idx)
                    (clojure.string/join
                      ","
                      (map
                        (fn [id]
                          (log/infof "Generating constraint %s %s" n id)
                          (->
                            (core/get-attribute entity id)
                            :name
                            column-name))
                        ids)))))
              (:unique cs))
        rows (concat pk as' cs')]
    (format
      "create table \"%s\" (\n  %s\n)"
      table
      (clojure.string/join ",\n  " rows))))

(defn generate-relation-ddl
  "Returns relation table DDL for given model and target relation"
  [_ {f :from
      t :to
      :as relation}]
  (let [table (relation->table-name relation)
        from-table (entity->table-name f)
        to-table (entity->table-name t)
        from-field (entity->relation-field f)
        to-field (entity->relation-field t)]
    (comment
      (entity->relation-field (:from relation))
      (core/cloned? (:to relation))
      (core/cloned? (:from relation))
      (entity->relation-field (:to relation)))
    (format
      "create table %s(\n %s\n)"
      table
      ;; Create table
      (if (= f t)
        (clojure.string/join
          ",\n "
          [(str to-field " bigint not null references \"" to-table "\"(_eid) on delete cascade")
           (str "unique(" to-field ")")])
        (clojure.string/join
          ",\n "
          [(str from-field " bigint not null references \"" from-table "\"(_eid) on delete cascade")
           (str to-field " bigint not null references \"" to-table "\"(_eid) on delete cascade")
           (str "unique(" from-field "," to-field ")")])))))

(defn generate-relation-indexes-ddl
  "Returns relation table DDL for given model and target relation"
  [_ {f :from
      t :to
      :as relation}]
  (let [table (relation->table-name relation)
        from-field (entity->relation-field f)
        to-field (entity->relation-field t)]
    [(format "create index %s_fidx on \"%s\" (%s);" table table from-field)
     (format "create index %s_tidx on \"%s\" (%s);" table table to-field)]))

;; Deployment syncronization
(defn stale-attribute? [attribute]
  (let [{:keys [diff removed?]} (core/projection-data attribute)]
    (or (:active diff) removed?)))

(defn deactivate-stale-attributes!
  [con {:keys [id attributes]
        :as entity}]
  (let [as (mapv :id (filter stale-attribute? attributes))]
    (jdbc/execute-one!
      con
      ["update modeling_eywa_datasets_entities_attributes set active = false where entity=? and id=ANY(?)" id (long-array as)])
    entity))

(defn analyze-projection
  [projection]
  (let [new-entities
        (filter
          core/added?
          (core/get-entities projection))
        ;;
        ;; NEW: Detect reactivated entities (exist but were inactive)
        reactivated-entities
        (filter
          (fn [entity]
            (and (not (core/added? entity))
                 (false? (:active (core/suppress entity))) ; Was inactive
                 (true? (:active entity)))) ; Now active
          (core/get-entities projection))
        ;;
        changed-enties
        (filter
          core/diff?
          (core/get-entities projection))
        ;;
        new-relations
        (filter
          (fn [relation]
            (core/added? relation))
          (core/get-relations projection))
        ;;
        changed-relations
        (filter
          (fn [relation]
            (core/diff? relation))
          (core/get-relations projection))
        ;;
        {nrr true
         nr false} (group-by core/recursive-relation? new-relations)
        ;;
        {crr true
         cr false} (group-by core/recursive-relation? changed-relations)]
    {:new/entities new-entities
     :reactivated/entities reactivated-entities ; NEW
     :changed/entities changed-enties
     :new/relations nr
     :new/recursive-relations nrr
     :changed/relations cr
     :changed/recursive-relations crr}))

;;
(defn attribute-delta->ddl
  [entity
   {:keys [name type]
    {:keys [values]} :configuration
    :as attribute}]
  ;; Don't look at primary key since that is EYWA problem
  (let [new-table (entity->table-name entity)
        oentity (core/suppress entity)
        old-table (entity->table-name oentity)
        diff (core/diff attribute)]
    (if (core/new-attribute? attribute)
      ;;
      (do
        (log/debugf "Adding attribute %s to table %s" name old-table)
        (case type
          "enum"
          ;; Create new enum type and add it to table
          [(generate-entity-attribute-enum-ddl new-table attribute)
           (format "alter table \"%s\" add column if not exists %s %s"
                   old-table (column-name name)
                   (normalize-name (str new-table \space name)))]
          ;; Add new scalar column to table
          [(str "alter table \"" old-table "\" add column if not exists " (column-name name) " " (type->ddl type))]
          ;; TODO - remove mandatory
          #_[(cond-> (str "alter table \"" old-table "\" add column if not exists " (column-name name) " " (type->ddl type))
               (= "mandatory" constraint) (str " not null"))]))
      ;;
      (when (or
              (:name (core/diff entity)) ;; If entity name has changed check if there are some enums
              (not-empty (dissoc diff :pk))) ;; If any other change happend follow steps
        (let [{dn :name
               dt :type
               dconfig :configuration} diff
              column (column-name (or dn name))
              old-enum-name (normalize-name (str old-table \space (or dn name)))
              new-enum-name (normalize-name (str new-table \space name))]
          (when dt (check-type-conversion! entity attribute dt type))
          (cond-> []
            ;; Change attribute name
            (and dn (not= (column-name dn) (column-name name)))
            (conj
              (do
                (log/debugf "Renaming table %s column %s -> %s" old-table column (column-name name))
                (format
                  "alter table \"%s\" rename column %s to %s"
                  old-table column (column-name name))))
            ;; If attribute type has changed to enum
            ;; than create new enum type with defined values
            (and (= type "enum") (some? dt))
            (conj (generate-enum-type-ddl new-enum-name values))
            ;; If type is one of
            ;; Set all current values to null
            ;; NOTE: This is now caught by validation and should throw an error
            (and dt
                 (#{"avatar"} dt)
                 (#{"json"} type))
            (conj
              (do
                (log/debugf "Setting all avatar values to NULL")
                (format "update \"%s\" set %s = NULL" old-table column)))
            ;; Type has changed so you better cast to target type
            dt
            (as-> statements
                  (log/debugf "Changing table %s column %s type %s -> %s" old-table column dt type)
              (if (#{"user" "group" "role"} type)
                (let [attribute-name (normalize-name (or dn name))
                      constraint-name (str old-table \_ attribute-name "_fkey")
                      refered-table (case type
                                      "user" (user-table)
                                      "group" (group-table)
                                      "role" (role-table))]
                  (conj
                    (vec statements)
                    (format "alter table \"%s\" drop constraint %s" old-table constraint-name)
                    (format
                      "alter table \"%s\" add constraint \"%s\" foreign key (%s) references \"%s\"(_eid) on delete set null"
                      old-table constraint-name attribute-name refered-table)))
                ;; Proceed with acceptable type alter
                (conj
                  statements
                  (cond->
                    (format
                      "alter table \"%s\" alter column %s type %s"
                      old-table column
                      (case type
                        "enum" new-enum-name
                        (type->ddl type)))
                      (= "int" type) (str " using(trim(" column ")::integer)")
                      (= "float" type) (str " using(trim(" column ")::float)")
                      (= "string" type) (str " using(" column "::text)")
                   ; (= "json" type) (str " using(" column "::jsonb)")
                      (= "json" type) (str " using\n case when "
                                           column " ~ '^\\s*\\{.*\\}\\s*$' OR "
                                           column " ~ '^\\s*\\[.*\\]\\s*$'\nthen " column
                                           "::jsonb\nelse NULL end;")
                      (= "transit" type) (str " using(" column "::text)")
                      (= "avatar" type) (str " using(" column "::text)")
                      (= "encrypted" type) (str " using(" column "::text)")
                      (= "hashed" type) (str " using(" column "::text)")
                      (= "boolean" type) (str " using(trim(" column ")::boolean)")
                      (= "enum" type) (str " using(" column ")::" old-enum-name)))))
            ;; If attribute was previously enum and now it is not enum
            ;; than delete enum type
            (= dt "enum")
            (conj (format "drop type \"%s\"" old-enum-name))
            ;; If enum name has changed than apply changes
            (and (= type "enum") (not= old-enum-name new-enum-name))
            (conj (format "alter type %s rename to %s" old-enum-name new-enum-name))
            ;; If type is enum and it hasn't changed
            ;; but enum configuration has changed, than create statements to
            ;; compensate changes  
            (and (= type "enum") (nil? dt))
            (as-> statements
                  (let [[ov nv] (clojure.data/diff
                                  (reduce
                                    (fn [r [idx {n :name
                                                 e :euuid}]]
                                      (if (not-empty n)
                                        (assoc r (or e (get-in values [idx :euuid])) n)
                                        r))
                                    nil
                                    (map-indexed (fn [idx v] (vector idx v)) (:values dconfig)))
                                  (zipmap
                                    (map :euuid values)
                                    (map :name values)))
                        column (column-name name)]
                ; alter type my_enum rename to my_enum__;
                ; -- create the new enum
                ; create type my_enum as enum ('value1', 'value2', 'value3');

                ; -- alter all you enum columns
                ; alter table my_table
                ; alter column my_column type my_enum using my_column::text::my_enum;

                ; -- drop the old enum
                ; drop type my_enum__;
                    (log/tracef "Diff config\n%s" dconfig)
                    (log/tracef "Old enums: %s" ov)
                    (log/tracef "New enums: %s" nv)
                    (conj
                      statements
                      (format "alter type %s rename to %s__" new-enum-name new-enum-name)
                      (format "create type %s as enum (%s)" new-enum-name (clojure.string/join "," (map #(str \' % \') (remove empty? (concat (map :name values) (vals ov))))))
                  ;; UPDATE your_table SET new_column =
                  ;;    CASE your_column
                  ;;        WHEN 'active' THEN 'active'
                  ;;        WHEN 'inactive' THEN 'suspended'
                  ;;        WHEN 'pending' THEN 'pending'
                  ;;        ELSE your_column
                  ;;    END;
                      (if (empty? ov) ""
                          (str
                            "update " old-table " set " column " ="
                            "\n  case " column "\n    "
                            (clojure.string/join
                              "\n    "
                              (reduce-kv
                                (fn [r euuid old-name]
                                  (if-let [new-name (get nv euuid)]
                                    (conj r (str "     when '" old-name "' then '" new-name "'"))
                                    (conj r (str "     when '" old-name "' then '" old-name "'"))))
                                (list (str " else " column))
                                ov))
                            "\n   end;"))

                      (format "alter table \"%s\" alter column %s type %s using %s::text::%s"
                              old-table column new-enum-name column new-enum-name)
                      (format "drop type %s__" new-enum-name))))))))))

(defn orphaned-attribute->drop-ddl
  "Generates DROP COLUMN DDL for an orphaned attribute.
   An orphaned attribute exists only in a recalled/destroyed version,
   not in any remaining deployed versions.

   Returns a vector of DDL statements to execute."
  [{:keys [entity attribute]}]
  (let [table-name (entity->table-name entity)
        column-name (column-name (:name attribute))
        attr-type (:type attribute)]
    (cond-> []
      ;; Always drop the column
      true
      (conj (format "ALTER TABLE \"%s\" DROP COLUMN IF EXISTS %s"
                    table-name column-name))

      ;; If enum type, also drop the enum type definition
      (= "enum" attr-type)
      (conj (format "DROP TYPE IF EXISTS \"%s\" CASCADE"
                    (normalize-name (str table-name " " (:name attribute))))))))

;; 1. Change attributes by calling attribute-delta->ddl
;; 2. Rename table if needed
;; 3. Change constraints
(defn entity-delta->ddl
  [{:keys [attributes]
    :as entity}]
  (assert (core/diff? entity) "This entity is already synced with DB")
  (let [diff (core/diff entity)
        old-entity (core/suppress entity)
        old-table (entity->table-name old-entity)
        old-constraints (get-in old-entity [:configuration :constraints :unique])
        table (entity->table-name entity)
        attributes' (keep #(attribute-delta->ddl entity %) attributes)]
    (cond-> (reduce into [] attributes')
      ;; Renaming occured
      (:name diff)
      (into
        (cond->
          [(format "alter table \"%s\" rename to \"%s\"" old-table table)
           (format "alter table \"%s\" rename constraint \"%s_pkey\" to \"%s_pkey\"" table old-table table)
           (format "alter table \"%s\" rename constraint \"%s_euuid_key\" to \"%s_euuid_key\"" table old-table table)
           (format "alter sequence \"%s__eid_seq\" rename to \"%s__eid_seq\"" old-table table)
           ;; AUDIT
           (format "alter table \"%s\" rename constraint \"%s_%s_fkey\" to \"%s_%s_fkey\"" table old-table "modified_by" table "modified_by")
          ;;
           (some? old-constraints)
           (into
             (map-indexed
               (fn [idx _]
                 (let [constraint (str "_eucg_" idx)]
                   (format
                     "alter table \"%s\" rename constraint \"%s%s\" to \"%s%s\""
                     table old-table constraint
                     table constraint)))
               old-constraints))]))
      ;; If there are some differences in constraints
      (-> diff :configuration :constraints)
      (into
        ;; concatenate constraints
        (let [ncs (-> entity :configuration :constraints :unique)
              ocs old-constraints
              groups (max (count ocs) (count ncs))]
          ;; by reducing
          (when (pos? groups)
            (reduce
              (fn [statements idx]
                (let [o (try (nth ocs idx) (catch Throwable _ nil))
                      n (try (nth ncs idx) (catch Throwable _ nil))
                      constraint (str "_eucg_" idx)
                      new-constraint (format
                                       "alter table \"%s\" add constraint %s unique(%s)"
                                       table (str table constraint)
                                       (clojure.string/join
                                         ","
                                         (map
                                           (fn [id]
                                             (->
                                               (core/get-attribute entity id)
                                               :name
                                               column-name))
                                           n)))
                      drop-constraint (format
                                        "alter table \"%s\" drop constraint %s"
                                        table
                                        (str table constraint))]
                  (cond->
                    statements
                    ;; Add new constraint
                    (empty? o)
                    (conj new-constraint)
                    ;; Delete old constraint group
                    (empty? n)
                    (conj drop-constraint)
                    ;; When constraint has changed
                    (and
                      (every? not-empty [o n])
                      (not= (set o) (set n)))
                    (conj
                      drop-constraint
                      new-constraint))))
              []
              (range groups))))))))

(defn transform-relation
  [tx {:keys [from to]
       :as relation}]
  (let [diff (core/diff relation)
        _ (log/tracef "Transforming relation\ndiff=%s\nfrom=%s\nto=%s" diff (pr-str from) (pr-str to))
        from-diff (:from diff)
        to-diff (:to diff)
        old-from (core/suppress from)
        old-to (core/suppress to)
        old-relation (core/suppress relation)
        old-name (relation->table-name old-relation)
        ;; Assoc old from and to entities
        ;; This will be handled latter
        new-name (relation->table-name relation)]

    ;; When name has changed
    (when (not= old-name new-name)
      (let [sql (format
                  "alter table %s rename to %s"
                  old-name new-name)]
        (log/debugf "Renaming relation table %s->%s\n%s" old-name new-name sql)
        (execute-one! tx [sql])))
    ;; when to name has changed than change table column
    (when (:name to-diff)
      (let [o (entity->relation-field old-to)
            n (entity->relation-field to)
            sql (format
                  "alter table %s rename column %s to %s"
                  new-name o n)]
        (log/debugf "Renaming relation table %s -> %s\n%s" old-name new-name sql)
        (execute-one! tx [sql])))
    ;; when from name has changed than change table column
    (when (:name from-diff)
      (let [o (entity->relation-field old-from)
            n (entity->relation-field from)
            sql (format
                  "alter table %s rename column %s to %s"
                  new-name o n)]
        (log/debugf "Renaming relation %s -> %s\n%s" old-name new-name sql)
        (execute-one! tx [sql])))))

(defn column-exists?
  [tx table column]
  (let [sql (str
              "select exists ("
              "select 1 from pg_attribute where attrelid = '" table "'::regclass"
              " and attname = '" column "'"
              " and attnum > 0"
              " and not attisdropped"
              ");")]
    (:exists (execute-one! tx [sql]))))

(comment
  (do (def tx nil) (def table "ict_category") (def column "parent"))
  (with-open [con (jdbc/get-connection (:datasource *db*))]
    (column-exists? con "ict_category" "parent")))

;; 1. Generate new entities by creating tables
;;  - Create new types if needed by enum attributes
;; 2. Add audit attributes if present (modified_by,modified_on)
;; 3. Check if model has changed attributes
;;  - If so try to resolve changes by calling entity-delta->ddl
;; 4. Check if model has changed relations
;;  - If so try to resolve changes by calling transform-relation
;; 5. Generate new relations by connecting entities
(defn transform-database [ds projection configuration]
  (log/debugf "Transforming database\n%s" configuration)
  (let [{ne :new/entities
         re :reactivated/entities ; NEW
         nr :new/relations
         nrr :new/recursive-relations
         ce :changed/entities
         cr :changed/relations
         crr :changed/recursive-relations} (analyze-projection projection)
        {amt :who/table} configuration]
    (log/tracef
      "Transform projection analysis\nNew\n%s\nReactivated\n%s\nChanged\n%s"
      {:new/entities (map :name ne)
       :new/relations (map (juxt :from-label :to-label) nr)
       :new/recursive (map (juxt :from-label :to-label) nrr)}
      {:reactivated/entities (map :name re)} ; NEW
      {:changed/entities (map :name ce)
       :changed/relations (map (juxt :from-label :to-label) cr)
       :changed/recursive (map (juxt :from-label :to-label) crr)})
    (jdbc/with-transaction [tx ds]
      ;; Handle reactivated entities (table exists, just mark active)
      (when (not-empty re)
        (log/infof "Reactivating %d entities (tables already exist)" (count re))
        (doseq [{:keys [name]} re]
          (log/debugf "Reactivated entity: %s" name)))
      ;; Generate new entities
      (let [entity-priority {iu/user -100}
            ne (sort-by
                 (fn [{:keys [euuid]}]
                   (get entity-priority euuid 0))
                 ne)]
        (when (not-empty ne)
          (log/infof
            "Generating new entities... %s"
            (clojure.string/join ", " (map :name ne))))
        (doseq [{n :name
                 :as entity} ne
                :let [table-sql (generate-entity-ddl entity)
                      enum-sql (generate-entity-enums-ddl entity)
                      table (entity->table-name entity)]]
          (when (not-empty enum-sql)
            (log/debugf "Adding entity %s enums\n%s" n enum-sql)
            (execute! tx [enum-sql]))
          (log/debugf "Adding entity %s to DB\n%s" n table-sql)
          (execute-one! tx [table-sql])
          (let [sql (format
                      "alter table \"%s\" add column \"%s\" bigint references \"%s\"(_eid) on delete set null"
                      table "modified_by" amt)]
            (log/tracef "Adding table audit reference[who] column\n%s" sql)
            (execute-one! tx [sql]))
          (let [sql (format
                      "alter table \"%s\" add column \"%s\" timestamp not null default localtimestamp"
                      table "modified_on")]
            (log/tracef "Adding table audit reference[when] column\n%s" sql)
            (execute-one! tx [sql]))))
      ;; Change entities
      (when (not-empty ce) (log/info "Checking changed entities..."))
      (doseq [{n :name
               :as entity} ce
              :let [sql (entity-delta->ddl entity)]]
        (log/debugf "Changing entity %s" n)
        (doseq [statement sql]
          (log/debugf "Executing statement %s\n%s" n statement)
          (execute-one! tx [statement])))
      ;; Change relations
      (when (not-empty cr)
        (log/info "Checking changed trans entity relations..."))
      (doseq [r cr] (transform-relation tx r))
      ;; Change recursive relation
      (when (not-empty crr)
        (log/info "Checking changed recursive relations..."))
      (doseq [{{tname :name
                :as e} :to
               tl :to-label
               diff :diff
               euuid :euuid} crr
              :let [table (entity->table-name e)
                    ; _ (log/debugf "RECURSIVE RELATION\n%s" diff)
                    previous-column (when-some [label (not-empty (:to-label diff))]
                                      (column-name label))]]
        (when-not (and (some? tl) (not-empty tl))
          (throw
            (ex-info
              (str "Can't change recursive relation for entity " tname " that has empty label")
              {:entity e
               :relation {:euuid euuid
                          :label (:to-label diff)}
               :type ::core/error-recursive-no-label})))
        (if (empty? previous-column)
          (do
            (log/debugf
              "Previous deploy didn't have to label for recursive relation %s at entity %s"
              euuid tname)
            (when tl
              (when-not (column-exists? tx table tl)
                (let [sql (format
                            "alter table %s add %s bigint references \"%s\"(_eid) on delete cascade"
                            table tl table)]
                  (log/debug "Creating recursive relation for entity %s\n%s" tname sql)
                  (execute-one! tx [sql])))))
          ;; Apply changes
          (when diff
            (let [sql (format
                        "alter table %s rename column %s to %s"
                        table previous-column (column-name tl))]
              (log/debugf "Updating recursive relation for entity %s\n%s" tname sql)
              (execute-one! tx [sql])))))
      ;; Generate new relations
      (when (not-empty nr) (log/info "Generating new relations..."))
      (doseq [{{tname :name} :to
               {fname :name} :from
               :as relation} nr
              :let [sql (generate-relation-ddl projection relation)
                    [from-idx to-idx] (generate-relation-indexes-ddl projection relation)]]
        (log/debugf "Connecting entities %s <-> %s\n%s" fname tname sql)
        (execute-one! tx [sql])
        (when from-idx
          (log/debugf "Creating from indexes for relation: %s <-> %s\n%s" fname tname from-idx)
          (execute-one! tx [from-idx]))
        (when from-idx
          (log/debugf "Creating to indexes for relation: %s <-> %s\n%s" fname tname from-idx)
          (execute-one! tx [to-idx])))
      ;; Add new recursive relations
      (when (not-empty nrr)
        (log/info "Adding new recursive relations..."))
      (doseq [{{tname :name
                :as e} :to
               tl :to-label} nrr
              :when (not-empty tl)
              :let [table (entity->table-name e)
                    sql (format
                          "alter table %s add %s bigint references \"%s\"(_eid) on delete cascade"
                          table (column-name tl) table)]]
        (log/debug "Creating recursive relation for entity %s\n%s" tname sql)
        (execute-one! tx [sql])))))

(defn model->schema [model]
  (binding [*model* model]
    (reduce
      (fn [schema {:keys [euuid]
                   ename :name
                   :as entity}]
        (log/tracef "Building schema for entity %s[%s]" ename euuid)
        (let [table (entity->table-name entity)
              fields (reduce
                       (fn [fields {euuid :euuid
                                    aname :name
                                    t :type
                                    constraint :constraint}]
                         (log/tracef "Adding field %s in entity %s to schema" aname ename)
                         (let [f {:key (keyword (normalize-name aname))
                                  :euuid euuid
                                  :type t
                                  :constraint constraint}]
                           (assoc fields euuid
                                  (case t
                                    "enum"
                                    (assoc f :postgres/type (normalize-name (str table \space aname)))
                                    ;;
                                    "user"
                                    (assoc f :postgres/reference iu/user)
                                    ;;
                                    "group"
                                    (assoc f :postgres/reference iu/user-group)
                                    ;;
                                    "role"
                                    (assoc f :postgres/reference iu/user-role)
                                    ;;
                                    f))))
                       {:audit/who {:key :modified_by
                                    :type "user"
                                    :postgres/reference iu/user}}
                       (:attributes entity))
              {relations :relations
               recursions :recursions}
              (group-by
                (fn [{t :cardinality}]
                  (case t
                    "tree" :recursions
                    :relations))
                (core/focus-entity-relations model entity))
              ;;
              relations (reduce
                          (fn [relations
                               {:keys [euuid from to to-label cardinality]
                                :as relation}]
                          ;; ENTITY MIGHT BE CLONE... So get original entity for
                          ;; schema
                            (if (and
                                  (some? from) (some? to)
                                  (not-empty to-label))
                              (do
                                (log/tracef
                                  "Building schema for relation\n%s"
                                  {:euuid euuid
                                   :from (:name from)
                                   :to (:name to)
                                   :cardinality cardinality})
                                (assoc relations (keyword (normalize-name to-label))
                                       {:relation euuid
                                        :from (:euuid from)
                                        :from/field (entity->relation-field from)
                                        :from/table (entity->table-name from)
                                        :to (if (contains? (:clones model) (:euuid to))
                                              (get-in model [:clones (:euuid to) :entity])
                                              (:euuid to))
                                        :to/field (entity->relation-field to)
                                        :to/table (entity->table-name to)
                                        :table (relation->table-name relation)
                                        :type (case cardinality
                                                ("m2o" "o2o") :one
                                                ("m2m" "o2m") :many)}))
                              relations))
                          {}
                          relations)
              recursions (set (map (comp keyword normalize-name :to-label) recursions))
              mandatory-attributes (keep
                                     (fn [{:keys [constraint name]}]
                                       (when (#{"mandatory" "unique+mandatory"} constraint)
                                         (keyword (normalize-name name))))
                                     (:attributes entity))
              entity-schema (cond->
                              {:table table
                               :name (:name entity)
                               :constraints (cond->
                                              (get-in entity [:configuration :constraints])
                                              ;;
                                              (not-empty mandatory-attributes)
                                              (assoc :mandatory mandatory-attributes))
                               :fields fields
                               :field->attribute (reduce-kv
                                                   (fn [r a {field :key}]
                                                     (assoc r field a))
                                                   nil
                                                   fields)
                               :recursions recursions
                               :relations relations
                               :audit/who :modified_by
                               :audit/when :modified_on})]
          (assoc schema euuid entity-schema)))
      {}
      (core/get-entities model))))

(defn- get-dataset-versions
  "Gets all versions for a dataset by its :euuid or :name, ordered by modified_on desc"
  [{:keys [euuid name]}]
  (:versions
    (dataset/get-entity
      du/dataset
      (if euuid
        {:euuid euuid}
        {:name name})
      {:name nil
       :euuid nil
       :versions
       [{:selections
         {:euuid nil
          :name nil
          :model nil
          :deployed nil
          :modified_on nil}
         :args {:_order_by {:modified_on :desc}}}]})))

(defn rebuild-global-model-from-versions
  "Utility function to rebuild the global model from ALL deployed dataset versions.
   Use this as a backup/recovery mechanism if __deploy_history is corrupted.

   Algorithm:
   1. Get ALL deployed versions (not just latest) in ascending order
   2. Build global model with claims via reduce + join-models
   3. Determine which entities/relations are in LATEST version per dataset
   4. Compute :active flags based on step 3
   5. Return global model (does NOT save to __deploy_history)"
  []
  (let [;; Get ALL deployed versions from all datasets
        datasets (dataset/search-entity
                   du/dataset
                   nil
                   {:versions
                    [{:selections {:euuid nil
                                   :model nil
                                   :modified_on nil
                                   :dataset [{:selections {:euuid nil}}]}
                      :args {:_where {:deployed {:_boolean :TRUE}}}}]})

        ;; Extract and sort all versions by modification time (ascending)
        all-versions (sort-by
                       :modified_on
                       (mapcat :versions datasets))

        ;; Build global model with ALL claims
        ;; join-models handles active flags via "last deployment wins"
        initial-model (core/map->ERDModel {:entities {}
                                           :relations {}})
        final-model
        (reduce
          (fn [global {:keys [euuid model]}]
            (core/join-models global (core/add-claims model euuid)))
          initial-model
          all-versions)]

    final-model))

(defn last-deployed-model
  "Returns the global model with ALL entities from ALL deployed versions.
   Entities have :claimed-by sets and :active flags properly computed.

   This rebuilds from ALL deployed versions to ensure historical entities
   are included. Use this as the standard way to get the current global state."
  []
  (rebuild-global-model-from-versions))

;; Helper functions for claims-based deployment
(defn check-entity-name-conflicts!
  "Throws exception if new-model contains entities with conflicting names"
  [global new-model]
  (let [entities-by-table (reduce
                            (fn [r entity]
                              (assoc r (entity->table-name entity) entity))
                            {}
                            (core/get-entities global))]
    (doseq [new-entity (core/get-entities new-model)
            :let [table (entity->table-name new-entity)
                  found-entity (get entities-by-table table)]]
      (when (and
              (some? found-entity)
              (not= (:euuid found-entity) (:euuid new-entity)))
        (throw
          (ex-info
            (format "Entity name conflict: '%s' already exists with different UUID" (:name new-entity))
            {:type ::entity-name-conflict
             :new-entity new-entity
             :existing-entity (get entities-by-table table)
             :table-name (entity->table-name new-entity)}))))))

(extend-type neyho.eywa.Postgres
  core/DatasetProtocol
  (core/deploy! [this {:keys [model]
                       :as version}]
    (try
      (let [;; Get current global model WITH CLAIMS (or empty if first deployment)
            ;; MUST use last-deployed-model (not fallback) because we need claims
            global (or (try
                         (last-deployed-model)
                         (catch Throwable _
                           (log/warn "Cannot query dataset entities during deploy, starting with empty model")
                           nil))
                       (core/map->ERDModel {:entities {}
                                            :relations {}}))

              ;; 1. Check for entity name conflicts (throws on conflict)
            _ (check-entity-name-conflicts! global model)

              ;; 3. Call mount to transform database with updated global model
            dataset' (core/mount this (assoc version :model model))

              ;; 4. Prepare version metadata for saving (with original model v1, not global)
            version'' (assoc
                        version
                        :model (assoc model :version 1)
                        ; :entities (core/get-entities model)
                        ; :relations (map
                        ;              #(-> %
                        ;                   (update :from :euuid)
                        ;                   (update :to :euuid))
                        ;              (core/get-relations model))
                        :deployed true)]

          ;; Reload current projection so that you can sync data for new model
        (core/reload this dataset')

        ; (def version'' version'')
        (comment
          (spit "type_error.json" (->transit version''))
          (def this *db*)
          (sync-entity this du/dataset-version version'')
          (:relations version'')
          (def version'' (<-transit (slurp "type_error.json"))))
        ;; Mark version as deployed in database
        (sync-entity this du/dataset-version version'')

          ;; Rebuild global model from ALL deployed versions to compute correct :active flags
          ;; This ensures entities not in latest versions are marked as inactive
        (let [updated-model (assoc (rebuild-global-model-from-versions) :version 1)]
          (dataset/save-model updated-model)
          (core/add-to-deploy-history this updated-model)
          ;; Regenerate GraphQL schema with updated active flags
          (try
            (lacinia/add-shard ::datasets (fn [] (lacinia/generate-lacinia-schema this)))
            (catch Throwable e
              (log/error e "Couldn't add lacinia schema shard"))))

        (log/info "Preparing model for DB")
        version'')
      (catch Throwable e
        (log/errorf e "Couldn't deploy dataset %s@%s"
                    (:name version)
                    (:name (:dataset version)))
        (throw e))))
  ;;
  (core/recall!
    [this {:keys [euuid]}]
    (assert euuid "Version :euuid is required")

    ;; 1. Query the version to get its information
    (let [version (dataset/get-entity
                    du/dataset-version
                    {:euuid euuid}
                    {:euuid nil
                     :name nil
                     :model nil
                     :deployed nil
                     :dataset [{:selections {:euuid nil
                                             :name nil}}]})

          _ (when-not version
              (throw (ex-info (format "Version %s not found" euuid)
                              {:type ::version-not-found
                               :euuid euuid})))

          ;; 2. Check if version was deployed
          _ (when-not (:deployed version)
              (throw (ex-info (format "Version %s was never deployed, cannot recall" euuid)
                              {:type ::version-not-deployed
                               :euuid euuid
                               :version version})))

          dataset (:dataset version)
          dataset-name (:name dataset)
          dataset-euuid (:euuid dataset)

          ;; 3. Get all deployed versions for this dataset
          all-deployed-versions (filter :deployed (get-dataset-versions dataset))

          ;; 4. Determine if this is the only deployed version
          only-version? (= 1 (count all-deployed-versions))

          ;; 5. Determine if this is the most recent version
          most-recent-version (first all-deployed-versions)
          is-most-recent? (= euuid (:euuid most-recent-version))

          ;; 6. Get global model with claims
          global (dataset/deployed-model)
          version-uuids #{euuid}

          ;; 7. Find exclusive entities/relations for this version
          exclusive-entities (core/find-exclusive-entities global version-uuids)
          exclusive-relations (core/find-exclusive-relations global version-uuids)]

      (log/infof "Recalling version %s@%s (only=%s, most-recent=%s)"
                 dataset-name (:name version) only-version? is-most-recent?)

      ;; 8. Unmount ONLY exclusive entities/relations
      (when (or (not-empty exclusive-entities) (not-empty exclusive-relations))
        (log/infof "Pruning %d exclusive entities and %d exclusive relations"
                   (count exclusive-entities) (count exclusive-relations))
        (with-open [con (jdbc/get-connection (:datasource *db*))]
          ;; Drop relation tables
          (doseq [relation exclusive-relations
                  :let [{:keys [from to]
                         :as relation} (core/get-relation global (:euuid relation))
                        table-name (relation->table-name relation)
                        sql (format "drop table if exists \"%s\"" table-name)]]
            (try
              (execute-one! con [sql])
              (log/tracef "Removed relation table from %s to %s: %s"
                          (:name from) (:name to) table-name)
              (catch Throwable e
                (log/errorf e "Couldn't remove relation table %s" table-name))))
          ;; Drop entity tables
          (doseq [entity exclusive-entities
                  :let [entity (core/get-entity global (:euuid entity))]
                  :when (some? entity)]
            (try
              (let [table-name (entity->table-name entity)
                    sql (format "drop table if exists \"%s\"" table-name)
                    enums-sql (drop-entity-enums-ddl entity)]
                (log/tracef "Removing entity %s: %s" (:name entity) table-name)
                (execute-one! con [sql])
                ;; Delete enum types for this entity
                (when (not-empty enums-sql)
                  (log/trace "Removing %s enum types: %s" (:name entity) enums-sql)
                  (execute-one! con [enums-sql])))
              (catch Throwable e
                (log/errorf e "Couldn't remove entity table %s" (entity->table-name entity)))))))

      ;; 9. Delete version record
      (log/infof "Deleting version record %s@%s" dataset-name (:name version))
      (delete-entity this du/dataset-version {:euuid euuid})

      ;; 9.5. Drop orphaned attribute columns and rebuild model
      ;; After deleting the version, find attributes that exist ONLY in the recalled version
      ;; and drop their database columns
      (let [;; Rebuild global model from remaining deployed versions
            updated-model (rebuild-global-model-from-versions)
            recalled-model (:model version)

            ;; Project to find what recalled version would ADD to updated global
            ;; Attributes marked :added? don't exist in updated-model → orphaned
            projection (core/project updated-model recalled-model)

            ;; Find all orphaned attributes
            orphaned-attrs (for [entity (core/get-entities projection)
                                 attr (:attributes entity)
                                 :when (core/new-attribute? attr)]
                             {:entity entity
                              :attribute attr})]

        ;; Drop orphaned columns from database
        (when (not-empty orphaned-attrs)
          (log/infof "Dropping %d orphaned attribute columns from recalled version" (count orphaned-attrs))
          (with-open [con (jdbc/get-connection (:datasource *db*))]
            (doseq [orphan orphaned-attrs]
              (let [ddl-statements (orphaned-attribute->drop-ddl orphan)]
                (doseq [sql ddl-statements]
                  (try
                    (execute-one! con [sql])
                    (log/debugf "Dropped orphaned column: %s" sql)
                    (catch Throwable e
                      (log/errorf e "Failed to drop orphaned column: %s" sql))))))))

        ;; 10. Handle three cases: only version, most recent, or older version
        ;; All cases now just rebuild (reusing updated-model from step 9.5)
        (cond
          ;; Case 1: Only deployed version - delete the dataset itself
          only-version?
          (do
            (log/infof "Last version deleted, removing dataset %s" dataset-name)
            (dataset/delete-entity du/dataset {:euuid dataset-euuid})
            ;; Rebuild model without this dataset (reuse updated-model)
            (let [final-model (assoc updated-model :version 1)]
              (dataset/save-model final-model)
              (query/deploy-schema (model->schema final-model))
              (core/add-to-deploy-history this final-model)
              (try
                (lacinia/add-shard ::datasets (fn [] (lacinia/generate-lacinia-schema this)))
                (catch Throwable e
                  (log/error e "Couldn't add lacinia schema shard")))))

          ;; Case 2: Most recent version - SIMPLIFIED (no redeploy!)
          is-most-recent?
          (do
            (log/infof "Recalled most recent version, rebuilding from remaining versions")
            ;; Just rebuild from remaining versions (reuse updated-model)
            (let [final-model (assoc updated-model :version 1)]
              (dataset/save-model final-model)
              (query/deploy-schema (model->schema final-model))
              (core/add-to-deploy-history this final-model)
              (try
                (lacinia/add-shard ::datasets (fn [] (lacinia/generate-lacinia-schema this)))
                (catch Throwable e
                  (log/error e "Couldn't add lacinia schema shard"))))))

        ;; Case 3: Older version - just reload model without this version
        :else
        (do
          (log/infof "Recalled older version %s@%s, rebuilding model" dataset-name (:name version))
          ;; Rebuild global model from remaining deployed versions (reuse updated-model)
          (let [final-model (assoc updated-model :version 1)]
            (dataset/save-model final-model)
            (query/deploy-schema (model->schema final-model))
            (core/add-to-deploy-history this final-model)
            (try
              (lacinia/add-shard ::datasets (fn [] (lacinia/generate-lacinia-schema this)))
              (catch Throwable e
                (log/error e "Couldn't add lacinia schema shard"))))))))
  ;;
  (core/destroy! [this {:keys [euuid name]}]
    (assert (or name euuid) "Specify dataset name or euuid!")
    ;; Get all versions for this dataset
    (let [all-versions (get-dataset-versions {:euuid euuid
                                              :name name})]
      (when-not (empty? all-versions)
        (log/infof "Destroying dataset %s: recalling %d versions in reverse order"
                   (or name (str euuid)) (count all-versions))
        ;; Recall each version in reverse chronological order (most recent first)
        ;; This ensures proper cleanup and rollback behavior
        (doseq [version all-versions]
          (core/recall! this {:euuid (:euuid version)})))))
  (core/get-model
    [_]
    (dataset/deployed-model))
  (core/reload
    ([this]
     (comment
       (def this *db*)
       (dataset/deployed-model))
     (let [;; Get model with claims from last-deployed-model
           model-with-claims (core/get-last-deployed this)
           ;; Convert claims to :active flags
           model' (-> model-with-claims
                      core/ensure-active-flags
                      (assoc :version 1))
           schema (model->schema model')]
       (dataset/save-model model-with-claims)
       (query/deploy-schema schema)
       (try
         (lacinia/add-shard ::datasets (fn [] (lacinia/generate-lacinia-schema this)))
         (catch Throwable e
           (log/error e "Couldn't add lacinia schema shard")))
       model-with-claims))
    ([this {:keys [model euuid]}]
     (let [global (or
                    (core/get-model this)
                    (core/map->ERDModel nil))
           model' (core/join-models global (core/add-claims model euuid))
           schema (model->schema model')]
       (query/deploy-schema schema)
       (dataset/save-model model')
       (try
         (lacinia/add-shard ::datasets (fn [] (lacinia/generate-lacinia-schema this)))
         (catch Throwable e
           (log/error e "Couldn't add lacinia schema shard")))
       model')))
  (core/mount
    [this {model :model
           :as version}]
    (log/debugf "Mounting dataset version %s@%s" (:name version) (get-in version [:dataset :name]))
    (with-open [con (jdbc/get-connection (:datasource *db*))]
      (let [global (or
                     (core/get-model this)
                     (core/map->ERDModel nil))
            ;; Model already has updated definitions and claims from deploy!
            projection (core/project global model)]
        (binding [*model* global]
          (transform-database
            con projection
            {:who/table
             (entity->table-name
               (some
                 #(core/get-entity % iu/user)
                 [global projection]))}))
        ;; Return model as-is (already contains full global state with claims)
        version)))
  (core/unmount
    [this {:keys [model]}]
    ;; USE Global model to reference true DB state
    (let [global (core/get-model this)]
      (with-open [con (jdbc/get-connection (:datasource *db*))]
        (doseq [relation (core/get-relations model)
                :let [{:keys [from to]
                       :as relation} (core/get-relation global (:euuid relation))
                      sql (format "drop table if exists \"%s\"" (relation->table-name relation))]]
          (try
            (execute-one! con [sql])
            (log/tracef
              "Removing relation from %s to %s\n%s"
              (:name from)
              (:name to)
              sql)
            (delete-entity this du/dataset-relation (select-keys relation [:euuid]))
            (catch Throwable e
              (log/errorf e "Couldn't remove table %s" (relation->table-name relation)))))
        (doseq [entity (core/get-entities model)
                :let [{:keys [attributes]
                       :as entity} (core/get-entity global (:euuid entity))]
                :when (some? entity)]
          (try
            (let [sql (format "drop table if exists \"%s\"" (entity->table-name entity))
                  enums-sql (drop-entity-enums-ddl entity)]
              (log/tracef "Removing entity %s\n%s" (:name entity) sql)
              (execute-one! con [sql])
              (delete-entity this du/dataset-entity (select-keys entity [:euuid]))
              (doseq [{:keys [euuid]} attributes]
                (delete-entity this du/dataset-entity-attribute euuid))
              (when (not-empty enums-sql)
                (log/trace "Removing %s enum types: %s" (:name entity) enums-sql)
                (execute-one! con [enums-sql])))
            (catch Throwable e
              (log/errorf e "Couldn't remove table %s" (entity->table-name entity)))))))
    (core/reload this))
  (core/setup
    ([this]
     (comment
       (def this (postgres/from-env))
       (def db neyho.eywa.db/*db*)
       (def admin (postgres/admin-from-env))
       (def db (postgres/create-db admin (:db this))))
     (let [admin (postgres/admin-from-env)
           db (postgres/create-db admin (:db this))]
       ; (def admin (postgres/admin-from-env))
       ; (def db (postgres/connect (postgres/admin-from-env)))
       ;; Set this new database as default db
       (alter-var-root #'neyho.eywa.db/*db* (constantly db))
       ;;
       (log/infof "Initializing tables for host\n%s" (pr-str (dissoc db :password)))
       (core/create-deploy-history db)
       (log/info "Created __deploy_history")
       (as-> (<-transit (slurp (io/resource "dataset/iam.json"))) model
         (core/mount db model)
         (core/reload db model))
       ; (dataset/stack-entity iu/permission iam/permissions)
       (log/info "Mounted iam.json dataset")
       (binding [core/*return-type* :edn]
         (dataset/sync-entity iu/user *EYWA*)
         (dataset/bind-service-user #'neyho.eywa.data/*EYWA*))
       (log/info "*EYWA* user created")
       (binding [*user* (:_eid *EYWA*)]
         (comment
           (alter-var-root #'*user* (fn [_] (:_eid *EYWA*))))
         (as-> (<-transit (slurp (io/resource "dataset/dataset.json"))) model
           (core/mount db model)
           (core/reload db model))
         (log/info "Mounted dataset.json dataset")
         ; (dataset/stack-entity iu/permission dataset/permissions)
         ; (dataset/load-role-schema)
         ;;
         (log/info "Deploying IAM dataset")
         (core/deploy! db (<-transit (slurp (io/resource "dataset/iam.json"))))
         ;;
         (log/info "Deploying Datasets dataset")
         (core/deploy! db (<-transit (slurp (io/resource "dataset/dataset.json"))))
         ;;
         ; (log/info "Mounted oauth.json dataset")
         ; (core/deploy! db (<-transit (slurp (io/resource "dataset/oauth.json"))))
         ;;
         (log/info "Reloading")
         (core/reload db)
         (import-app "exports/app_eywa_frontend.json")
         (import-api "exports/api_eywa_graphql.json")
         (doseq [role ["exports/role_dataset_developer.json"
                       "exports/role_dataset_modeler.json"
                       "exports/role_dataset_explorer.json"
                       "exports/role_iam_admin.json"
                       "exports/role_iam_user.json"]]
           (import-role role))
         (log/info "Adding deployed model to history")
         (core/add-to-deploy-history db (core/get-model db)))))
    ([this options]
     (core/setup this)
     (iam/setup options)))
  (core/tear-down
    [this]
    (let [admin (postgres/admin-from-env)]
      (postgres/drop-db admin (:db this))))
  (core/get-last-deployed
    ([this]
     (try
       ;; Try to get directly from DB if datasets are initialised
       (last-deployed-model)
       (catch Throwable _
         ;; If that fails... Try to pull from __deployed
         (core/get-last-deployed this 0))))
    ([_ offset]
     (with-open [connection (jdbc/get-connection (:datasource *db*))]
       (when-let [m (n/execute-one!
                      connection
                      [(cond->
                         "select model from __deploy_history order by deployed_on desc"
                         offset (str " offset " offset))])]
         (let [model (-> m :model <-transit)
               clean-model (reduce
                             (fn [m entity]
                               (if (empty? (:attributes entity))
                                 (core/remove-entity m entity)
                                 m))
                             model
                             (core/get-entities model))
               clean-schema (model->schema clean-model)]
           (with-meta clean-model {:dataset/schema clean-schema}))))))
  (core/create-deploy-history [_]
    (with-open [connection (jdbc/get-connection (:datasource *db*))]
      (n/execute-one!
        connection
        [(format
           "create table __deploy_history (
           \"deployed_on\" timestamp not null default localtimestamp,
           \"model\" text)")])))
  (core/add-to-deploy-history [_ model]
    (with-open [connection (jdbc/get-connection (:datasource *db*))]
      (n/execute-one!
        connection
        ["insert into __deploy_history (model) values (?)"
         (->transit model)])))
  ;;
  lacinia/EYWAGraphQL
  (generate-lacinia-schema [_]
    (neyho.eywa.dataset.lacinia/generate-lacinia-schema))
  ;;
  neyho.eywa.dataset.sql.naming/SQLNameResolution
  (table [_ value]
    (let [schema (query/deployed-schema)]
      (get-in schema [value :table])))
  (relation [_ table value]
    (let [{:keys [relations]} (query/deployed-schema-entity table)]
      (if (keyword? value)
        (get-in relations [value :table])
        (some
          (fn [[_ {:keys [relation table]}]]
            (when (= relation value)
              table))
          relations))))
  (related-table [_ table value]
    (let [{:keys [relations]} (query/deployed-schema-entity table)]
      (if (keyword? value)
        (get-in relations [value :to/table])
        (some
          (fn [[_ {:keys [relation table]}]]
            (when (= relation value)
              table))
          relations))))
  (relation-from-field [_ table value]
    (let [{:keys [relations]} (query/deployed-schema-entity table)]
      (if (keyword? value)
        (get-in relations [value :from/field])
        (some
          (fn [[_ {:keys [relation :from/field]}]]
            (when (= relation value)
              field))
          relations))))
  (relation-to-field [_ table value]
    (let [{:keys [relations]} (query/deployed-schema-entity table)]
      (if (keyword? value)
        (get-in relations [value :to/field])
        (some
          (fn [[_ {:keys [relation :to/field]}]]
            (when (= relation value)
              field))
          relations)))))

;; Version compatibility fix
(defn fix-on-delete-set-null-to-references
  []
  (binding [*model* (dataset/deployed-model)]
    (let [model (dataset/deployed-model)
          entities (core/get-entities model)]
      (reduce
        (fn [r entity]
          (let [user-table-name (user-table)
                entity-table (entity->table-name entity)
                modified_by (str entity-table \_ "modified_by_fkey")
                refered-attributes (filter
                                     (comp
                                       #{"user" "group" "role"}
                                       :type)
                                     (:attributes entity))
                ;;
                current-result
                (conj r
                      (format "alter table \"%s\" drop constraint \"%s\"" entity-table modified_by)
                      (format
                        "alter table \"%s\" add constraint \"%s\" foreign key (modified_by) references \"%s\"(_eid) on delete set null"
                        entity-table modified_by user-table-name))]
            (reduce
              (fn [r {attribute-name :name
                      attribute-type :type}]
                (let [attribute-column (normalize-name attribute-name)
                      constraint-name (str entity-table \_ attribute-column "_fkey")
                      refered-table (case attribute-type
                                      "user" (user-table)
                                      "group" (group-table)
                                      "role" (role-table))]
                  (conj
                    r
                    (format "alter table \"%s\" drop constraint %s" entity-table constraint-name)
                    (format
                      "alter table \"%s\" add constraint \"%s\" foreign key (%s) references \"%s\"(_eid) on delete set null"
                      entity-table constraint-name attribute-column refered-table))))
              current-result
              refered-attributes)))
        []
        entities))))

(defn get-relation-indexes
  [relation]
  (let [table (relation->table-name relation)]
    (with-open [conn (jdbc/get-connection (:datasource *db*))]
      (let [metadata (.getMetaData conn)
            indexes (.getIndexInfo metadata nil "public" table false false)]
        (loop [col indexes
               result []]
          (let [idx (.next col)]
            (if-not idx result
                    (recur col (conj result
                                     {:index (.getString col "INDEX_NAME")
                                      :column (.getString col "COLUMN_NAME")
                                      :type (if (.getBoolean col "NON_UNIQUE") :non-unique :unique)})))))))))

(defn fix-on-reference-indexes
  []
  (let [statements (binding [*model* (dataset/deployed-model)]
                     (let [model (dataset/deployed-model)
                           relations (core/get-relations model)]
                       (reduce
                         (fn [r {:keys [from to]
                                 :as relation}]
                           (let [table (relation->table-name relation)
                                 from-field (entity->relation-field from)
                                 to-field (entity->relation-field to)
                                 indexes (set (map :index (get-relation-indexes relation)))]
                             (cond->
                               r
                               (not (indexes (str table \_ "fidx")))
                               (conj (format "create index %s_fidx on \"%s\" (%s);" table table from-field))
                               ;;
                               (not (indexes (str table \_ "tidx")))
                               (conj (format "create index %s_tidx on \"%s\" (%s);" table table to-field)))))
                         []
                         relations)))]
    (with-open [con (jdbc/get-connection (:datasource *db*))]
      (doseq [statement statements]
        (try
          (execute-one! con [statement])
          (log/info statement)
          (catch Throwable ex
            (log/error ex statement)))))))

(defn get-column-type
  "Get current column type from database"
  [table-name column-name]
  (try
    (execute-one! *db*
                  ["SELECT data_type 
        FROM information_schema.columns 
        WHERE table_name = ? AND column_name = ? AND table_schema = 'public'"
                   table-name column-name])
    (catch Exception _
      (log/warn "Could not get column type for" table-name column-name)
      nil)))

(defn fix-int-types
  "Fix integer types by converting integer columns to bigint for:
   - All int type attributes
   - All references to user/group/role entities (foreign keys)
   - Skip if column is already bigint"
  []
  (binding [*model* (dataset/deployed-model)]
    (let [model (dataset/deployed-model)
          entities (core/get-entities model)
          statements (reduce
                       (fn [statements entity]
                         (let [entity-table (entity->table-name entity)

                              ;; Handle modified_by column (always references user)
                               modified-by-type (get-column-type entity-table "modified_by")
                               modified-by-statements
                               (if (and modified-by-type (= "integer" (:data_type modified-by-type)))
                                 [(format "ALTER TABLE \"%s\" ALTER COLUMN modified_by TYPE bigint" entity-table)]
                                 [])

                              ;; Handle entity attributes
                               attribute-statements
                               (reduce
                                 (fn [attr-statements {:keys [name type]}]
                                   (if-not (contains? #{"user" "group" "role" "int"} type)
                                     attr-statements
                                     (let [column-name (normalize-name name)
                                           current-type (get-column-type entity-table column-name)
                                           needs-conversion? (and current-type (= "integer" (:data_type current-type)))]
                                       (if needs-conversion?
                                         (conj attr-statements
                                               (format "ALTER TABLE \"%s\" ALTER COLUMN %s TYPE bigint"
                                                       entity-table column-name))
                                         attr-statements))))
                                 []
                                 (:attributes entity))]

                           (concat statements modified-by-statements attribute-statements)))
                       []
                       entities)]
      (with-open [con (jdbc/get-connection (:datasource *db*))]
        (doseq [statement statements]
          (try
            (execute-one! con [statement])
            (log/info "✅" statement)
            (catch Throwable ex
              (log/error "❌ EX:" ex)
              (log/error "   " statement))))))))

(defn get-column-constraints
  "Get column constraints from database including NOT NULL"
  [table-name column-name]
  (try
    (execute-one! *db*
                  ["SELECT column_name, is_nullable, data_type
        FROM information_schema.columns 
        WHERE table_name = ? AND column_name = ? AND table_schema = 'public'"
                   table-name column-name])
    (catch Exception _
      (log/warn "Could not get column constraints for" table-name column-name)
      nil)))

(defn is-mandatory-attribute?
  "Check if attribute has mandatory constraint in the dataset model"
  [attribute]
  (= "mandatory" (:constraint attribute)))

(defn fix-mandatory-constraints
  "Remove NOT NULL constraints from all mandatory fields in the dataset model.
   This allows more flexible data entry by making all fields nullable at the database level
   while still preserving the logical mandatory constraint in the dataset model."
  []
  (binding [*model* (dataset/deployed-model)]
    (let [model (dataset/deployed-model)
          entities (core/get-entities model)
          statements (reduce
                       (fn [statements entity]
                         (let [entity-table (entity->table-name entity)

                              ;; Get all mandatory attributes for this entity
                               mandatory-attributes (filter is-mandatory-attribute? (:attributes entity))

                              ;; Generate ALTER TABLE statements for mandatory attributes
                               attribute-statements
                               (reduce
                                 (fn [attr-statements {:keys [name]}]
                                   (let [column-name (normalize-name name)
                                         column-info (get-column-constraints entity-table column-name)
                                         has-not-null? (and column-info (= "NO" (:is_nullable column-info)))]
                                     (if has-not-null?
                                       (conj attr-statements
                                             (format "ALTER TABLE \"%s\" ALTER COLUMN %s DROP NOT NULL"
                                                     entity-table column-name))
                                       attr-statements)))
                                 []
                                 mandatory-attributes)]

                           (concat statements attribute-statements)))
                       []
                       entities)]
      ;; Execute the statements
      (with-open [con (jdbc/get-connection (:datasource *db*))]
        (doseq [statement statements]
          (try
            (execute-one! con [statement])
            (log/info "✅" statement)
            (catch Throwable ex
              (log/error "❌ EX:" ex)
              (log/error "   " statement))))))))
