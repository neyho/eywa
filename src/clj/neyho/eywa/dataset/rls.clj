(ns neyho.eywa.dataset.rls
  "Row-Level Security (RLS) Guards implementation.

   Injects WHERE conditions into queries based on compiled guard configurations
   that filter rows by relationships to IAM entities (User, UserGroup, UserRole).

   RLS is opt-in per entity - if :rls is present in compiled schema, guards
   are applied. No global env var needed.

   Uses pre-compiled schema from model->schema which resolves UUIDs to
   table/column names at deploy time."
  (:require
    [clojure.string :as str]
    [clojure.tools.logging :as log]
    [neyho.eywa.iam.access :as access]
    [neyho.eywa.iam.access.context :refer [*rls*]]))


;; =============================================================================
;; RLS Operation Context
;; =============================================================================

(def ^:dynamic *operation*
  "Current RLS operation - :read, :write, or :delete. Defaults to :read."
  :read)


;; =============================================================================
;; Guard Application Rules
;; =============================================================================

(defn should-apply-guards?
  "Check if RLS guards should be applied for current request.

   Guards are NOT applied when:
   - No RLS context (system/internal operations)
   - User is superuser

   Note: RLS is independent of EYWA_IAM_ENFORCE_ACCESS.
   If entity has :rls in schema, guards apply."
  []
  (and (some? *rls*)
       (not (access/superuser?))))


;; =============================================================================
;; Session Value Helpers
;; =============================================================================

(defn- get-session-values
  "Get the session values to match against based on match type.
   Returns vector of _eid values from *rls* context."
  [match]
  (when *rls*
    (case match
      :user (when-let [uid (:user *rls*)] [uid])
      :group (when-let [gids (seq (:groups *rls*))] (vec gids))
      :role (when-let [rids (seq (:roles *rls*))] (vec rids))
      nil)))


;; =============================================================================
;; SQL Generation - Using Pre-compiled Schema
;; =============================================================================

(defn- build-ref-sql
  "Build direct column match SQL for a :ref condition.

   Compiled condition structure:
   {:type :ref :column \"created_by\" :match :user}

   Example outputs:
   - Single value: alias.\"created_by\" = ?
   - Multiple values: alias.\"assignee_group\" IN (?, ?, ?)"
  [main-alias column session-values]
  (if (= 1 (count session-values))
    {:sql (format "%s.\"%s\" = ?" main-alias column)
     :params session-values}
    {:sql (format "%s.\"%s\" IN (%s)" main-alias column
                  (str/join "," (repeat (count session-values) "?")))
     :params session-values}))


(defn- build-relation-sql
  "Build EXISTS subquery for a :relation condition.

   Compiled condition structure:
   {:type :relation
    :match :user
    :hops [{:table \"rel_table\" :from-field \"entity_id\" :to-field \"user_id\"}]}

   Single hop example:
   EXISTS (SELECT 1 FROM \"rel_table\" l0
           WHERE l0.from_field = alias._eid AND l0.to_field = ?)

   Multi-hop example:
   EXISTS (SELECT 1 FROM \"rel1\" l0
           JOIN \"rel2\" l1 ON l0.to_field = l1.from_field
           WHERE l0.from_field = alias._eid AND l1.to_field = ?)"
  [main-alias hops session-values]
  (let [first-hop (first hops)
        ;; Build FROM clause
        from-clause (format "\"%s\" l0" (:table first-hop))

        ;; Build JOIN clauses for subsequent hops
        join-clauses (map-indexed
                       (fn [idx hop]
                         (let [prev-idx idx
                               curr-idx (inc idx)
                               prev-hop (nth hops idx)]
                           (format "JOIN \"%s\" l%d ON l%d.\"%s\" = l%d.\"%s\""
                                   (:table hop)
                                   curr-idx
                                   prev-idx
                                   (:to-field prev-hop)
                                   curr-idx
                                   (:from-field hop))))
                       (rest hops))

        join-sql (str/join " " join-clauses)

        ;; Final hop determines the target field
        final-idx (dec (count hops))
        final-hop (last hops)

        ;; Build WHERE clause
        where-sql (if (= 1 (count session-values))
                    (format "l0.\"%s\" = %s._eid AND l%d.\"%s\" = ?"
                            (:from-field first-hop) main-alias
                            final-idx (:to-field final-hop))
                    (format "l0.\"%s\" = %s._eid AND l%d.\"%s\" IN (%s)"
                            (:from-field first-hop) main-alias
                            final-idx (:to-field final-hop)
                            (str/join "," (repeat (count session-values) "?"))))]
    {:sql (if (empty? join-clauses)
            (format "EXISTS (SELECT 1 FROM %s WHERE %s)" from-clause where-sql)
            (format "EXISTS (SELECT 1 FROM %s %s WHERE %s)" from-clause join-sql where-sql))
     :params session-values}))


(defn- build-hybrid-sql
  "Build EXISTS + column match for a :hybrid condition.

   Compiled condition structure:
   {:type :hybrid
    :match :user
    :hops [{:table \"...\" :from-field \"...\" :to-field \"...\"}]
    :final-table \"target_table\"
    :final-column \"ref_column\"}

   Traverses relation hops, then matches final column against session values."
  [main-alias hops final-table final-column session-values]
  (let [first-hop (first hops)
        from-clause (format "\"%s\" l0" (:table first-hop))

        ;; Build JOIN clauses including final table
        join-clauses (concat
                       ;; Intermediate hops
                       (map-indexed
                         (fn [idx hop]
                           (let [prev-idx idx
                                 curr-idx (inc idx)
                                 prev-hop (nth hops idx)]
                             (format "JOIN \"%s\" l%d ON l%d.\"%s\" = l%d.\"%s\""
                                     (:table hop)
                                     curr-idx
                                     prev-idx
                                     (:to-field prev-hop)
                                     curr-idx
                                     (:from-field hop))))
                         (rest hops))
                       ;; Final table join
                       (let [last-idx (dec (count hops))
                             last-hop (last hops)
                             final-join-idx (count hops)]
                         [(format "JOIN \"%s\" l%d ON l%d.\"%s\" = l%d._eid"
                                  final-table
                                  final-join-idx
                                  last-idx
                                  (:to-field last-hop)
                                  final-join-idx)]))

        join-sql (str/join " " join-clauses)
        final-idx (count hops)

        where-sql (if (= 1 (count session-values))
                    (format "l0.\"%s\" = %s._eid AND l%d.\"%s\" = ?"
                            (:from-field first-hop) main-alias
                            final-idx final-column)
                    (format "l0.\"%s\" = %s._eid AND l%d.\"%s\" IN (%s)"
                            (:from-field first-hop) main-alias
                            final-idx final-column
                            (str/join "," (repeat (count session-values) "?"))))]
    {:sql (format "EXISTS (SELECT 1 FROM %s %s WHERE %s)" from-clause join-sql where-sql)
     :params session-values}))


;; =============================================================================
;; Condition & Guard SQL Generation
;; =============================================================================

(defn condition-to-sql
  "Generate SQL for a single compiled condition.

   Condition types:
   - :ref     {:type :ref :column \"name\" :match :user/:group/:role}
   - :relation {:type :relation :match :user/:group/:role :hops [...]}
   - :hybrid  {:type :hybrid :match :user/:group/:role :hops [...] :final-table \"t\" :final-column \"c\"}

   Returns {:sql \"...\" :params [...]} or nil if no session values"
  [main-alias condition]
  (let [{:keys [type match column hops final-table final-column]} condition
        session-values (get-session-values match)]
    (when (seq session-values)
      (case type
        :ref (build-ref-sql main-alias column session-values)
        :relation (build-relation-sql main-alias hops session-values)
        :hybrid (build-hybrid-sql main-alias hops final-table final-column session-values)
        nil))))


(defn guard-to-sql
  "Generate SQL for a single guard (AND of all conditions).

   Returns {:sql \"(cond1 AND cond2)\" :params [...]} or nil"
  [main-alias guard]
  (let [{:keys [conditions]} guard
        condition-sqls (keep #(condition-to-sql main-alias %) conditions)]
    (when (seq condition-sqls)
      (if (= 1 (count condition-sqls))
        (first condition-sqls)
        {:sql (format "(%s)" (str/join " AND " (map :sql condition-sqls)))
         :params (vec (mapcat :params condition-sqls))}))))


(defn compile-guards-to-sql
  "Compile all applicable guards for an operation to SQL (OR of guards).

   Arguments:
   - main-alias: Table alias for the main entity
   - guards: Vector of compiled guard configurations
   - operation: :read, :write, or :delete

   Returns {:sql \"(guard1 OR guard2)\" :params [...]} or nil"
  [main-alias guards operation]
  (let [applicable-guards (filter #(contains? (:operation %) operation) guards)
        guard-sqls (keep #(guard-to-sql main-alias %) applicable-guards)]
    (when (seq guard-sqls)
      (if (= 1 (count guard-sqls))
        (first guard-sqls)
        {:sql (format "(%s)" (str/join " OR " (map :sql guard-sqls)))
         :params (vec (mapcat :params guard-sqls))}))))


;; =============================================================================
;; Main Enhancement Function
;; =============================================================================

(defn enhance-args
  "Inject RLS conditions into query args.

   Uses pre-compiled :rls from schema (set by model->schema at deploy time).

   Arguments:
   - schema: Current query schema node with :entity/as, :rls, etc.
   - stack-data: Current [stack data] tuple
   - operation: :read, :write, or :delete

   Returns modified [stack data] with RLS conditions injected"
  [schema [stack data] operation]
  (let [{:keys [entity entity/as rls]} schema
        {:keys [enabled guards]} rls]
    (cond
      ;; No RLS config or not enabled
      (not enabled)
      [stack data]

      ;; Bypass for superuser/no user/enforcement disabled
      (not (should-apply-guards?))
      [stack data]

      ;; Apply RLS guards
      :else
      (let [{:keys [sql params]} (compile-guards-to-sql as guards operation)]
        (if sql
          (do
            (log/debugf "RLS: Applying guards for entity %s, operation %s: %s"
                        entity operation sql)
            [(conj stack [:and [sql]])
             (into (vec data) params)])
          ;; No applicable guards for this operation - deny all access
          (do
            (log/warnf "RLS: No applicable guards for entity %s, operation %s - denying access"
                       entity operation)
            [(conj stack [:and ["1=0"]])
             data]))))))


(defn enhance-args-for-read
  "Convenience wrapper for read operations"
  [schema stack-data]
  (enhance-args schema stack-data :read))


(defn enhance-args-for-write
  "Convenience wrapper for write operations"
  [schema stack-data]
  (enhance-args schema stack-data :write))


(defn enhance-args-for-delete
  "Convenience wrapper for delete operations"
  [schema stack-data]
  (enhance-args schema stack-data :delete))
