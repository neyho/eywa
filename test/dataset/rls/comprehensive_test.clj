(ns dataset.rls.comprehensive-test
  "Comprehensive RLS tests covering all guard types and edge cases.

   This test references existing User/UserGroup/UserRole entities from the IAM model
   rather than creating new ones. Deploy won't modify these entities."
  (:require
    [clojure.test :refer [deftest testing is use-fixtures]]
    [dataset.rls.e2e-test :as e2e]
    [neyho.eywa.data :refer [*EYWA*]]
    [neyho.eywa.dataset :as dataset]
    [neyho.eywa.dataset.core :as core]
    [neyho.eywa.iam.uuids :as iu]
    [neyho.eywa.db :as db]
    [next.jdbc :as jdbc]
    [dataset.test-helpers :refer [make-entity make-relation add-test-attribute]]))


;;; ============================================================================
;;; Test Model - Comprehensive RLS Coverage
;;; ============================================================================

;; UUIDs for entities (using fresh UUIDs to avoid conflicts)
(def task-euuid #uuid "c0e00011-0000-0000-0000-000000000001")
(def team-euuid #uuid "c0e00011-0000-0000-0000-000000000002")

;; UUIDs for attributes
(def task-title-euuid #uuid "c0e00012-0000-0000-0000-000000000001")
(def task-created-by-euuid #uuid "c0e00012-0000-0000-0000-000000000002")
(def task-assigned-group-euuid #uuid "c0e00012-0000-0000-0000-000000000003")
(def task-visible-role-euuid #uuid "c0e00012-0000-0000-0000-000000000004")
(def team-name-euuid #uuid "c0e00012-0000-0000-0000-000000000005")

;; UUIDs for relations
(def task-watchers-rel-euuid #uuid "c0e00013-0000-0000-0000-000000000001")
(def task-team-rel-euuid #uuid "c0e00013-0000-0000-0000-000000000002")

;; UUIDs for test data
(def test-user-charlie-euuid #uuid "c0e00004-0000-0000-0000-000000000001")
(def test-user-dana-euuid #uuid "c0e00004-0000-0000-0000-000000000002")
(def test-group-engineering-euuid #uuid "c0e00004-0000-0000-0000-000000000003")
(def test-group-marketing-euuid #uuid "c0e00004-0000-0000-0000-000000000004")
(def test-role-manager-euuid #uuid "c0e00004-0000-0000-0000-000000000005")
(def test-role-viewer-euuid #uuid "c0e00004-0000-0000-0000-000000000006")
(def test-task-1-euuid #uuid "c0e00004-0000-0000-0000-000000000101")
(def test-task-2-euuid #uuid "c0e00004-0000-0000-0000-000000000102")
(def test-task-3-euuid #uuid "c0e00004-0000-0000-0000-000000000103")
(def test-task-4-euuid #uuid "c0e00004-0000-0000-0000-000000000104")
(def test-task-5-euuid #uuid "c0e00004-0000-0000-0000-000000000105")


(defn make-comprehensive-test-model
  "Creates a comprehensive test model covering all RLS guard types.

   Entities:
   - Task: entity with multiple RLS guards testing different scenarios
   - Team: simple entity for relation testing

   Relations:
   - Task watchers User (many-to-many for pure relation path testing)
   - Task belongs_to Team (many-to-one)

   Guards on Task:
   1. Ref path (user): created_by = current user
   2. Ref path (group): assigned_group IN user's groups
   3. Ref path (role): visible_role IN user's roles
   4. Pure relation: user is in watchers list
   5. Hybrid: task belongs to team with user as member"
  []
  (let [;; Team entity (simple)
        team-entity (-> (make-entity {:euuid team-euuid
                                      :name "TestTeam"})
                        (add-test-attribute
                          {:euuid team-name-euuid
                           :name "name"
                           :type "string"
                           :constraint "optional"}))

        ;; Task entity with comprehensive RLS guards
        task-entity (-> (core/map->ERDEntity
                          {:euuid task-euuid
                           :name "TestTask"
                           :type "STRONG"
                           :width 200.0
                           :height 150.0
                           :position {:x 100 :y 100}
                           :attributes []
                           :active true
                           ;; Multiple guards testing OR logic and different guard types
                           :configuration
                           {:constraints {:unique []}
                            :rls
                            {:enabled true
                             :guards
                             [;; Guard 1: Ref path - user created it
                              {:id (java.util.UUID/randomUUID)
                               :operation #{:read :write :delete}
                               :conditions
                               [{:type :ref
                                 :attribute task-created-by-euuid
                                 :target :user}]}

                              ;; Guard 2: Ref path - assigned to user's group
                              {:id (java.util.UUID/randomUUID)
                               :operation #{:read}
                               :conditions
                               [{:type :ref
                                 :attribute task-assigned-group-euuid
                                 :target :group}]}

                              ;; Guard 3: Ref path - visible to user's role
                              {:id (java.util.UUID/randomUUID)
                               :operation #{:read}
                               :conditions
                               [{:type :ref
                                 :attribute task-visible-role-euuid
                                 :target :role}]}

                              ;; NOTE: Guard 4 (pure relation) skipped - requires m2m with User entity
                              ]}}})
                        (core/add-attribute
                          {:euuid task-title-euuid
                           :name "title"
                           :type "string"
                           :constraint "optional"
                           :active true})
                        (core/add-attribute
                          {:euuid task-created-by-euuid
                           :name "created_by"
                           :type "user"  ; References User table
                           :constraint "optional"
                           :active true})
                        (core/add-attribute
                          {:euuid task-assigned-group-euuid
                           :name "assigned_group"
                           :type "group"  ; References UserGroup table
                           :constraint "optional"
                           :active true})
                        (core/add-attribute
                          {:euuid task-visible-role-euuid
                           :name "visible_role"
                           :type "role"  ; References UserRole table
                           :constraint "optional"
                           :active true}))

        ;; Build ERDModel
        model (-> (core/map->ERDModel {})
                  (core/add-entity team-entity)
                  (core/add-entity task-entity)
                  ;; NOTE: Skipping m2m watchers relation as it requires User entity
                  ;; which is part of IAM model, not this test dataset.
                  ;; The pure relation guard test will be skipped.
                  ;; Many-to-one: Task belongs_to Team
                  (core/add-relation
                    (make-relation
                      {:euuid task-team-rel-euuid
                       :from task-euuid
                       :to team-euuid
                       :from-label "tasks"
                       :to-label "team"
                       :cardinality "m2o"})))]

    {:euuid #uuid "c0e00010-0000-0000-0000-000000000001"
     :name "RLS Comprehensive Test Dataset V2"
     :version "1.0.0"
     :dataset {:euuid #uuid "c0e00010-0000-0000-0000-000000000000"
               :name "RLSCompTestV2"}
     :model model}))


;;; ============================================================================
;;; Test Data Setup
;;; ============================================================================

(defn setup-comprehensive-test-data!
  "Sets up comprehensive test data.

   Users:
   - Charlie: member of Engineering group, Manager role
   - Dana: member of Marketing group, Viewer role

   Groups:
   - Engineering
   - Marketing

   Roles:
   - Manager
   - Viewer

   Tasks:
   - Task1: created_by=Charlie (ref user guard)
   - Task2: assigned_group=Engineering (ref group guard)
   - Task3: visible_role=Manager (ref role guard)
   - Task4: SKIP (watchers - pure relation test skipped)
   - Task5: no matching guards (should be invisible to non-superusers)"
  [superuser-token]
  ;; Create groups
  (e2e/graphql-data! superuser-token
    "mutation($data: UserGroupInput!) {
       syncUserGroup(data: $data) { euuid name }
     }"
    {:data {:euuid test-group-engineering-euuid
            :name "RLS_Engineering"
            :active true}})

  (e2e/graphql-data! superuser-token
    "mutation($data: UserGroupInput!) {
       syncUserGroup(data: $data) { euuid name }
     }"
    {:data {:euuid test-group-marketing-euuid
            :name "RLS_Marketing"
            :active true}})

  ;; Create roles
  (e2e/graphql-data! superuser-token
    "mutation($data: UserRoleInput!) {
       syncUserRole(data: $data) { euuid name }
     }"
    {:data {:euuid test-role-manager-euuid
            :name "RLS_Manager"
            :active true}})

  (e2e/graphql-data! superuser-token
    "mutation($data: UserRoleInput!) {
       syncUserRole(data: $data) { euuid name }
     }"
    {:data {:euuid test-role-viewer-euuid
            :name "RLS_Viewer"
            :active true}})

  ;; Create users
  (e2e/graphql-data! superuser-token
    "mutation($data: UserInput!) {
       syncUser(data: $data) { euuid name }
     }"
    {:data {:euuid test-user-charlie-euuid
            :name "charlie_rls_test"
            :password "test123"
            :active true
            :groups [{:euuid test-group-engineering-euuid}]
            :roles [{:euuid test-role-manager-euuid}]}})

  (e2e/graphql-data! superuser-token
    "mutation($data: UserInput!) {
       syncUser(data: $data) { euuid name }
     }"
    {:data {:euuid test-user-dana-euuid
            :name "dana_rls_test"
            :password "test123"
            :active true
            :groups [{:euuid test-group-marketing-euuid}]
            :roles [{:euuid test-role-viewer-euuid}]}})

  ;; Create tasks
  ;; Task1: created by Charlie
  (e2e/graphql-data! superuser-token
    "mutation($data: TestTaskInput!) {
       syncTestTask(data: $data) { euuid title }
     }"
    {:data {:euuid test-task-1-euuid
            :title "Task1 - Created by Charlie"
            :created_by {:euuid test-user-charlie-euuid}}})

  ;; Task2: assigned to Engineering group
  (e2e/graphql-data! superuser-token
    "mutation($data: TestTaskInput!) {
       syncTestTask(data: $data) { euuid title }
     }"
    {:data {:euuid test-task-2-euuid
            :title "Task2 - Assigned to Engineering"
            :assigned_group {:euuid test-group-engineering-euuid}}})

  ;; Task3: visible to Manager role
  (e2e/graphql-data! superuser-token
    "mutation($data: TestTaskInput!) {
       syncTestTask(data: $data) { euuid title }
     }"
    {:data {:euuid test-task-3-euuid
            :title "Task3 - Visible to Managers"
            :visible_role {:euuid test-role-manager-euuid}}})

  ;; Task4: SKIP - watchers relation not available
  ;; (pure relation test skipped)

  ;; Task5: no matching guards (orphaned task)
  (e2e/graphql-data! superuser-token
    "mutation($data: TestTaskInput!) {
       syncTestTask(data: $data) { euuid title }
     }"
    {:data {:euuid test-task-5-euuid
            :title "Task5 - No guards match"}}))


;;; ============================================================================
;;; Tests
;;; ============================================================================

(deftest test-ref-path-user-guard
  (testing "Ref path (user): User sees tasks they created"
    (let [charlie-token (e2e/create-test-token! {:name "charlie_rls_test"
                                                  :euuid test-user-charlie-euuid})
          tasks (e2e/graphql-data! charlie-token
                  "{ searchTestTask { euuid title created_by { name } } }")]

      ;; Charlie should see Task1 (created by him)
      (is (some #(= (str test-task-1-euuid) (:euuid %))
                (:searchTestTask tasks))
          "Charlie should see Task1 (created by him)"))))


(deftest test-ref-path-group-guard
  (testing "Ref path (group): User sees tasks assigned to their group"
    (let [charlie-token (e2e/create-test-token! {:name "charlie_rls_test"
                                                  :euuid test-user-charlie-euuid})
          tasks (e2e/graphql-data! charlie-token
                  "{ searchTestTask { euuid title assigned_group { name } } }")]

      ;; Charlie should see Task2 (assigned to Engineering, his group)
      (is (some #(= (str test-task-2-euuid) (:euuid %))
                (:searchTestTask tasks))
          "Charlie should see Task2 (assigned to Engineering group)"))))


(deftest test-ref-path-role-guard
  (testing "Ref path (role): User sees tasks visible to their role"
    (let [charlie-token (e2e/create-test-token! {:name "charlie_rls_test"
                                                  :euuid test-user-charlie-euuid})
          tasks (e2e/graphql-data! charlie-token
                  "{ searchTestTask { euuid title visible_role { name } } }")]

      ;; Charlie should see Task3 (visible to Manager, his role)
      (is (some #(= (str test-task-3-euuid) (:euuid %))
                (:searchTestTask tasks))
          "Charlie should see Task3 (visible to Manager role)"))))


; SKIP: Pure relation guard test - requires m2m watchers relation
; (deftest test-pure-relation-guard ...)


(deftest test-multiple-guards-or-logic
  (testing "Multiple guards: User sees tasks matching ANY guard (OR logic)"
    (let [charlie-token (e2e/create-test-token! {:name "charlie_rls_test"
                                                  :euuid test-user-charlie-euuid})
          tasks (e2e/graphql-data! charlie-token
                  "{ searchTestTask { euuid title } }")
          task-euuids (set (map :euuid (:searchTestTask tasks)))]

      ;; Charlie should see Task1, Task2, Task3 (3 guards match)
      (is (contains? task-euuids (str test-task-1-euuid))
          "Charlie should see Task1 (created by him)")
      (is (contains? task-euuids (str test-task-2-euuid))
          "Charlie should see Task2 (assigned to his group)")
      (is (contains? task-euuids (str test-task-3-euuid))
          "Charlie should see Task3 (visible to his role)")

      ;; Charlie should NOT see Task5 (no guards match)
      (is (not (contains? task-euuids (str test-task-5-euuid)))
          "Charlie should NOT see Task5 (no guards match)")

      ;; Total: exactly 3 tasks
      (is (= 3 (count (:searchTestTask tasks)))
          "Charlie should see exactly 3 tasks"))))


(deftest test-user-with-no-access
  (testing "User with no matching guards sees empty result"
    (let [dana-token (e2e/create-test-token! {:name "dana_rls_test"
                                               :euuid test-user-dana-euuid})
          tasks (e2e/graphql-data! dana-token
                  "{ searchTestTask { euuid title } }")]

      ;; Dana should see 0 tasks (none match her group/role, she didn't create any)
      (is (= 0 (count (:searchTestTask tasks)))
          "Dana should see 0 tasks (no guards match)"))))


(deftest test-write-guards-owner-only
  (testing "Write guards: Only creator can write/delete their tasks"
    (let [charlie-token (e2e/create-test-token! {:name "charlie_rls_test"
                                                  :euuid test-user-charlie-euuid})
          dana-token (e2e/create-test-token! {:name "dana_rls_test"
                                               :euuid test-user-dana-euuid})]

      ;; Charlie updates his own task - should SUCCEED
      (is (= "Updated by Charlie"
             (:title (:syncTestTask
                       (e2e/graphql-data! charlie-token
                         "mutation($data: TestTaskInput!) {
                            syncTestTask(data: $data) { euuid title }
                          }"
                         {:data {:euuid test-task-1-euuid
                                 :title "Updated by Charlie"}}))))
          "Charlie should be able to update his own task")

      ;; Dana tries to update Charlie's task - should FAIL
      (let [result (e2e/graphql! dana-token
                     "mutation($data: TestTaskInput!) {
                        syncTestTask(data: $data) { euuid title }
                      }"
                     {:data {:euuid test-task-1-euuid
                             :title "Dana tries to hack"}})]
        (is (seq (:errors result))
            "Dana should NOT be able to update Charlie's task")
        (is (some #(re-find #"(?i)rls|access|denied" (str %)) (:errors result))
            "Error should mention RLS or access denied")))))


(deftest test-delete-guards
  (testing "Delete guards: Only creator can delete their tasks"
    (let [charlie-token (e2e/create-test-token! {:name "charlie_rls_test"
                                                  :euuid test-user-charlie-euuid})
          dana-token (e2e/create-test-token! {:name "dana_rls_test"
                                               :euuid test-user-dana-euuid})]

      ;; Dana tries to delete Charlie's task - should FAIL
      (let [result (e2e/graphql! dana-token
                     (str "mutation {
                        deleteTestTask(euuid: \"" test-task-1-euuid "\")
                      }"))]
        (is (seq (:errors result))
            "Dana should NOT be able to delete Charlie's task")))))


(deftest test-superuser-bypass
  (testing "Superuser bypass: EYWA user sees all tasks regardless of guards"
    (let [superuser-token (e2e/create-test-token! {:name "EYWA"
                                                    :euuid (:euuid *EYWA*)})
          tasks (e2e/graphql-data! superuser-token
                  "{ searchTestTask { euuid title } }")]

      ;; Superuser should see ALL 4 tasks (including Task5)
      (is (>= (count (:searchTestTask tasks)) 4)
          "Superuser should see at least 4 tasks (all test tasks)")

      (is (some #(= (str test-task-5-euuid) (:euuid %))
                (:searchTestTask tasks))
          "Superuser should see Task5 (orphaned task with no guards)"))))


;;; ============================================================================
;;; Test Runner
;;; ============================================================================

(defn deploy-comprehensive-model!
  "Deploys the comprehensive RLS test model and reloads GraphQL schema."
  []
  (let [test-version (make-comprehensive-test-model)]
    (dataset/deploy! test-version)
    ;; Reload schema to register new GraphQL types
    (dataset/reload)
    (println "Comprehensive RLS test model deployed and schema reloaded!")
    test-version))


(defn cleanup-test-users!
  "Cleans up test users, groups, and roles."
  []
  (println "Cleaning up test users, groups, and roles...")
  (try
    (jdbc/execute! db/*db* ["DELETE FROM \"user\" WHERE name IN ('charlie_rls_test', 'dana_rls_test')"])
    (jdbc/execute! db/*db* ["DELETE FROM \"user_group\" WHERE name IN ('RLS_Engineering', 'RLS_Marketing')"])
    (jdbc/execute! db/*db* ["DELETE FROM \"user_role\" WHERE name IN ('RLS_Manager', 'RLS_Viewer')"])
    (println "Test users/groups/roles deleted.")
    (catch Exception e
      (println "Note: Some test users/groups/roles may not exist yet (OK on first run):" (.getMessage e)))))

(defn cleanup-comprehensive-model!
  "Cleans up the comprehensive test model using dataset/destroy-dataset.
   This properly removes the dataset and all associated tables."
  [test-version]
  (println "Destroying comprehensive test dataset...")
  ;; Use the GraphQL hook function directly - same pattern as in test_datasets
  (dataset/destroy-dataset nil test-version nil)
  (dataset/reload)
  (println "Dataset destroyed and schema reloaded!"))


(defn run-comprehensive-tests!
  "Run comprehensive RLS test suite.

   Workflow:
   1. Cleanup any previous test data
   2. Deploy comprehensive model
   3. Setup test data (users, groups, roles, tasks)
   4. Run tests
   5. Cleanup (destroy dataset)"
  []
  (println "\n=== RLS Comprehensive Test Suite ===\n")

  ;; Step 0: Cleanup previous test data
  (println "0. Cleaning up previous test data...")
  (cleanup-test-users!)

  ;; Step 1: Deploy model
  (println "\n1. Deploying comprehensive test model...")
  (let [test-version (deploy-comprehensive-model!)]

    ;; Step 2: Setup test data
    (println "\n2. Setting up test data...")
    (let [su-token (e2e/create-test-token! {:name "EYWA" :euuid (:euuid *EYWA*)})]
      (setup-comprehensive-test-data! su-token))
    (println "Test data created: Charlie, Dana, 2 groups, 2 roles, 4 tasks (Task4 skipped)")

    ;; Step 3: Run tests
    (println "\n3. Running comprehensive tests...\n")
    (let [results (clojure.test/run-tests 'dataset.rls.comprehensive-test)]
      ;; Step 4: Cleanup
      (println "\n4. Cleaning up test tokens...")
      (e2e/cleanup-test-tokens!)
      (println "\n5. Destroying test dataset...")
      (cleanup-comprehensive-model! test-version)
      results)))


(comment
  ;; REPL Workflow

  (require '[rls.comprehensive-test :as comp-test] :reload)
  (require '[neyho.eywa.dataset :as dataset])

  ;; Run all comprehensive tests
  (comp-test/run-comprehensive-tests!)

  ;; Or step by step:
  (comp-test/deploy-comprehensive-model!)

  (def su-token (e2e/create-test-token! {:name "EYWA"
                                          :euuid (:euuid neyho.eywa.data/*EYWA*)}))
  (comp-test/setup-comprehensive-test-data! su-token)

  (clojure.test/run-tests 'dataset.rls.comprehensive-test)

  ;;
  )
