(ns dataset.rls.multihop-relation-test
  "Tests for MULTI-HOP PURE RELATION RLS guards.

   Tests relation paths with 2+ hops, e.g.:
   - Task → Project → Team → User (3 hops)
   - Task → Project → members User (2 hops)

   This tests the JOIN chain logic in build-relation-sql."
  (:require
    [clojure.test :refer [deftest testing is]]
    [dataset.rls.e2e-test :as e2e]
    [neyho.eywa.data :refer [*EYWA*]]
    [neyho.eywa.dataset :as dataset]
    [neyho.eywa.dataset.core :as core]
    [neyho.eywa.iam.uuids :as iu]
    [dataset.test-helpers :refer [make-entity make-relation add-test-attribute]]))


;;; ============================================================================
;;; Test Model - Multi-Hop Relation Guards
;;; ============================================================================

;; Entity UUIDs
(def team-euuid #uuid "f2000001-0000-0000-0000-000000000001")
(def project-euuid #uuid "f2000001-0000-0000-0000-000000000002")
(def task-euuid #uuid "f2000001-0000-0000-0000-000000000003")

;; Attribute UUIDs
(def team-name-euuid #uuid "f2000002-0000-0000-0000-000000000001")
(def project-name-euuid #uuid "f2000002-0000-0000-0000-000000000002")
(def task-title-euuid #uuid "f2000002-0000-0000-0000-000000000003")
(def task-description-euuid #uuid "f2000002-0000-0000-0000-000000000004")

;; Relation UUIDs
(def team-members-rel-euuid #uuid "f2000003-0000-0000-0000-000000000001")
(def project-team-rel-euuid #uuid "f2000003-0000-0000-0000-000000000002")
(def task-project-rel-euuid #uuid "f2000003-0000-0000-0000-000000000003")

;; Test data UUIDs
(def test-user-charlie-euuid #uuid "f2000004-0000-0000-0000-000000000001")
(def test-user-diana-euuid #uuid "f2000004-0000-0000-0000-000000000002")
(def test-team-alpha-euuid #uuid "f2000005-0000-0000-0000-000000000001")
(def test-team-beta-euuid #uuid "f2000005-0000-0000-0000-000000000002")
(def test-project-1-euuid #uuid "f2000006-0000-0000-0000-000000000001")
(def test-project-2-euuid #uuid "f2000006-0000-0000-0000-000000000002")
(def test-task-1-euuid #uuid "f2000007-0000-0000-0000-000000000001")
(def test-task-2-euuid #uuid "f2000007-0000-0000-0000-000000000002")
(def test-task-3-euuid #uuid "f2000007-0000-0000-0000-000000000003")


(defn make-multihop-test-model
  "Creates a test model with multi-hop pure relation guards.

   Entity Hierarchy:
   - Team (has many members via m2m to User)
   - Project (belongs to Team via m2o)
   - Task (belongs to Project via m2o)

   Relations:
   - Team ←→ User (m2m: members)
   - Project → Team (m2o: team)
   - Task → Project (m2o: project)

   Multi-hop path: Task → Project → Team → User

   Guard on Task:
   - User can access task if they are member of the team that owns the project"
  []
  (let [;; Get deployed User entity
        user-entity (dataset/deployed-entity iu/user)

        ;; Team entity
        team-entity (-> (make-entity {:euuid team-euuid
                                      :name "MultiHopTeam"})
                        (add-test-attribute
                          {:euuid team-name-euuid
                           :name "name"
                           :type "string"
                           :constraint "optional"}))

        ;; Project entity
        project-entity (-> (make-entity {:euuid project-euuid
                                         :name "MultiHopProject"})
                           (add-test-attribute
                             {:euuid project-name-euuid
                              :name "name"
                              :type "string"
                              :constraint "optional"}))

        ;; Task entity with MULTI-HOP relation guard
        task-entity (-> (core/map->ERDEntity
                          {:euuid task-euuid
                           :name "MultiHopTask"
                           :type "STRONG"
                           :width 200.0
                           :height 150.0
                           :position {:x 100 :y 100}
                           :attributes []
                           :active true
                           :configuration
                           {:constraints {:unique []}
                            :rls
                            {:enabled true
                             :guards
                             [;; MULTI-HOP RELATION GUARD
                              ;; Path: Task → Project → Team → User
                              {:id (java.util.UUID/randomUUID)
                               :operation #{:read :write}
                               :conditions
                               [{:type :relation
                                 :steps [{:relation-euuid task-project-rel-euuid}
                                         {:relation-euuid project-team-rel-euuid}
                                         {:relation-euuid team-members-rel-euuid}]
                                 :target :user}]}]}}})
                        (core/add-attribute
                          {:euuid task-title-euuid
                           :name "title"
                           :type "string"
                           :constraint "optional"
                           :active true})
                        (core/add-attribute
                          {:euuid task-description-euuid
                           :name "description"
                           :type "string"
                           :constraint "optional"
                           :active true}))

        ;; Build model
        model (-> (core/map->ERDModel {})
                  (core/add-entity user-entity)
                  (core/add-entity team-entity)
                  (core/add-entity project-entity)
                  (core/add-entity task-entity)
                  ;; Relation 1: Team ←→ User (m2m members)
                  (core/add-relation
                    (make-relation
                      {:euuid team-members-rel-euuid
                       :from team-euuid
                       :to iu/user
                       :from-label "teams"
                       :to-label "members"
                       :cardinality "m2m"}))
                  ;; Relation 2: Project → Team (m2o)
                  (core/add-relation
                    (make-relation
                      {:euuid project-team-rel-euuid
                       :from project-euuid
                       :to team-euuid
                       :from-label "projects"
                       :to-label "team"
                       :cardinality "m2o"}))
                  ;; Relation 3: Task → Project (m2o)
                  (core/add-relation
                    (make-relation
                      {:euuid task-project-rel-euuid
                       :from task-euuid
                       :to project-euuid
                       :from-label "tasks"
                       :to-label "project"
                       :cardinality "m2o"})))]

    {:euuid #uuid "f2000000-0000-0000-0000-000000000001"
     :name "Multi-Hop Relation RLS Test Dataset"
     :version "1.0.0"
     :dataset {:euuid #uuid "f2000000-0000-0000-0000-000000000000"
               :name "MultiHopRLS"}
     :model model}))


;;; ============================================================================
;;; Test Data Setup
;;; ============================================================================

(defn setup-multihop-test-data!
  "Sets up test data for multi-hop relation RLS testing.

   Structure:
   - Team Alpha: members = [Charlie]
   - Team Beta: members = [Diana]
   - Project 1: team = Alpha
   - Project 2: team = Beta
   - Task 1: project = Project 1 (Charlie can see via Alpha)
   - Task 2: project = Project 1 (Charlie can see via Alpha)
   - Task 3: project = Project 2 (Diana can see via Beta)

   Multi-hop paths:
   - Task 1 → Project 1 → Team Alpha → Charlie ✓
   - Task 2 → Project 1 → Team Alpha → Charlie ✓
   - Task 3 → Project 2 → Team Beta → Diana ✓
   - Task 3 → Project 2 → Team Beta → Charlie ✗ (not in Beta)"
  [superuser-token]
  ;; Create users
  (e2e/graphql-data! superuser-token
    "mutation($data: UserInput!) {
       syncUser(data: $data) { euuid name }
     }"
    {:data {:euuid test-user-charlie-euuid
            :name "charlie_multihop"
            :password "test123"
            :active true}})

  (e2e/graphql-data! superuser-token
    "mutation($data: UserInput!) {
       syncUser(data: $data) { euuid name }
     }"
    {:data {:euuid test-user-diana-euuid
            :name "diana_multihop"
            :password "test123"
            :active true}})

  ;; Create teams with members
  (e2e/graphql-data! superuser-token
    "mutation($data: MultiHopTeamInput!) {
       syncMultiHopTeam(data: $data) { euuid name }
     }"
    {:data {:euuid test-team-alpha-euuid
            :name "Team Alpha"
            :members [{:euuid test-user-charlie-euuid}]}})

  (e2e/graphql-data! superuser-token
    "mutation($data: MultiHopTeamInput!) {
       syncMultiHopTeam(data: $data) { euuid name }
     }"
    {:data {:euuid test-team-beta-euuid
            :name "Team Beta"
            :members [{:euuid test-user-diana-euuid}]}})

  ;; Create projects
  (e2e/graphql-data! superuser-token
    "mutation($data: MultiHopProjectInput!) {
       syncMultiHopProject(data: $data) { euuid name }
     }"
    {:data {:euuid test-project-1-euuid
            :name "Project 1"
            :team {:euuid test-team-alpha-euuid}}})

  (e2e/graphql-data! superuser-token
    "mutation($data: MultiHopProjectInput!) {
       syncMultiHopProject(data: $data) { euuid name }
     }"
    {:data {:euuid test-project-2-euuid
            :name "Project 2"
            :team {:euuid test-team-beta-euuid}}})

  ;; Create tasks
  (e2e/graphql-data! superuser-token
    "mutation($data: MultiHopTaskInput!) {
       syncMultiHopTask(data: $data) { euuid title }
     }"
    {:data {:euuid test-task-1-euuid
            :title "Task 1 - Alpha's task"
            :description "Charlie can see this (Team Alpha member)"
            :project {:euuid test-project-1-euuid}}})

  (e2e/graphql-data! superuser-token
    "mutation($data: MultiHopTaskInput!) {
       syncMultiHopTask(data: $data) { euuid title }
     }"
    {:data {:euuid test-task-2-euuid
            :title "Task 2 - Another Alpha task"
            :description "Charlie can see this too"
            :project {:euuid test-project-1-euuid}}})

  (e2e/graphql-data! superuser-token
    "mutation($data: MultiHopTaskInput!) {
       syncMultiHopTask(data: $data) { euuid title }
     }"
    {:data {:euuid test-task-3-euuid
            :title "Task 3 - Beta's task"
            :description "Diana can see this (Team Beta member)"
            :project {:euuid test-project-2-euuid}}}))


;;; ============================================================================
;;; Tests - Multi-Hop Relation Guards
;;; ============================================================================

(deftest test-multihop-charlie-sees-alpha-tasks
  (testing "Multi-hop (3 hops): Charlie sees Team Alpha's tasks"
    (let [charlie-token (e2e/create-test-token! {:name "charlie_multihop"
                                                  :euuid test-user-charlie-euuid})
          tasks (e2e/graphql-data! charlie-token
                  "{ searchMultiHopTask { euuid title project { name team { name } } } }")]

      ;; Charlie should see both Task 1 and Task 2 (via Team Alpha)
      (is (= 2 (count (:searchMultiHopTask tasks)))
          "Charlie should see exactly 2 tasks (Team Alpha's tasks)")

      (let [task-euuids (set (map :euuid (:searchMultiHopTask tasks)))]
        (is (contains? task-euuids (str test-task-1-euuid))
            "Charlie should see Task 1 (Task → Project 1 → Team Alpha → Charlie)")
        (is (contains? task-euuids (str test-task-2-euuid))
            "Charlie should see Task 2 (Task → Project 1 → Team Alpha → Charlie)")
        (is (not (contains? task-euuids (str test-task-3-euuid)))
            "Charlie should NOT see Task 3 (Team Beta's task)")))))


(deftest test-multihop-diana-sees-beta-tasks
  (testing "Multi-hop (3 hops): Diana sees Team Beta's tasks"
    (let [diana-token (e2e/create-test-token! {:name "diana_multihop"
                                                :euuid test-user-diana-euuid})
          tasks (e2e/graphql-data! diana-token
                  "{ searchMultiHopTask { euuid title } }")]

      ;; Diana should see only Task 3 (via Team Beta)
      (is (= 1 (count (:searchMultiHopTask tasks)))
          "Diana should see exactly 1 task (Team Beta's task)")

      (is (= (str test-task-3-euuid)
             (:euuid (first (:searchMultiHopTask tasks))))
          "Diana should see Task 3 (Task → Project 2 → Team Beta → Diana)"))))


(deftest test-multihop-nested-query-verification
  (testing "Multi-hop: Nested GraphQL shows full relation path"
    (let [charlie-token (e2e/create-test-token! {:name "charlie_multihop"
                                                  :euuid test-user-charlie-euuid})
          tasks (e2e/graphql-data! charlie-token
                  "{ searchMultiHopTask {
                       euuid
                       title
                       project {
                         euuid
                         name
                         team {
                           euuid
                           name
                           members { euuid name }
                         }
                       }
                     } }")]

      ;; Verify full relation path for Task 1
      (let [task1 (first (filter #(= (str test-task-1-euuid) (:euuid %))
                                 (:searchMultiHopTask tasks)))]
        (is (some? task1) "Task 1 should exist")
        (is (= "Project 1" (:name (:project task1)))
            "Task 1 should show Project 1")
        (is (= "Team Alpha" (:name (:team (:project task1))))
            "Project 1 should show Team Alpha")
        (is (some #(= "charlie_multihop" (:name %))
                  (:members (:team (:project task1))))
            "Team Alpha should show Charlie as member")))))


(deftest test-multihop-write-access
  (testing "Multi-hop: Team members can write to team's tasks"
    (let [charlie-token (e2e/create-test-token! {:name "charlie_multihop"
                                                  :euuid test-user-charlie-euuid})]

      ;; Charlie updates Task 1 (his team's task) - should SUCCEED
      (is (= "Updated by Charlie"
             (:title (:syncMultiHopTask
                       (e2e/graphql-data! charlie-token
                         "mutation($data: MultiHopTaskInput!) {
                            syncMultiHopTask(data: $data) { euuid title }
                          }"
                         {:data {:euuid test-task-1-euuid
                                 :title "Updated by Charlie"}}))))
          "Charlie should be able to update his team's task"))))


(deftest test-multihop-write-denied-other-team
  (testing "Multi-hop: Non-team members cannot write to team's tasks"
    (let [charlie-token (e2e/create-test-token! {:name "charlie_multihop"
                                                  :euuid test-user-charlie-euuid})]

      ;; Charlie tries to update Task 3 (Team Beta's task) - should FAIL
      (let [result (e2e/graphql! charlie-token
                     "mutation($data: MultiHopTaskInput!) {
                        syncMultiHopTask(data: $data) { euuid title }
                      }"
                     {:data {:euuid test-task-3-euuid
                             :title "Charlie tries to hack Beta's task"}})]
        (is (seq (:errors result))
            "Charlie should NOT be able to update Team Beta's task")
        (is (some #(re-find #"(?i)rls|access|denied" (str %)) (:errors result))
            "Error should mention RLS or access denied")))))


(deftest test-multihop-aggregation
  (testing "Multi-hop: Aggregations respect multi-hop RLS guards"
    (let [charlie-token (e2e/create-test-token! {:name "charlie_multihop"
                                                  :euuid test-user-charlie-euuid})
          diana-token (e2e/create-test-token! {:name "diana_multihop"
                                                :euuid test-user-diana-euuid})
          charlie-count (e2e/graphql-data! charlie-token
                          "{ aggregateMultiHopTask { count } }")
          diana-count (e2e/graphql-data! diana-token
                        "{ aggregateMultiHopTask { count } }")]

      ;; Charlie should count 2 tasks (Team Alpha)
      (is (= 2 (:count (:aggregateMultiHopTask charlie-count)))
          "Charlie should count 2 tasks (Team Alpha)")

      ;; Diana should count 1 task (Team Beta)
      (is (= 1 (:count (:aggregateMultiHopTask diana-count)))
          "Diana should count 1 task (Team Beta)"))))


(deftest test-multihop-superuser-sees-all
  (testing "Multi-hop: Superuser sees all tasks regardless of team membership"
    (let [su-token (e2e/create-test-token! {:name "EYWA"
                                            :euuid (:euuid *EYWA*)})
          tasks (e2e/graphql-data! su-token
                  "{ searchMultiHopTask { euuid title } }")]

      ;; Superuser should see all 3 tasks
      (is (>= (count (:searchMultiHopTask tasks)) 3)
          "Superuser should see all tasks")

      (let [task-euuids (set (map :euuid (:searchMultiHopTask tasks)))]
        (is (contains? task-euuids (str test-task-1-euuid))
            "Superuser should see Task 1")
        (is (contains? task-euuids (str test-task-2-euuid))
            "Superuser should see Task 2")
        (is (contains? task-euuids (str test-task-3-euuid))
            "Superuser should see Task 3")))))


;;; ============================================================================
;;; Test Runner
;;; ============================================================================

(defn deploy-multihop-model!
  "Deploys the multi-hop relation RLS test model."
  []
  (let [test-version (make-multihop-test-model)]
    (dataset/deploy! test-version)
    (dataset/reload)
    (println "Multi-hop relation RLS test model deployed!")
    test-version))


(defn cleanup-test-users!
  "Cleans up test users."
  []
  (try
    (require '[next.jdbc :as jdbc])
    (require '[neyho.eywa.db :as db])
    ((resolve 'jdbc/execute!) (deref (resolve 'db/*db*))
     ["DELETE FROM \"user\" WHERE name IN ('charlie_multihop', 'diana_multihop')"])
    (println "Test users deleted.")
    (catch Exception e
      (println "Note: Some test users may not exist (OK on first run):" (.getMessage e)))))


(defn cleanup-multihop-model!
  "Destroys the multi-hop test model."
  []
  (cleanup-test-users!)
  (dataset/destroy-dataset nil (:dataset (make-multihop-test-model)) nil)
  (dataset/reload)
  (println "Multi-hop relation RLS test model destroyed!"))


(defn run-multihop-tests!
  "Run multi-hop relation RLS test suite.

   This test proves that:
   1. Multi-hop relation guards work (3-hop path)
   2. JOIN chains are correctly generated
   3. Access is determined by traversing multiple relations
   4. Users only see data accessible via their team membership

   Path tested: Task → Project → Team → User (3 hops)"
  []
  (println "\n=== RLS Multi-Hop Relation Test Suite ===\n")

  ;; Cleanup previous run
  (println "0. Cleaning up previous test data...")
  (cleanup-test-users!)

  ;; Deploy model
  (println "\n1. Deploying multi-hop relation test model...")
  (deploy-multihop-model!)

  ;; Setup data
  (println "\n2. Setting up test data...")
  (let [su-token (e2e/create-test-token! {:name "EYWA" :euuid (:euuid *EYWA*)})]
    (setup-multihop-test-data! su-token))
  (println "Test data created: 2 teams, 2 projects, 3 tasks, 2 users")

  ;; Run tests
  (println "\n3. Running multi-hop relation tests...\n")
  (let [results (clojure.test/run-tests 'dataset.rls.multihop-relation-test)]
    ;; Cleanup
    (println "\n4. Cleaning up test tokens...")
    (e2e/cleanup-test-tokens!)
    (println "\n5. Destroying test dataset...")
    (cleanup-multihop-model!)
    results))


(comment
  ;; REPL Workflow
  (require '[rls.multihop-relation-test :as mh-test] :reload)

  ;; Run all multi-hop tests
  (mh-test/run-multihop-tests!)

  ;; Step by step
  (mh-test/deploy-multihop-model!)
  (def su-token (e2e/create-test-token! {:name "EYWA"
                                         :euuid (:euuid neyho.eywa.data/*EYWA*)}))
  (mh-test/setup-multihop-test-data! su-token)
  (clojure.test/run-tests 'dataset.rls.multihop-relation-test)
  (mh-test/cleanup-multihop-model!)

  ;;
  )
