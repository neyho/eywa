(ns dataset-claims.test-claims-system
  (:require
    [clojure.pprint :refer [pprint]]
    [next.jdbc :as jdbc]
    [neyho.eywa.dataset :as dataset]
    [neyho.eywa.dataset.core :as core]
    [neyho.eywa.db :refer [*db*]]))

(println "\n====================================")
(println "DATASET CLAIMS SYSTEM TEST")
(println "====================================\n")

;; Helper functions to create test models using protocol methods
(defn create-test-dataset-a []
  (let [;; Create empty model
        model (core/map->ERDModel {})

        ;; Create Product entity with attributes
        product-entity (-> (core/map->ERDEntity {:euuid #uuid "aaaaaaaa-1111-0000-0000-000000000001"
                                                 :name "Product"})
                           (core/add-attribute {:euuid #uuid "aaaaaaaa-1111-1111-0000-000000000001"
                                                :name "Name"
                                                :type "string"
                                                :constraint "mandatory"})
                           (core/add-attribute {:euuid #uuid "aaaaaaaa-1111-1111-0000-000000000002"
                                                :name "Price"
                                                :type "float"}))

        ;; Create Category entity with attributes
        category-entity (-> (core/map->ERDEntity {:euuid #uuid "aaaaaaaa-1111-0000-0000-000000000002"
                                                  :name "Category"})
                            (core/add-attribute {:euuid #uuid "aaaaaaaa-1111-1111-0000-000000000003"
                                                 :name "Name"
                                                 :type "string"
                                                 :constraint "mandatory"}))

        ;; Add entities to model
        model-with-entities (-> model
                                (core/add-entity product-entity)
                                (core/add-entity category-entity))

        ;; Add relation
        model-with-relations (core/add-relation
                               model-with-entities
                               (core/map->ERDRelation
                                 {:euuid #uuid "aaaaaaaa-2222-0000-0000-000000000001"
                                  :from #uuid "aaaaaaaa-1111-0000-0000-000000000001"
                                  :to #uuid "aaaaaaaa-1111-0000-0000-000000000002"
                                  :from-label "product"
                                  :to-label "category"
                                  :cardinality "m2o"}))]

    {:euuid #uuid "aaaaaaaa-0000-0000-0000-000000000001"
     :name "Test Dataset A"
     :version "1.0.0"
     :dataset {:euuid #uuid "aaaaaaaa-0000-0000-0000-000000000000"
               :name "TestA"}
     :model model-with-relations}))

(defn create-test-dataset-b []
  (let [;; Create empty model
        model (core/map->ERDModel {})

        ;; Create Product entity (SAME UUID as Dataset A) with attributes
        product-entity (-> (core/map->ERDEntity {:euuid #uuid "aaaaaaaa-1111-0000-0000-000000000001"
                                                 :name "Product"})
                           (core/add-attribute {:euuid #uuid "aaaaaaaa-1111-1111-0000-000000000001"
                                                :name "Name"
                                                :type "string"
                                                :constraint "mandatory"})
                           (core/add-attribute {:euuid #uuid "aaaaaaaa-1111-1111-0000-000000000002"
                                                :name "Price"
                                                :type "float"})
                           (core/add-attribute {:euuid #uuid "bbbbbbbb-1111-1111-0000-000000000004"
                                                :name "Description"
                                                :type "string"}))

        ;; Create Order entity with attributes
        order-entity (-> (core/map->ERDEntity {:euuid #uuid "bbbbbbbb-1111-0000-0000-000000000003"
                                               :name "Order"})
                         (core/add-attribute {:euuid #uuid "bbbbbbbb-1111-1111-0000-000000000005"
                                              :name "OrderNumber"
                                              :type "string"
                                              :constraint "mandatory"}))

        ;; Add entities to model
        model-with-entities (-> model
                                (core/add-entity product-entity)
                                (core/add-entity order-entity))

        ;; Add relation
        model-with-relations (core/add-relation
                               model-with-entities
                               (core/map->ERDRelation
                                 {:euuid #uuid "bbbbbbbb-2222-0000-0000-000000000001"
                                  :from #uuid "bbbbbbbb-1111-0000-0000-000000000003"
                                  :to #uuid "aaaaaaaa-1111-0000-0000-000000000001"
                                  :from-label "order"
                                  :to-label "product"
                                  :cardinality "m2m"}))]

    {:euuid #uuid "bbbbbbbb-0000-0000-0000-000000000001"
     :name "Test Dataset B"
     :version "1.0.0"
     :dataset {:euuid #uuid "bbbbbbbb-0000-0000-0000-000000000000"
               :name "TestB"}
     :model model-with-relations}))

(defn create-test-dataset-conflict []
  (let [;; Create empty model
        model (core/map->ERDModel {})

        ;; Create Product entity with DIFFERENT UUID but SAME NAME
        product-entity (-> (core/map->ERDEntity {:euuid #uuid "cccccccc-1111-0000-0000-000000000001"
                                                 :name "Product"})
                           (core/add-attribute {:euuid #uuid "cccccccc-1111-1111-0000-000000000001"
                                                :name "Code"
                                                :type "string"}))

        ;; Add entity to model
        model-with-entity (core/add-entity model product-entity)]

    {:euuid #uuid "cccccccc-0000-0000-0000-000000000001"
     :name "Test Dataset Conflict"
     :version "1.0.0"
     :dataset {:euuid #uuid "cccccccc-0000-0000-0000-000000000000"
               :name "TestConflict"}
     :model model-with-entity}))

(defn inspect-claims []
  (println "\n--- Current Model Claims ---")
  (let [model (dataset/deployed-model)]
    (println "\n** Entities with Claims:")
    (doseq [[euuid entity] (:entities model)]
      (when-let [claims (:claimed-by entity)]
        (println (format "  %s (%s)" (:name entity) euuid))
        (println (format "    Claims: %s" claims))
        (println (format "    Active: %s" (boolean (:active entity))))))

    (println "\n** Relations with Claims:")
    (doseq [[euuid relation] (:relations model)]
      (when-let [claims (:claimed-by relation)]
        (println (format "  %s -> %s (%s)"
                         (:from-label relation)
                         (:to-label relation)
                         euuid))
        (println (format "    Claims: %s" claims))
        (println (format "    Active: %s" (boolean (:active relation))))))))

(defn list-tables []
  (println "\n--- PostgreSQL Tables ---")
  (with-open [con (jdbc/get-connection (:datasource neyho.eywa.db/*db*))]
    (let [tables (jdbc/execute!
                   con
                   ["SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND tablename NOT LIKE '%modeling%' AND tablename NOT LIKE '%deploy%' ORDER BY tablename"])]
      (doseq [table tables]
        (println (format "  - %s" (:pg_tables/tablename table)))))))

(defn test-scenario [name test-fn]
  (println "\n")
  (println "=====================================")
  (println (str "TEST: " name))
  (println "=====================================")
  (try
    (test-fn)
    (println "\n✅ Test completed")
    (catch Exception e
      (println "\n❌ Test failed with exception:")
      (println (.getMessage e))
      (when-let [data (ex-data e)]
        (println "\nException data:")
        (pprint data)))))

;; Test Scenarios

(defn scenario-1-shared-entity []
  (println "\nScenario 1: Shared Entity Deployment")
  (println "-------------------------------------")

  (println "\n1. Deploy Dataset A (Product, Category)")
  (let [dataset-a (create-test-dataset-a)]
    (core/deploy! *db* dataset-a)
    (println "   ✓ Dataset A deployed"))

  (inspect-claims)
  (list-tables)

  (println "\n2. Deploy Dataset B (Product[shared], Order)")
  (let [dataset-b (create-test-dataset-b)]
    (core/deploy! *db* dataset-b)
    (println "   ✓ Dataset B deployed"))

  (inspect-claims)

  (println "\n3. Verify Product is claimed by BOTH datasets")
  (let [model (dataset/deployed-model)
        product-entity (get-in model [:entities #uuid "aaaaaaaa-1111-0000-0000-000000000001"])
        claims (:claimed-by product-entity)]
    (println (format "   Product claims: %s" claims))
    (println (format "   Number of claims: %d" (count claims)))
    (if (= 2 (count claims))
      (println "   ✓ Product correctly claimed by both datasets")
      (println "   ✗ ERROR: Expected 2 claims!")))

  (println "\n4. Delete Dataset A")
  (let [dataset-a-uuid #uuid "aaaaaaaa-0000-0000-0000-000000000000"
        dataset (dataset/get-entity
                  neyho.eywa.dataset.uuids/dataset
                  {:euuid dataset-a-uuid}
                  {:versions [{:selections {:euuid nil
                                            :name nil}}]})]
    (def dataset dataset)
    (core/destroy! *db* dataset)
    (println "   ✓ Dataset A deleted"))

  (inspect-claims)
  (list-tables)

  (println "\n5. Verify Product table STILL EXISTS and Category is GONE")
  (let [model (dataset/deployed-model)
        product (get-in model [:entities #uuid "aaaaaaaa-1111-0000-0000-000000000001"])
        category (get-in model [:entities #uuid "aaaaaaaa-1111-0000-0000-000000000002"])]
    (println (format "   Product active: %s (should be true)" (:active product)))
    (println (format "   Product claims: %s" (:claimed-by product)))
    (println (format "   Category active: %s (should be false/nil)" (:active category)))
    (println (format "   Category claims: %s" (:claimed-by category)))))

(defn scenario-2-name-conflict []
  (println "\nScenario 2: Name Conflict Detection")
  (println "------------------------------------")

  (println "\n1. Deploy Dataset A (Product with UUID A)")
  (let [dataset-a (create-test-dataset-a)]
    (core/deploy! *db* dataset-a)
    (println "   ✓ Dataset A deployed"))

  (println "\n2. Try to deploy Conflict Dataset (Product with UUID C)")
  (println "   Expected: Should FAIL with entity-name-conflict")
  (try
    (let [dataset-conflict (create-test-dataset-conflict)]
      (core/deploy! *db* dataset-conflict)
      (println "   ✗ ERROR: Deploy succeeded but should have failed!"))
    (catch Exception e
      (if (= :neyho.eywa.dataset.postgres/entity-name-conflict
             (:type (ex-data e)))
        (do
          (println "   ✓ Correctly rejected with entity-name-conflict")
          (println (format "   Message: %s" (.getMessage e))))
        (throw e)))))

(defn scenario-3-reactivation []
  (println "\nScenario 3: Entity Reactivation")
  (println "--------------------------------")

  (println "\n1. Deploy Dataset A (Product, Category)")
  (let [dataset-a (create-test-dataset-a)]
    (core/deploy! *db* dataset-a)
    (println "   ✓ Dataset A deployed"))

  (list-tables)
  (println "   Note: product table exists")

  (println "\n2. Delete Dataset A (Product becomes inactive)")
  (let [dataset-a-uuid #uuid "aaaaaaaa-0000-0000-0000-000000000000"
        dataset (dataset/get-entity
                  neyho.eywa.dataset.uuids/dataset
                  {:euuid dataset-a-uuid}
                  {:versions [{:selections {:euuid nil
                                            :name nil}}]})]
    (core/destroy! *db* dataset)
    (println "   ✓ Dataset A deleted"))

  (list-tables)
  (println "   Note: Tables should be DROPPED (exclusive to Dataset A)")

  (println "\n3. Re-deploy Dataset A (same UUIDs)")
  (let [dataset-a (create-test-dataset-a)]
    (core/deploy! *db* dataset-a)
    (println "   ✓ Dataset A re-deployed"))

  (inspect-claims)
  (list-tables)

  (println "\n4. Verify reactivation")
  (let [model (dataset/deployed-model)
        product (get-in model [:entities #uuid "aaaaaaaa-1111-0000-0000-000000000001"])]
    (println (format "   Product active: %s (should be true)" (:active product)))
    (println (format "   Product claims: %s" (:claimed-by product)))
    (println "   Note: Table should exist (recreated or reactivated)")))

(defn cleanup []
  (println "\n\nCLEANUP")
  (println "--------")
  (println "Deleting test datasets...")
  (try
    ;; Delete Dataset A
    (try
      (let [dataset (dataset/get-entity
                      neyho.eywa.dataset.uuids/dataset
                      {:euuid #uuid "aaaaaaaa-0000-0000-0000-000000000000"}
                      {:versions [{:selections {:euuid nil
                                                :name nil}}]})]
        (when dataset
          (core/destroy! *db* dataset)
          (println "  ✓ Deleted Dataset A")))
      (catch Exception e
        (println (format "  Dataset A: %s" (.getMessage e)))))

    ;; Delete Dataset B
    (try
      (let [dataset (dataset/get-entity
                      neyho.eywa.dataset.uuids/dataset
                      {:euuid #uuid "bbbbbbbb-0000-0000-0000-000000000000"}
                      {:versions [{:selections {:euuid nil
                                                :name nil}}]})]
        (when dataset
          (core/destroy! *db* dataset)
          (println "  ✓ Deleted Dataset B")))
      (catch Exception e
        (println (format "  Dataset B: %s" (.getMessage e)))))

    ;; Delete Conflict Dataset
    (try
      (let [dataset (dataset/get-entity
                      neyho.eywa.dataset.uuids/dataset
                      {:euuid #uuid "cccccccc-0000-0000-0000-000000000000"}
                      {:versions [{:selections {:euuid nil
                                                :name nil}}]})]
        (when dataset
          (core/destroy! *db* dataset)
          (println "  ✓ Deleted Conflict Dataset")))
      (catch Exception e
        (println (format "  Conflict Dataset: %s" (.getMessage e)))))

    (println "\n✓ Cleanup complete")
    (catch Exception e
      (println (format "\n✗ Cleanup failed: %s" (.getMessage e))))))

;; Main test runner
(defn run-tests []
  (println "\nStarting claims system tests...\n")

  ;; Clean up before starting
  (cleanup)

  ;; Run test scenarios
  (test-scenario "Scenario 1: Shared Entity Deployment" scenario-1-shared-entity)
  (cleanup)

  (test-scenario "Scenario 2: Name Conflict Detection" scenario-2-name-conflict)
  (cleanup)

  (test-scenario "Scenario 3: Entity Reactivation" scenario-3-reactivation)
  (cleanup)

  (println "\n")
  (println "====================================")
  (println "ALL TESTS COMPLETE")
  (println "====================================\n"))

;; Run the tests
(comment
  (run-tests))
