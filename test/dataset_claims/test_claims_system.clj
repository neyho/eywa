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

;; Dataset for testing "last deployed wins"
(defn create-test-dataset-c []
  (let [model (core/map->ERDModel {})
        ;; Product with SAME UUID as Dataset A & B, but different attributes
        product-entity (-> (core/map->ERDEntity {:euuid #uuid "aaaaaaaa-1111-0000-0000-000000000001"
                                                 :name "Product"})
                           (core/add-attribute {:euuid #uuid "aaaaaaaa-1111-1111-0000-000000000001"
                                                :name "Name"
                                                :type "string"
                                                :constraint "mandatory"})
                           (core/add-attribute {:euuid #uuid "cccccccc-1111-1111-0000-000000000010"
                                                :name "SKU"
                                                :type "string"
                                                :constraint "mandatory"}))
        model-with-entity (core/add-entity model product-entity)]
    {:euuid #uuid "cccccccc-0000-0000-0000-000000000001"
     :name "Test Dataset C"
     :version "1.0.0"
     :dataset {:euuid #uuid "cccccccc-0000-0000-0000-000000000000"
               :name "TestC"}
     :model model-with-entity}))

;; Dataset for testing multiple shared entities
(defn create-test-dataset-d []
  (let [model (core/map->ERDModel {})
        ;; Share Product and Category from Dataset A
        product-entity (-> (core/map->ERDEntity {:euuid #uuid "aaaaaaaa-1111-0000-0000-000000000001"
                                                 :name "Product"})
                           (core/add-attribute {:euuid #uuid "aaaaaaaa-1111-1111-0000-000000000001"
                                                :name "Name"
                                                :type "string"}))
        category-entity (-> (core/map->ERDEntity {:euuid #uuid "aaaaaaaa-1111-0000-0000-000000000002"
                                                  :name "Category"})
                            (core/add-attribute {:euuid #uuid "aaaaaaaa-1111-1111-0000-000000000003"
                                                 :name "Name"
                                                 :type "string"}))
        ;; Add exclusive entity
        supplier-entity (-> (core/map->ERDEntity {:euuid #uuid "dddddddd-1111-0000-0000-000000000001"
                                                  :name "Supplier"})
                            (core/add-attribute {:euuid #uuid "dddddddd-1111-1111-0000-000000000001"
                                                 :name "Name"
                                                 :type "string"}))
        model-with-entities (-> model
                                (core/add-entity product-entity)
                                (core/add-entity category-entity)
                                (core/add-entity supplier-entity))]
    {:euuid #uuid "dddddddd-0000-0000-0000-000000000001"
     :name "Test Dataset D"
     :version "1.0.0"
     :dataset {:euuid #uuid "dddddddd-0000-0000-0000-000000000000"
               :name "TestD"}
     :model model-with-entities}))

;; Dataset A v2 with renamed entity
(defn create-test-dataset-a-v2 []
  (let [model (core/map->ERDModel {})
        ;; Same UUID as Product, but renamed to "Item"
        item-entity (-> (core/map->ERDEntity {:euuid #uuid "aaaaaaaa-1111-0000-0000-000000000001"
                                              :name "Item"})
                        (core/add-attribute {:euuid #uuid "aaaaaaaa-1111-1111-0000-000000000001"
                                             :name "Name"
                                             :type "string"}))
        category-entity (-> (core/map->ERDEntity {:euuid #uuid "aaaaaaaa-1111-0000-0000-000000000002"
                                                  :name "Category"})
                            (core/add-attribute {:euuid #uuid "aaaaaaaa-1111-1111-0000-000000000003"
                                                 :name "Name"
                                                 :type "string"}))
        model-with-entities (-> model
                                (core/add-entity item-entity)
                                (core/add-entity category-entity))]
    {:euuid #uuid "aaaaaaaa-0000-0000-0000-000000000002"
     :name "Test Dataset A v2"
     :version "2.0.0"
     :dataset {:euuid #uuid "aaaaaaaa-0000-0000-0000-000000000000"
               :name "TestA"}
     :model model-with-entities}))

;; Dataset with shared relation
(defn create-test-dataset-e []
  (let [model (core/map->ERDModel {})
        ;; Share Product and Category
        product-entity (-> (core/map->ERDEntity {:euuid #uuid "aaaaaaaa-1111-0000-0000-000000000001"
                                                 :name "Product"})
                           (core/add-attribute {:euuid #uuid "aaaaaaaa-1111-1111-0000-000000000001"
                                                :name "Name"
                                                :type "string"}))
        category-entity (-> (core/map->ERDEntity {:euuid #uuid "aaaaaaaa-1111-0000-0000-000000000002"
                                                  :name "Category"})
                            (core/add-attribute {:euuid #uuid "aaaaaaaa-1111-1111-0000-000000000003"
                                                 :name "Name"
                                                 :type "string"}))
        model-with-entities (-> model
                                (core/add-entity product-entity)
                                (core/add-entity category-entity))
        ;; Share the SAME relation UUID as Dataset A
        model-with-relations (core/add-relation
                               model-with-entities
                               (core/map->ERDRelation
                                 {:euuid #uuid "aaaaaaaa-2222-0000-0000-000000000001"
                                  :from #uuid "aaaaaaaa-1111-0000-0000-000000000001"
                                  :to #uuid "aaaaaaaa-1111-0000-0000-000000000002"
                                  :from-label "product"
                                  :to-label "category"
                                  :cardinality "m2o"}))]
    {:euuid #uuid "eeeeeeee-0000-0000-0000-000000000001"
     :name "Test Dataset E"
     :version "1.0.0"
     :dataset {:euuid #uuid "eeeeeeee-0000-0000-0000-000000000000"
               :name "TestE"}
     :model model-with-relations}))

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

;; Test result tracking
(def test-results (atom []))

(defn record-result! [test-name status message]
  (swap! test-results conj {:test test-name
                            :status status
                            :message message}))

(defn assert-equals [test-name expected actual description]
  (if (= expected actual)
    (do
      (println (format "   ✅ PASS: %s" description))
      (record-result! test-name :pass description)
      true)
    (do
      (println (format "   ❌ FAIL: %s" description))
      (println (format "      Expected: %s" expected))
      (println (format "      Got:      %s" actual))
      (record-result! test-name :fail (format "%s (expected %s, got %s)" description expected actual))
      false)))

(defn assert-true [test-name actual description]
  (if actual
    (do
      (println (format "   ✅ PASS: %s" description))
      (record-result! test-name :pass description)
      true)
    (do
      (println (format "   ❌ FAIL: %s" description))
      (record-result! test-name :fail description)
      false)))

(defn assert-exception [test-name exception-type-key description]
  (fn [e]
    (if (= exception-type-key (:type (ex-data e)))
      (do
        (println (format "   ✅ PASS: %s" description))
        (record-result! test-name :pass description)
        true)
      (do
        (println (format "   ❌ FAIL: %s" description))
        (println (format "      Expected exception type: %s" exception-type-key))
        (println (format "      Got: %s" (:type (ex-data e))))
        (record-result! test-name :fail (format "%s (wrong exception type)" description))
        false))))

(defn test-scenario [name test-fn]
  (println "\n")
  (println "=====================================")
  (println (str "TEST: " name))
  (println "=====================================")
  (try
    (test-fn)
    (catch Exception e
      (println (format "\n   ❌ UNEXPECTED EXCEPTION: %s" (.getMessage e)))
      (record-result! name :error (.getMessage e))
      (when-let [data (ex-data e)]
        (println "\n   Exception data:")
        (pprint data)))))

;; Test Scenarios
(comment
  (cleanup)
  (def test-name "Scenario 1")
  (def model (dataset/deployed-model)))

(defn scenario-1-shared-entity []
  (let [test-name "Scenario 1"]
    (println "\n1. Deploy Dataset A (Product, Category)")
    (core/deploy! *db* (create-test-dataset-a))

    (println "\n2. Deploy Dataset B (Product[shared], Order)")
    (core/deploy! *db* (create-test-dataset-b))

    (println "\n3. Verify Product claimed by BOTH datasets")
    (let [product-entity (get-in (dataset/deployed-model) [:entities #uuid "aaaaaaaa-1111-0000-0000-000000000001"])
          claims (:claimed-by product-entity)]
      (assert-equals test-name 2 (count claims) "Product should be claimed by 2 datasets"))

    (println "\n4. Delete Dataset A")
    (let [dataset-a-uuid #uuid "aaaaaaaa-0000-0000-0000-000000000000"
          dataset (dataset/get-entity
                    neyho.eywa.dataset.uuids/dataset
                    {:euuid dataset-a-uuid}
                    {:versions [{:selections {:euuid nil
                                              :name nil}}]})]
      (core/destroy! *db* dataset))

    (println "\n5. Verify Product still exists, Category deleted")
    (let [model (dataset/deployed-model)
          product (get-in model [:entities #uuid "aaaaaaaa-1111-0000-0000-000000000001"])
          category (get-in model [:entities #uuid "aaaaaaaa-1111-0000-0000-000000000002"])]
      (assert-true test-name (:active product) "Product should still be active")
      (assert-equals test-name 1 (count (:claimed-by product)) "Product should have 1 claim remaining")
      (assert-true test-name (not (:active category)) "Category should be inactive")
      (assert-equals test-name 0 (count (:claimed-by category)) "Category should have 0 claims")))
  (let [test-name "Scenario 1"]
    (println "\n1. Deploy Dataset A (Product, Category)")
    (core/deploy! *db* (create-test-dataset-a))

    (println "\n2. Deploy Dataset B (Product[shared], Order)")
    (core/deploy! *db* (create-test-dataset-b))

    (println "\n3. Verify Product claimed by BOTH datasets")
    (let [product-entity (get-in (dataset/deployed-model) [:entities #uuid "aaaaaaaa-1111-0000-0000-000000000001"])
          claims (:claimed-by product-entity)]
      (assert-equals test-name 2 (count claims) "Product should be claimed by 2 datasets"))

    (println "\n4. Delete Dataset A")
    (let [dataset-a-uuid #uuid "aaaaaaaa-0000-0000-0000-000000000000"
          dataset (dataset/get-entity
                    neyho.eywa.dataset.uuids/dataset
                    {:euuid dataset-a-uuid}
                    {:versions [{:selections {:euuid nil
                                              :name nil}}]})]
      (core/destroy! *db* dataset))

    (println "\n5. Verify Product still exists, Category deleted")
    (let [model (dataset/deployed-model)
          product (get-in model [:entities #uuid "aaaaaaaa-1111-0000-0000-000000000001"])
          category (get-in model [:entities #uuid "aaaaaaaa-1111-0000-0000-000000000002"])]
      (assert-true test-name (:active product) "Product should still be active")
      (assert-equals test-name 1 (count (:claimed-by product)) "Product should have 1 claim remaining")
      (assert-true test-name (not (:active category)) "Category should be inactive")
      (assert-equals test-name 0 (count (:claimed-by category)) "Category should have 0 claims"))))

(defn scenario-2-name-conflict []
  (let [test-name "Scenario 2"]
    (println "\n1. Deploy Dataset A (Product with UUID A)")
    (core/deploy! *db* (create-test-dataset-a))

    (println "\n2. Try to deploy Conflict Dataset (same name, different UUID)")
    (try
      (core/deploy! *db* (create-test-dataset-conflict))
      (assert-true test-name false "Should have thrown entity-name-conflict")
      (catch Exception e
        ((assert-exception test-name :neyho.eywa.dataset.postgres/entity-name-conflict
                           "Should reject deployment with conflicting entity name") e)))))

(defn scenario-3-reactivation []
  (let [test-name "Scenario 3"]
    (println "\n1. Deploy Dataset A (Product, Category)")
    (core/deploy! *db* (create-test-dataset-a))

    (println "\n2. Delete Dataset A (exclusive entities should be dropped)")
    (let [dataset-a-uuid #uuid "aaaaaaaa-0000-0000-0000-000000000000"
          dataset (dataset/get-entity
                    neyho.eywa.dataset.uuids/dataset
                    {:euuid dataset-a-uuid}
                    {:versions [{:selections {:euuid nil
                                              :name nil}}]})]
      (core/destroy! *db* dataset))

    (let [model-after-delete (dataset/deployed-model)
          product-after-delete (get-in model-after-delete [:entities #uuid "aaaaaaaa-1111-0000-0000-000000000001"])]
      (assert-true test-name (not (:active product-after-delete)) "Product should be inactive after deletion"))

    (println "\n3. Re-deploy Dataset A (should reactivate entities)")
    (core/deploy! *db* (create-test-dataset-a))

    (println "\n4. Verify entities were reactivated")
    (let [model (dataset/deployed-model)
          product (get-in model [:entities #uuid "aaaaaaaa-1111-0000-0000-000000000001"])
          category (get-in model [:entities #uuid "aaaaaaaa-1111-0000-0000-000000000002"])]
      (assert-true test-name (:active product) "Product should be active after redeployment")
      (assert-equals test-name 1 (count (:claimed-by product)) "Product should have 1 claim")
      (assert-true test-name (:active category) "Category should be active after redeployment"))))

(defn scenario-4-last-deployed-wins []
  (let [test-name "Scenario 4"]
    (println "\n1. Deploy Dataset A (Product with Name, Price)")
    (core/deploy! *db* (create-test-dataset-a))

    (println "\n2. Deploy Dataset C (Product with Name, SKU - same UUID, different attributes)")
    (core/deploy! *db* (create-test-dataset-c))

    (println "\n3. Verify Product has 2 claims and C's definition (last wins)")
    (let [model (dataset/deployed-model)
          product (get-in model [:entities #uuid "aaaaaaaa-1111-0000-0000-000000000001"])
          attributes (:attributes product)
          attr-names (set (map :name attributes))]
      (assert-equals test-name 2 (count (:claimed-by product)) "Product should have 2 claims")
      (assert-true test-name (:active product) "Product should be active")
      (assert-true test-name (contains? attr-names "SKU") "Product should have SKU attribute (from Dataset C)")
      (assert-true test-name (not (contains? attr-names "Price")) "Product should not have Price attribute (A's definition replaced)"))))

(defn scenario-5-multiple-shared-entities []
  (let [test-name "Scenario 5"]
    (println "\n1. Deploy Dataset A (Product, Category)")
    (core/deploy! *db* (create-test-dataset-a))

    (println "\n2. Deploy Dataset D (Product, Category shared; Supplier exclusive)")
    (core/deploy! *db* (create-test-dataset-d))

    (println "\n3. Verify shared entities have 2 claims")
    (let [model (dataset/deployed-model)
          product (get-in model [:entities #uuid "aaaaaaaa-1111-0000-0000-000000000001"])
          category (get-in model [:entities #uuid "aaaaaaaa-1111-0000-0000-000000000002"])
          supplier (get-in model [:entities #uuid "dddddddd-1111-0000-0000-000000000001"])]
      (assert-equals test-name 2 (count (:claimed-by product)) "Product should have 2 claims")
      (assert-equals test-name 2 (count (:claimed-by category)) "Category should have 2 claims")
      (assert-equals test-name 1 (count (:claimed-by supplier)) "Supplier should have 1 claim"))

    (println "\n4. Delete Dataset A")
    (let [dataset-a-uuid #uuid "aaaaaaaa-0000-0000-0000-000000000000"
          dataset (dataset/get-entity
                    neyho.eywa.dataset.uuids/dataset
                    {:euuid dataset-a-uuid}
                    {:versions [{:selections {:euuid nil
                                              :name nil}}]})]
      (core/destroy! *db* dataset))

    (println "\n5. Verify Product and Category survive, exclusive relation deleted")
    (let [model (dataset/deployed-model)
          product (get-in model [:entities #uuid "aaaaaaaa-1111-0000-0000-000000000001"])
          category (get-in model [:entities #uuid "aaaaaaaa-1111-0000-0000-000000000002"])
          supplier (get-in model [:entities #uuid "dddddddd-1111-0000-0000-000000000001"])
          product-category-relation (get-in model [:relations #uuid "aaaaaaaa-2222-0000-0000-000000000001"])]
      (assert-true test-name (:active product) "Product should still be active (shared)")
      (assert-equals test-name 1 (count (:claimed-by product)) "Product should have 1 claim remaining")
      (assert-true test-name (:active category) "Category should still be active (shared)")
      (assert-equals test-name 1 (count (:claimed-by category)) "Category should have 1 claim remaining")
      (assert-true test-name (:active supplier) "Supplier should still be active")
      (assert-true test-name (not (:active product-category-relation)) "Product-Category relation should be inactive (exclusive to A)"))))

(defn scenario-6-entity-renaming []
  (let [test-name "Scenario 6"]
    (println "\n1. Deploy Dataset A v1 (Product, Category)")
    (core/deploy! *db* (create-test-dataset-a))

    (let [model-v1 (dataset/deployed-model)
          product-v1 (get-in model-v1 [:entities #uuid "aaaaaaaa-1111-0000-0000-000000000001"])]
      (assert-equals test-name "Product" (:name product-v1) "Entity should be named 'Product' in v1"))

    (println "\n2. Deploy Dataset A v2 (same UUID, renamed to 'Item')")
    (core/deploy! *db* (create-test-dataset-a-v2))

    (println "\n3. Verify entity renamed, claims persist with UUID")
    (let [model-v2 (dataset/deployed-model)
          item (get-in model-v2 [:entities #uuid "aaaaaaaa-1111-0000-0000-000000000001"])
          dataset-a-v1-uuid #uuid "aaaaaaaa-0000-0000-0000-000000000001"
          dataset-a-v2-uuid #uuid "aaaaaaaa-0000-0000-0000-000000000002"]
      (assert-equals test-name "Item" (:name item) "Entity should be renamed to 'Item'")
      (assert-true test-name (:active item) "Entity should still be active")
      (assert-true test-name (contains? (:claimed-by item) dataset-a-v2-uuid) "Should have v2 claim")
      (assert-true test-name (not (contains? (:claimed-by item) dataset-a-v1-uuid)) "Should NOT have v1 claim (replaced)"))
    (core/destroy! *db* (dataset/get-entity
                          neyho.eywa.dataset.uuids/dataset
                          {:euuid #uuid "aaaaaaaa-0000-0000-0000-000000000000"}
                          {:euuid nil
                           :name nil
                           :versions [{:selections
                                       {:euuid nil}}]}))
    nil))



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

    ;; Delete Dataset C
    (try
      (let [dataset (dataset/get-entity
                      neyho.eywa.dataset.uuids/dataset
                      {:euuid #uuid "cccccccc-0000-0000-0000-000000000000"}
                      {:versions [{:selections {:euuid nil
                                                :name nil}}]})]
        (when dataset
          (core/destroy! *db* dataset)
          (println "  ✓ Deleted Dataset C")))
      (catch Exception e
        (println (format "  Dataset C: %s" (.getMessage e)))))

    ;; Delete Dataset D
    (try
      (let [dataset (dataset/get-entity
                      neyho.eywa.dataset.uuids/dataset
                      {:euuid #uuid "dddddddd-0000-0000-0000-000000000000"}
                      {:versions [{:selections {:euuid nil
                                                :name nil}}]})]
        (when dataset
          (core/destroy! *db* dataset)
          (println "  ✓ Deleted Dataset D")))
      (catch Exception e
        (println (format "  Dataset D: %s" (.getMessage e)))))

    ;; Delete Dataset E
    (try
      (let [dataset (dataset/get-entity
                      neyho.eywa.dataset.uuids/dataset
                      {:euuid #uuid "eeeeeeee-0000-0000-0000-000000000000"}
                      {:versions [{:selections {:euuid nil
                                                :name nil}}]})]
        (when dataset
          (core/destroy! *db* dataset)
          (println "  ✓ Deleted Dataset E")))
      (catch Exception e
        (println (format "  Dataset E: %s" (.getMessage e)))))

    (println "\n✓ Cleanup complete")
    (catch Exception e
      (println (format "\n✗ Cleanup failed: %s" (.getMessage e))))))

;; Main test runner with summary
(defn run-tests []
  (reset! test-results [])
  (println "\n====================================")
  (println "DATASET CLAIMS SYSTEM TEST")
  (println "====================================")

  ;; Clean up before starting
  (cleanup)

  ;; Run test scenarios
  (test-scenario "Scenario 1: Shared Entity Deployment" scenario-1-shared-entity)
  (cleanup)

  (test-scenario "Scenario 2: Name Conflict Detection" scenario-2-name-conflict)
  (cleanup)

  (test-scenario "Scenario 3: Entity Reactivation" scenario-3-reactivation)
  (cleanup)

  (test-scenario "Scenario 4: Last Deployed Wins" scenario-4-last-deployed-wins)
  (cleanup)

  (test-scenario "Scenario 5: Multiple Shared Entities" scenario-5-multiple-shared-entities)
  (cleanup)

  (test-scenario "Scenario 6: Entity Renaming" scenario-6-entity-renaming)
  (cleanup)


  ;; Print summary
  (let [results @test-results
        passed (count (filter #(= :pass (:status %)) results))
        failed (count (filter #(= :fail (:status %)) results))
        errors (count (filter #(= :error (:status %)) results))
        total (count results)]
    (println "\n")
    (println "====================================")
    (println "TEST SUMMARY")
    (println "====================================")
    (println (format "Total assertions: %d" total))
    (println (format "✅ Passed: %d" passed))
    (println (format "❌ Failed: %d" failed))
    (println (format "💥 Errors: %d" errors))
    (println "====================================")

    (when (> failed 0)
      (println "\nFailed assertions:")
      (doseq [{:keys [test message]} (filter #(= :fail (:status %)) results)]
        (println (format "  [%s] %s" test message))))

    (when (> errors 0)
      (println "\nErrors:")
      (doseq [{:keys [test message]} (filter #(= :error (:status %)) results)]
        (println (format "  [%s] %s" test message))))

    (println "")
    (if (and (= 0 failed) (= 0 errors))
      (println "🎉 ALL TESTS PASSED!")
      (println "⚠️  SOME TESTS FAILED"))
    (println "====================================\n")))

;; Run the tests
(comment
  (run-tests))
