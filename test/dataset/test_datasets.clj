(ns dataset.test-datasets
  (:require
   [clojure.pprint :refer [pprint]]
   [clojure.string :as str]
   [dataset.test-helpers :refer [make-entity make-relation add-test-attribute]]
   [next.jdbc :as jdbc]
   [neyho.eywa.dataset :as dataset]
   [neyho.eywa.dataset.core :as core]
   [neyho.eywa.dataset.uuids :as du]
   [neyho.eywa.db :refer [*db*]]
   [neyho.eywa.lacinia :as lacinia]))

(println "\n====================================")
(println "DATASET SYSTEM TEST")
(println "====================================\n")

;; Helper functions to create test models using protocol methods
(defn create-test-dataset-a []
  (let [;; Create empty model
        model (core/map->ERDModel {})

        ;; Create Product entity with attributes
        product-entity (-> (make-entity {:euuid #uuid "aaaaaaaa-1111-0000-0000-000000000001"
                                         :name "Product"})
                           (add-test-attribute {:euuid #uuid "aaaaaaaa-1111-1111-0000-000000000001"
                                                :name "Name"
                                                :type "string"
                                                :constraint "mandatory"})
                           (add-test-attribute {:euuid #uuid "aaaaaaaa-1111-1111-0000-000000000002"
                                                :name "Price"
                                                :type "float"}))

        ;; Create Category entity with attributes
        category-entity (-> (make-entity {:euuid #uuid "aaaaaaaa-1111-0000-0000-000000000002"
                                          :name "Category"})
                            (add-test-attribute {:euuid #uuid "aaaaaaaa-1111-1111-0000-000000000003"
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
                              (make-relation
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
        product-entity (-> (make-entity {:euuid #uuid "aaaaaaaa-1111-0000-0000-000000000001"
                                         :name "Product"})
                           (add-test-attribute {:euuid #uuid "aaaaaaaa-1111-1111-0000-000000000001"
                                                :name "Name"
                                                :type "string"
                                                :constraint "mandatory"})
                           (add-test-attribute {:euuid #uuid "aaaaaaaa-1111-1111-0000-000000000002"
                                                :name "Price"
                                                :type "float"})
                           (add-test-attribute {:euuid #uuid "bbbbbbbb-1111-1111-0000-000000000004"
                                                :name "Description"
                                                :type "string"}))

        ;; Create Order entity with attributes
        order-entity (-> (make-entity {:euuid #uuid "bbbbbbbb-1111-0000-0000-000000000003"
                                       :name "Order"})
                         (add-test-attribute {:euuid #uuid "bbbbbbbb-1111-1111-0000-000000000005"
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
                              (make-relation
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
        product-entity (-> (make-entity {:euuid #uuid "cccccccc-1111-0000-0000-000000000001"
                                         :name "Product"})
                           (add-test-attribute {:euuid #uuid "cccccccc-1111-1111-0000-000000000001"
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
        product-entity (-> (make-entity {:euuid #uuid "aaaaaaaa-1111-0000-0000-000000000001"
                                         :name "Product"})
                           (add-test-attribute {:euuid #uuid "aaaaaaaa-1111-1111-0000-000000000001"
                                                :name "Name"
                                                :type "string"
                                                :constraint "mandatory"})
                           (add-test-attribute {:euuid #uuid "cccccccc-1111-1111-0000-000000000010"
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
        product-entity (-> (make-entity {:euuid #uuid "aaaaaaaa-1111-0000-0000-000000000001"
                                         :name "Product"})
                           (add-test-attribute {:euuid #uuid "aaaaaaaa-1111-1111-0000-000000000001"
                                                :name "Name"
                                                :type "string"}))
        category-entity (-> (make-entity {:euuid #uuid "aaaaaaaa-1111-0000-0000-000000000002"
                                          :name "Category"})
                            (add-test-attribute {:euuid #uuid "aaaaaaaa-1111-1111-0000-000000000003"
                                                 :name "Name"
                                                 :type "string"}))
        ;; Add exclusive entity
        supplier-entity (-> (make-entity {:euuid #uuid "dddddddd-1111-0000-0000-000000000001"
                                          :name "Supplier"})
                            (add-test-attribute {:euuid #uuid "dddddddd-1111-1111-0000-000000000001"
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
        item-entity (-> (make-entity {:euuid #uuid "aaaaaaaa-1111-0000-0000-000000000001"
                                      :name "Item"})
                        (add-test-attribute {:euuid #uuid "aaaaaaaa-1111-1111-0000-000000000001"
                                             :name "Name"
                                             :type "string"}))
        category-entity (-> (make-entity {:euuid #uuid "aaaaaaaa-1111-0000-0000-000000000002"
                                          :name "Category"})
                            (add-test-attribute {:euuid #uuid "aaaaaaaa-1111-1111-0000-000000000003"
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
        product-entity (-> (make-entity {:euuid #uuid "aaaaaaaa-1111-0000-0000-000000000001"
                                         :name "Product"})
                           (add-test-attribute {:euuid #uuid "aaaaaaaa-1111-1111-0000-000000000001"
                                                :name "Name"
                                                :type "string"}))
        category-entity (-> (make-entity {:euuid #uuid "aaaaaaaa-1111-0000-0000-000000000002"
                                          :name "Category"})
                            (add-test-attribute {:euuid #uuid "aaaaaaaa-1111-1111-0000-000000000003"
                                                 :name "Name"
                                                 :type "string"}))
        model-with-entities (-> model
                                (core/add-entity product-entity)
                                (core/add-entity category-entity))
        ;; Share the SAME relation UUID as Dataset A
        model-with-relations (core/add-relation
                              model-with-entities
                              (make-relation
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

(defn get-table-columns [table-name]
  "Returns set of column names for a table"
  (with-open [con (jdbc/get-connection (:datasource neyho.eywa.db/*db*))]
    (let [columns (jdbc/execute!
                   con
                   ["SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = ?" table-name])]
      (set (map :columns/column_name columns)))))

(defn table-exists? [table-name]
  "Check if a table exists in the database"
  (with-open [con (jdbc/get-connection (:datasource neyho.eywa.db/*db*))]
    (let [result (jdbc/execute!
                  con
                  ["SELECT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'public' AND tablename = ?)" table-name])]
      (:exists (first result)))))

(defn get-graphql-types []
  "Returns set of GraphQL object type names from the schema"
  (try
    (when-let [schema @lacinia/compiled]
      (let [type-keys (keys schema)
            ;; Filter out special GraphQL keys (:Query, :Mutation, etc) and introspection types
            entity-types (filter
                          #(and (keyword? %)
                                (not (#{:Query :Mutation :Subscription} %))
                                (not (str/starts-with? (name %) "__")))
                          type-keys)]
        (set (map name entity-types))))
    (catch Exception e
      (println (format "Warning: Could not get GraphQL schema: %s" (.getMessage e)))
      #{})))

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
    (let [product-entity (get-in
                          (dataset/deployed-model)
                          [:entities #uuid "aaaaaaaa-1111-0000-0000-000000000001"])
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
          price-attr (first (filter #(= "Price" (:name %)) attributes))
          sku-attr (first (filter #(= "SKU" (:name %)) attributes))
          name-attr (first (filter #(= "Name" (:name %)) attributes))]
      (assert-equals test-name 2 (count (:claimed-by product)) "Product should have 2 claims")
      (assert-true test-name (:active product) "Product should be active")
      (assert-true test-name (some? sku-attr) "Product should have SKU attribute (from Dataset C)")
      (assert-true test-name (:active sku-attr) "SKU should be active (in last deployed)")
      (assert-true test-name (some? name-attr) "Product should have Name attribute")
      (assert-true test-name (:active name-attr) "Name should be active (in last deployed)")
      (assert-true test-name (some? price-attr) "Product should have Price attribute (persisted)")
      (assert-true test-name (not (:active price-attr)) "Price should be inactive (not in last deployed)"))))

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
      (assert-true test-name (contains? (:claimed-by item) dataset-a-v1-uuid) "Should have v1 claim (preserved for rollback)"))
    (core/destroy! *db* (dataset/get-entity
                         neyho.eywa.dataset.uuids/dataset
                         {:euuid #uuid "aaaaaaaa-0000-0000-0000-000000000000"}
                         {:euuid nil
                          :name nil
                          :versions [{:selections
                                      {:euuid nil}}]}))
    nil))

;; Helper datasets for recall testing
(defn create-order-v1 []
  "Create Order dataset v1 with just Order entity"
  (let [model (core/map->ERDModel {})
        order-entity (-> (make-entity {:euuid #uuid "eeeeeeee-1111-0000-0000-000000000001"
                                       :name "Order"})
                         (add-test-attribute {:euuid #uuid "eeeeeeee-1111-1111-0000-000000000001"
                                              :name "OrderNumber"
                                              :type "string"
                                              :constraint "mandatory"}))
        model-with-entity (core/add-entity model order-entity)]
    {:euuid #uuid "eeeeeeee-0000-0000-0000-000000000001"
     :name "Order Dataset v1"
     :dataset {:euuid #uuid "eeeeeeee-0000-0000-0000-000000000000"
               :name "Order Dataset"}
     :model model-with-entity}))

(defn create-order-v2 []
  "Create Order dataset v2 with Order + Shipment entities"
  (let [model (core/map->ERDModel {})
        ;; Re-include Order entity from v1
        order-entity (-> (make-entity {:euuid #uuid "eeeeeeee-1111-0000-0000-000000000001"
                                       :name "Order"})
                         (add-test-attribute {:euuid #uuid "eeeeeeee-1111-1111-0000-000000000001"
                                              :name "OrderNumber"
                                              :type "string"
                                              :constraint "mandatory"}))
        ;; Add new Shipment entity
        shipment-entity (-> (make-entity {:euuid #uuid "eeeeeeee-1111-0000-0000-000000000002"
                                          :name "Shipment"})
                            (add-test-attribute {:euuid #uuid "eeeeeeee-1111-1111-0000-000000000003"
                                                 :name "TrackingCode"
                                                 :type "string"}))
        model-with-entities (-> model
                                (core/add-entity order-entity)
                                (core/add-entity shipment-entity))]
    {:euuid #uuid "eeeeeeee-0000-0000-0000-000000000002"
     :name "Order Dataset v2"
     :dataset {:euuid #uuid "eeeeeeee-0000-0000-0000-000000000000"
               :name "Order Dataset"}
     :model model-with-entities}))

;; Recall test scenarios (Scenario 7)
(defn scenario-7a-recall-only-version []
  "Test recalling the only deployed version - should delete dataset entirely"
  (let [test-name "Scenario 7a"]
    (println "\n1. Deploy Dataset A v1 (only version)")
    (let [version (create-test-dataset-a)]
      (core/deploy! *db* version)

      (println "\n2. Verify deployment successful")
      (let [model (dataset/deployed-model)
            product (core/get-entity model #uuid "aaaaaaaa-1111-0000-0000-000000000001")]
        (assert-true test-name (:active product) "Product should be active after deployment")
        (assert-true test-name (table-exists? "product") "Product table should exist"))

      (println "\n3. Recall the only version")
      (core/recall! *db* {:euuid (:euuid version)})

      (println "\n4. Verify dataset deleted entirely")
      (let [model (dataset/deployed-model)
            product (core/get-entity model #uuid "aaaaaaaa-1111-0000-0000-000000000001")
            category (core/get-entity model #uuid "aaaaaaaa-1111-0000-0000-000000000002")
            dataset (try
                      (dataset/get-entity
                       neyho.eywa.dataset.uuids/dataset
                       {:euuid #uuid "aaaaaaaa-0000-0000-0000-000000000000"}
                       {:euuid nil})
                      (catch Throwable _ nil))]
        (assert-true test-name (nil? product) "Product should not exist in model after recall")
        (assert-true test-name (nil? category) "Category should not exist in model after recall")
        (assert-true test-name (not (table-exists? "product")) "Product table should be dropped")
        (assert-true test-name (not (table-exists? "category")) "Category table should be dropped")
        (assert-true test-name (nil? dataset) "Dataset itself should be deleted")))
    nil))

(defn scenario-7b-recall-most-recent []
  "Test recalling most recent version - should rollback to previous version"
  (let [test-name "Scenario 7b"]
    (println "\n1. Deploy Dataset A v1 (Product with Name, Price)")
    (let [v1 (create-test-dataset-a)]
      (core/deploy! *db* v1)

      (println "\n2. Deploy Dataset A v2 (renamed Product to Item, added Description)")
      (let [v2 (create-test-dataset-a-v2)]
        (core/deploy! *db* v2)

        (println "\n3. Verify v2 is active (entity renamed to Item)")
        (let [model-before (dataset/deployed-model)
              item (core/get-entity model-before #uuid "aaaaaaaa-1111-0000-0000-000000000001")]
          (assert-equals test-name "Item" (:name item) "Entity should be named 'Item' in v2")
          (assert-true test-name (:active item) "Item should be active")
          (assert-equals test-name 2 (count (:claimed-by item)) "Should have 2 claims (v1 + v2)"))

        (println "\n4. Recall most recent version (v2)")
        (core/recall! *db* {:euuid (:euuid v2)})

        (println "\n5. Verify rolled back to v1 (entity reverted to Product)")
        (let [model-after (dataset/deployed-model)
              product (core/get-entity model-after #uuid "aaaaaaaa-1111-0000-0000-000000000001")]
          (assert-equals test-name "Product" (:name product) "Entity should be reverted to 'Product' from v1")
          (assert-true test-name (:active product) "Product should still be active")
          (assert-equals test-name 1 (count (:claimed-by product)) "Should have 1 claim (only v1)")
          (assert-true test-name (contains? (:claimed-by product) (:euuid v1)) "Should be claimed by v1"))

        ;; Cleanup
        (core/destroy! *db* {:euuid #uuid "aaaaaaaa-0000-0000-0000-000000000000"})))
    nil))

(defn scenario-7c-recall-older-version []
  "Test recalling an older version - should just remove it without affecting current"
  (let [test-name "Scenario 7c"]
    (println "\n1. Deploy Order Dataset v1 (Order entity)")
    (let [v1 (create-order-v1)]
      (core/deploy! *db* v1)

      (println "\n2. Deploy Order Dataset v2 (Order + Shipment)")
      (let [v2 (create-order-v2)]
        (core/deploy! *db* v2)

        (println "\n3. Verify both entities active")
        (let [model-before (dataset/deployed-model)
              order (core/get-entity model-before #uuid "eeeeeeee-1111-0000-0000-000000000001")
              shipment (core/get-entity model-before #uuid "eeeeeeee-1111-0000-0000-000000000002")]
          (assert-true test-name (:active order) "Order should be active")
          (assert-true test-name (:active shipment) "Shipment should be active")
          (assert-equals test-name 2 (count (:claimed-by order)) "Order claimed by v1 + v2"))

        (println "\n4. Recall older version (v1)")
        (core/recall! *db* {:euuid (:euuid v1)})

        (println "\n5. Verify v2 remains active, v1 claim removed")
        (let [model-after (dataset/deployed-model)
              order (core/get-entity model-after #uuid "eeeeeeee-1111-0000-0000-000000000001")
              shipment (core/get-entity model-after #uuid "eeeeeeee-1111-0000-0000-000000000002")]
          (assert-true test-name (:active order) "Order should still be active (in v2)")
          (assert-true test-name (:active shipment) "Shipment should still be active (in v2)")
          (assert-equals test-name 1 (count (:claimed-by order)) "Order should have 1 claim (only v2)")
          (assert-true test-name (contains? (:claimed-by order) (:euuid v2)) "Order claimed by v2")
          (assert-true test-name (table-exists? "order") "Order table should still exist")
          (assert-true test-name (table-exists? "shipment") "Shipment table should still exist"))

        ;; Cleanup
        (core/destroy! *db* {:euuid #uuid "eeeeeeee-0000-0000-0000-000000000000"})))
    nil))

;; Datasets for lifecycle testing (Scenario 8)
(defn create-lifecycle-v1 []
  "Initial version with TestUser and TestRole entities"
  (let [model (core/map->ERDModel {})
        user-entity (-> (make-entity {:euuid #uuid "ffffffff-1111-0000-0000-000000000001"
                                      :name "TestUser"})
                        (add-test-attribute {:euuid #uuid "ffffffff-1111-1111-0000-000000000001"
                                             :name "Email"
                                             :type "string"
                                             :constraint "mandatory"}))
        role-entity (-> (make-entity {:euuid #uuid "ffffffff-1111-0000-0000-000000000002"
                                      :name "TestRole"})
                        (add-test-attribute {:euuid #uuid "ffffffff-1111-1111-0000-000000000002"
                                             :name "Name"
                                             :type "string"}))
        model-with-entities (-> model
                                (core/add-entity user-entity)
                                (core/add-entity role-entity))]
    {:euuid #uuid "ffffffff-0000-0000-0000-000000000001"
     :name "Lifecycle Test v1"
     :version "1.0.0"
     :dataset {:euuid #uuid "ffffffff-0000-0000-0000-000000000000"
               :name "LifecycleTest"}
     :model model-with-entities}))

(defn create-lifecycle-v2 []
  "Add TestPermission entity and relation"
  (let [model (core/map->ERDModel {})
        user-entity (-> (make-entity {:euuid #uuid "ffffffff-1111-0000-0000-000000000001"
                                      :name "TestUser"})
                        (add-test-attribute {:euuid #uuid "ffffffff-1111-1111-0000-000000000001"
                                             :name "Email"
                                             :type "string"}))
        role-entity (-> (make-entity {:euuid #uuid "ffffffff-1111-0000-0000-000000000002"
                                      :name "TestRole"})
                        (add-test-attribute {:euuid #uuid "ffffffff-1111-1111-0000-000000000002"
                                             :name "Name"
                                             :type "string"}))
        permission-entity (-> (make-entity {:euuid #uuid "ffffffff-1111-0000-0000-000000000003"
                                            :name "TestPermission"})
                              (add-test-attribute {:euuid #uuid "ffffffff-1111-1111-0000-000000000003"
                                                   :name "Code"
                                                   :type "string"}))
        model-with-entities (-> model
                                (core/add-entity user-entity)
                                (core/add-entity role-entity)
                                (core/add-entity permission-entity))
        model-with-relations (core/add-relation
                              model-with-entities
                              (make-relation
                               {:euuid #uuid "ffffffff-2222-0000-0000-000000000001"
                                :from #uuid "ffffffff-1111-0000-0000-000000000002"
                                :to #uuid "ffffffff-1111-0000-0000-000000000003"
                                :from-label "test_role"
                                :to-label "test_permission"
                                :cardinality "m2m"}))]
    {:euuid #uuid "ffffffff-0000-0000-0000-000000000002"
     :name "Lifecycle Test v2"
     :version "2.0.0"
     :dataset {:euuid #uuid "ffffffff-0000-0000-0000-000000000000"
               :name "LifecycleTest"}
     :model model-with-relations}))

(defn create-lifecycle-v3 []
  "Remove TestRole, keep TestUser and TestPermission"
  (let [model (core/map->ERDModel {})
        user-entity (-> (make-entity {:euuid #uuid "ffffffff-1111-0000-0000-000000000001"
                                      :name "TestUser"})
                        (add-test-attribute {:euuid #uuid "ffffffff-1111-1111-0000-000000000001"
                                             :name "Email"
                                             :type "string"}))
        permission-entity (-> (make-entity {:euuid #uuid "ffffffff-1111-0000-0000-000000000003"
                                            :name "TestPermission"})
                              (add-test-attribute {:euuid #uuid "ffffffff-1111-1111-0000-000000000003"
                                                   :name "Code"
                                                   :type "string"}))
        model-with-entities (-> model
                                (core/add-entity user-entity)
                                (core/add-entity permission-entity))]
    {:euuid #uuid "ffffffff-0000-0000-0000-000000000003"
     :name "Lifecycle Test v3"
     :version "3.0.0"
     :dataset {:euuid #uuid "ffffffff-0000-0000-0000-000000000000"
               :name "LifecycleTest"}
     :model model-with-entities}))

(defn scenario-8-iterative-lifecycle []
  (let [test-name "Scenario 8"]
    (println "\n=== Phase 1: Initial Deployment ===")
    (println "1. Deploy v1 (TestUser, TestRole)")
    (core/deploy! *db* (create-lifecycle-v1))

    (let [model (dataset/deployed-model)]
      (assert-true test-name (table-exists? "testuser") "TestUser table should exist")
      (assert-true test-name (table-exists? "testrole") "TestRole table should exist")
      (assert-true test-name (contains? (get-graphql-types) "TestUser") "GraphQL should have TestUser type")
      (assert-true test-name (contains? (get-graphql-types) "TestRole") "GraphQL should have TestRole type"))

    (println "\n=== Phase 2: Add Entity and Relation ===")
    (println "2. Deploy v2 (TestUser, TestRole, TestPermission + relation)")
    (core/deploy! *db* (create-lifecycle-v2))

    (let [model (dataset/deployed-model)
          user (get-in model [:entities #uuid "ffffffff-1111-0000-0000-000000000001"])
          role (get-in model [:entities #uuid "ffffffff-1111-0000-0000-000000000002"])
          permission (get-in model [:entities #uuid "ffffffff-1111-0000-0000-000000000003"])
          relation (get-in model [:relations #uuid "ffffffff-2222-0000-0000-000000000001"])]
      (assert-true test-name (table-exists? "testpermission") "TestPermission table should exist")
      (assert-true test-name (:active permission) "TestPermission should be active")
      (assert-true test-name (:active relation) "TestRole->TestPermission relation should be active")
      (assert-equals test-name 2 (count (:claimed-by user)) "TestUser should have 2 claims (v1 + v2)")
      (assert-equals test-name 2 (count (:claimed-by role)) "TestRole should have 2 claims (v1 + v2)")
      (assert-true test-name (contains? (get-graphql-types) "TestPermission") "GraphQL should have TestPermission type"))

    (println "\n=== Phase 3: Remove Entity ===")
    (println "3. Deploy v3 (TestUser, TestPermission - removed TestRole)")
    (core/deploy! *db* (create-lifecycle-v3))

    (let [model (dataset/deployed-model)
          user (get-in model [:entities #uuid "ffffffff-1111-0000-0000-000000000001"])
          role (get-in model [:entities #uuid "ffffffff-1111-0000-0000-000000000002"])
          permission (get-in model [:entities #uuid "ffffffff-1111-0000-0000-000000000003"])
          relation (get-in model [:relations #uuid "ffffffff-2222-0000-0000-000000000001"])]
      (assert-true test-name (not (:active role)) "TestRole should be inactive (not in current version v3)")
      (assert-true test-name (not (:active relation)) "TestRole->TestPermission relation should be inactive")
      (assert-true test-name (table-exists? "testrole") "TestRole table should REMAIN (for rollback to v1/v2)")
      (assert-true test-name (:active user) "TestUser should still be active")
      (assert-true test-name (:active permission) "TestPermission should still be active")
      (assert-equals test-name 3 (count (:claimed-by user)) "TestUser should have 3 claims (v1 + v2 + v3)")
      (assert-equals test-name 2 (count (:claimed-by role)) "TestRole should still have 2 claims (v1 + v2)")
      (assert-true test-name (not (contains? (get-graphql-types) "TestRole")) "GraphQL should NOT have TestRole type")
      (assert-true test-name (contains? (get-graphql-types) "TestUser") "GraphQL should still have TestUser type"))

    (println "\n=== Phase 4: Verify Database Schema ===")
    (let [user-columns (get-table-columns "testuser")
          permission-columns (get-table-columns "testpermission")]
      (assert-true test-name (contains? user-columns "email") "TestUser table should have email column")
      (assert-true test-name (contains? permission-columns "code") "TestPermission table should have code column"))))

;; Datasets for attribute persistence testing (Scenario 9)
(defn create-user-v1 []
  "Create User Service v1 with attributes: Email, Phone, FirstName"
  (let [model (core/map->ERDModel {})
        user-entity (-> (make-entity {:euuid #uuid "99999999-1111-0000-0000-000000000001"
                                      :name "Test9User"})
                        (add-test-attribute {:euuid #uuid "99999999-1111-1111-0000-000000000001"
                                             :name "Email"
                                             :type "string"
                                             :constraint "mandatory"
                                             :active true})
                        (add-test-attribute {:euuid #uuid "99999999-1111-1111-0000-000000000002"
                                             :name "Phone"
                                             :type "string"
                                             :active true})
                        (add-test-attribute {:euuid #uuid "99999999-1111-1111-0000-000000000003"
                                             :name "FirstName"
                                             :type "string"
                                             :active true}))
        model-with-entity (core/add-entity model user-entity)]
    {:euuid #uuid "99999999-0000-0000-0000-000000000001"
     :name "User Service v1"
     :version "1.0.0"
     :dataset {:euuid #uuid "99999999-0000-0000-0000-000000000000"
               :name "UserService"}
     :model model-with-entity}))

(defn create-user-v2 []
  "Create User Service v2 - removes Phone, adds Address. Attrs: Email, FirstName, Address"
  (let [model (core/map->ERDModel {})
        user-entity (-> (make-entity {:euuid #uuid "99999999-1111-0000-0000-000000000001"
                                      :name "Test9User"})
                        (add-test-attribute {:euuid #uuid "99999999-1111-1111-0000-000000000001"
                                             :name "Email"
                                             :type "string"
                                             :constraint "mandatory"
                                             :active true})
                        ;; Phone REMOVED (not in v2)
                        (add-test-attribute {:euuid #uuid "99999999-1111-1111-0000-000000000003"
                                             :name "FirstName"
                                             :type "string"
                                             :active true})
                        ;; Address ADDED (new in v2)
                        (add-test-attribute {:euuid #uuid "99999999-1111-1111-0000-000000000004"
                                             :name "Address"
                                             :type "string"
                                             :active true}))
        model-with-entity (core/add-entity model user-entity)]
    {:euuid #uuid "99999999-0000-0000-0000-000000000002"
     :name "User Service v2"
     :version "2.0.0"
     :dataset {:euuid #uuid "99999999-0000-0000-0000-000000000000"
               :name "UserService"}
     :model model-with-entity}))

(defn create-user-v3 []
  "Create User Service v3 - re-adds Phone, removes FirstName, adds LastName. Attrs: Email, Phone, Address, LastName"
  (let [model (core/map->ERDModel {})
        user-entity (-> (make-entity {:euuid #uuid "99999999-1111-0000-0000-000000000001"
                                      :name "Test9User"})
                        (add-test-attribute {:euuid #uuid "99999999-1111-1111-0000-000000000001"
                                             :name "Email"
                                             :type "string"
                                             :constraint "mandatory"
                                             :active true})
                        ;; Phone RE-ADDED (was in v1, not in v2, back in v3)
                        (add-test-attribute {:euuid #uuid "99999999-1111-1111-0000-000000000002"
                                             :name "Phone"
                                             :type "string"
                                             :active true})
                        ;; FirstName REMOVED (not in v3)
                        (add-test-attribute {:euuid #uuid "99999999-1111-1111-0000-000000000004"
                                             :name "Address"
                                             :type "string"
                                             :active true})
                        ;; LastName ADDED (new in v3)
                        (add-test-attribute {:euuid #uuid "99999999-1111-1111-0000-000000000005"
                                             :name "LastName"
                                             :type "string"
                                             :active true}))
        model-with-entity (core/add-entity model user-entity)]
    {:euuid #uuid "99999999-0000-0000-0000-000000000003"
     :name "User Service v3"
     :version "3.0.0"
     :dataset {:euuid #uuid "99999999-0000-0000-0000-000000000000"
               :name "UserService"}
     :model model-with-entity}))

(defn scenario-9-attribute-persistence []
  (let [test-name "Scenario 9"]
    (println "\n=== Phase 1: Initial Deployment (v1) ===")
    (println "1. Deploy v1 with [Email, Phone, FirstName]")
    (core/deploy! *db* (create-user-v1))

    (comment
      (def test-name "Scenario 9")
      (def model (dataset/deployed-model))
      (def user (core/get-entity model #uuid "99999999-1111-0000-0000-000000000001")))
    (let [model (dataset/deployed-model)
          {attrs :attributes
           :as user} (core/get-entity model #uuid "99999999-1111-0000-0000-000000000001")
          email-attr (core/get-attribute user #uuid "99999999-1111-1111-0000-000000000001")
          phone-attr (core/get-attribute user #uuid "99999999-1111-1111-0000-000000000002")
          firstname-attr (core/get-attribute user #uuid "99999999-1111-1111-0000-000000000003")]

      (assert-equals test-name 3 (count attrs) "Should have 3 attributes")
      (assert-true test-name (:active email-attr) "Email should be active")
      (assert-true test-name (:active phone-attr) "Phone should be active")
      (assert-true test-name (:active firstname-attr) "FirstName should be active"))

    (println "\n=== Phase 2: Remove Phone, Add Address (v2) ===")
    (println "2. Deploy v2 with [Email, FirstName, Address]")
    (core/deploy! *db* (create-user-v2))

    (let [model (dataset/deployed-model)
          user (get-in model [:entities #uuid "99999999-1111-0000-0000-000000000001"])
          attrs (:attributes user)
          email-attr (first (filter #(= "Email" (:name %)) attrs))
          phone-attr (first (filter #(= "Phone" (:name %)) attrs))
          firstname-attr (first (filter #(= "FirstName" (:name %)) attrs))
          address-attr (first (filter #(= "Address" (:name %)) attrs))]

      ;; CRITICAL: All 4 attributes should exist (including removed Phone)
      (assert-equals test-name 4 (count attrs) "Should have 4 attributes (3 active + 1 inactive)")

      ;; Phone should EXIST but be INACTIVE
      (assert-true test-name (some? phone-attr) "Phone should still exist in global model")
      (assert-true test-name (not (:active phone-attr)) "Phone should be INACTIVE (not in v2)")

      ;; Email should be ACTIVE
      (assert-true test-name (:active email-attr) "Email should be active")

      ;; FirstName should be ACTIVE
      (assert-true test-name (:active firstname-attr) "FirstName should be active")

      ;; Address should be ACTIVE
      (assert-true test-name (:active address-attr) "Address should be active"))

    ;; Verify DB schema - ALL columns persist (inactive columns kept for data preservation)
    (let [user-columns (get-table-columns "test9user")]
      (assert-true test-name (contains? user-columns "email") "DB should have email column")
      (assert-true test-name (contains? user-columns "firstname") "DB should have firstname column")
      (assert-true test-name (contains? user-columns "address") "DB should have address column")
      (assert-true test-name (contains? user-columns "phone") "DB should have phone column (inactive but persisted)"))

    (println "\n=== Phase 3: Re-add Phone, Remove FirstName, Add LastName (v3) ===")
    (println "3. Deploy v3 with [Email, Phone, Address, LastName]")
    (core/deploy! *db* (create-user-v3))

    (let [model (dataset/deployed-model)
          user (get-in model [:entities #uuid "99999999-1111-0000-0000-000000000001"])
          attrs (:attributes user)
          email-attr (first (filter #(= "Email" (:name %)) attrs))
          phone-attr (first (filter #(= "Phone" (:name %)) attrs))
          firstname-attr (first (filter #(= "FirstName" (:name %)) attrs))
          address-attr (first (filter #(= "Address" (:name %)) attrs))
          lastname-attr (first (filter #(= "LastName" (:name %)) attrs))]

      ;; All 5 attributes should exist
      (assert-equals test-name 5 (count attrs) "Should have 5 attributes total (4 active + 1 inactive)")

      ;; Phone should be ACTIVE again (reactivated)
      (assert-true test-name (:active phone-attr) "Phone should be ACTIVE (reactivated in v3)")

      ;; FirstName should be INACTIVE now
      (assert-true test-name (some? firstname-attr) "FirstName should still exist in global model")
      (assert-true test-name (not (:active firstname-attr)) "FirstName should be INACTIVE")

      ;; Email should be active
      (assert-true test-name (:active email-attr) "Email should be active")

      ;; Address should be active
      (assert-true test-name (:active address-attr) "Address should be active")

      ;; LastName should be active
      (assert-true test-name (:active lastname-attr) "LastName should be active"))

    ;; Verify DB - ALL columns persist (inactive columns kept for data preservation)
    (let [user-columns (get-table-columns "test9user")]
      (assert-true test-name (contains? user-columns "email") "DB should have email")
      (assert-true test-name (contains? user-columns "phone") "DB should have phone (reactivated)")
      (assert-true test-name (contains? user-columns "address") "DB should have address")
      (assert-true test-name (contains? user-columns "lastname") "DB should have lastname")
      (assert-true test-name (contains? user-columns "firstname") "DB should have firstname (inactive but persisted)"))

    (println "\n=== Phase 4: Rollback - Recall v3 ===")
    (println "4. Recall most recent version (v3) - should rollback to v2")
    (core/recall! *db* {:euuid #uuid "99999999-0000-0000-0000-000000000003"})

    (let [model (dataset/deployed-model)
          user (get-in model [:entities #uuid "99999999-1111-0000-0000-000000000001"])
          attrs (:attributes user)
          email-attr (first (filter #(= "Email" (:name %)) attrs))
          phone-attr (first (filter #(= "Phone" (:name %)) attrs))
          firstname-attr (first (filter #(= "FirstName" (:name %)) attrs))
          address-attr (first (filter #(= "Address" (:name %)) attrs))
          lastname-attr (first (filter #(= "LastName" (:name %)) attrs))]

      ;; After recall v3: 4 attributes remain (LastName removed - was only in v3)
      (assert-equals test-name 4 (count attrs) "LastName should be removed after recall")

      ;; After rollback to v2: Email, FirstName, Address should be active
      (assert-true test-name (:active email-attr) "Email should be active")

      ;; Phone should be INACTIVE again (not in v2, but exists in v1)
      (assert-true test-name (not (:active phone-attr)) "Phone should be INACTIVE after rollback")

      ;; FirstName should be ACTIVE again (back in v2)
      (assert-true test-name (:active firstname-attr) "FirstName should be ACTIVE again")

      ;; Address should be ACTIVE
      (assert-true test-name (:active address-attr) "Address should be active")

      ;; LastName should NOT exist (was orphaned - only in v3)
      (assert-true test-name (nil? lastname-attr) "LastName should not exist after recall"))

    ;; Verify DB schema: inactive columns persist, orphaned columns dropped
    (let [user-columns (get-table-columns "test9user")]
      (assert-true test-name (contains? user-columns "email") "DB should have email")
      (assert-true test-name (contains? user-columns "firstname") "DB should have firstname (reactivated)")
      (assert-true test-name (contains? user-columns "address") "DB should have address")
      (assert-true test-name (contains? user-columns "phone") "DB should have phone (inactive but not orphaned - exists in v1)")
      (assert-true test-name (not (contains? user-columns "lastname")) "DB should NOT have lastname (orphaned - only in v3)"))

    (println "\n=== Phase 5: Redeploy v3 - Verify Data Loss ===")
    (println "5. Redeploy v3 after recall - LastName column should be recreated empty")
    (core/deploy! *db* (create-user-v3))

    (let [model (dataset/deployed-model)
          user (get-in model [:entities #uuid "99999999-1111-0000-0000-000000000001"])
          attrs (:attributes user)
          email-attr (first (filter #(= "Email" (:name %)) attrs))
          phone-attr (first (filter #(= "Phone" (:name %)) attrs))
          firstname-attr (first (filter #(= "FirstName" (:name %)) attrs))
          address-attr (first (filter #(= "Address" (:name %)) attrs))
          lastname-attr (first (filter #(= "LastName" (:name %)) attrs))]

      ;; All 5 attributes back (LastName recreated)
      (assert-equals test-name 5 (count attrs) "All 5 attributes after redeploy")

      ;; Active flags should match v3
      (assert-true test-name (:active email-attr) "Email should be active")
      (assert-true test-name (:active phone-attr) "Phone should be active (v3)")
      (assert-true test-name (:active address-attr) "Address should be active")
      (assert-true test-name (:active lastname-attr) "LastName should be active (recreated)")
      (assert-true test-name (not (:active firstname-attr)) "FirstName should be inactive"))

    ;; Verify DB: LastName column recreated
    (let [user-columns (get-table-columns "test9user")]
      (assert-true test-name (contains? user-columns "email") "DB should have email")
      (assert-true test-name (contains? user-columns "phone") "DB should have phone")
      (assert-true test-name (contains? user-columns "address") "DB should have address")
      (assert-true test-name (contains? user-columns "firstname") "DB should have firstname (inactive)")
      (assert-true test-name (contains? user-columns "lastname") "DB should have lastname (recreated)"))

    ;; Verify LastName column is EMPTY (data was lost during recall)
    (let [rows (with-open [con (jdbc/get-connection (:datasource *db*))]
                 (jdbc/execute! con ["SELECT lastname FROM test9user WHERE lastname IS NOT NULL"]))]
      (assert-equals test-name 0 (count rows) "LastName column should be empty (data lost on recall)"))

    ;; Cleanup
    (core/destroy! *db* {:euuid #uuid "99999999-0000-0000-0000-000000000000"})))

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

    ;; Delete Lifecycle Dataset
    (try
      (let [dataset (dataset/get-entity
                     neyho.eywa.dataset.uuids/dataset
                     {:euuid #uuid "ffffffff-0000-0000-0000-000000000000"}
                     {:versions [{:selections {:euuid nil
                                               :name nil}}]})]
        (when dataset
          (core/destroy! *db* dataset)
          (println "  ✓ Deleted Lifecycle Dataset")))
      (catch Exception e
        (println (format "  Lifecycle Dataset: %s" (.getMessage e)))))

    ;; Delete UserService Dataset
    (try
      (let [dataset (dataset/get-entity
                     neyho.eywa.dataset.uuids/dataset
                     {:euuid #uuid "99999999-0000-0000-0000-000000000000"}
                     {:versions [{:selections {:euuid nil
                                               :name nil}}]})]
        (when dataset
          (core/destroy! *db* dataset)
          (println "  ✓ Deleted UserService Dataset")))
      (catch Exception e
        (println (format "  UserService Dataset: %s" (.getMessage e)))))

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

  (test-scenario "Scenario 7a: Recall Only Version" scenario-7a-recall-only-version)
  (cleanup)

  (test-scenario "Scenario 7b: Recall Most Recent (Rollback)" scenario-7b-recall-most-recent)
  (cleanup)

  (test-scenario "Scenario 7c: Recall Older Version" scenario-7c-recall-older-version)
  (cleanup)

  (test-scenario "Scenario 8: Iterative Lifecycle & Integration" scenario-8-iterative-lifecycle)
  (cleanup)

  (test-scenario "Scenario 9: Attribute Persistence & Active Flags" scenario-9-attribute-persistence)
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
