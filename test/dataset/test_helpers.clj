(ns dataset.test-helpers
  "Helper functions for dataset testing that provide compatibility
   between v1.0.0 (with UI attributes) and v1.0.2 (without UI attributes)"
  (:require
   [neyho.eywa.dataset.core :as core]))

;; Default UI values for entities (v1.0.0 compatibility)
(def default-entity-ui
  {:width 150.0
   :height 100.0
   :position {:x 0 :y 0}
   :type "STRONG"})

;; Default UI values for relations (v1.0.0 compatibility)
(def default-relation-ui
  {:path {:coordinates []}
   :from-label nil
   :to-label nil})

(defn make-entity
  "Creates an ERDEntity with sensible defaults for UI attributes.
   Accepts a map with :euuid, :name, and optionally :width, :height, :position, :type.
   If UI attributes are not provided, defaults will be used for v1.0.0 compatibility."
  [{:keys [euuid name width height position type] :as entity-map}]
  (core/map->ERDEntity
   (merge
    default-entity-ui
    {:euuid euuid
     :name name
     :original nil
     :clone nil
     :configuration {:constraints {:unique []}}
     :attributes []}
    (when width {:width width})
    (when height {:height height})
    (when position {:position position})
    (when type {:type type}))))

(defn make-relation
  "Creates an ERDRelation with sensible defaults for UI attributes.
   Accepts a map with :euuid, :from, :to, :cardinality, and optionally :from-label, :to-label, :path.
   If UI attributes are not provided, defaults will be used for v1.0.0 compatibility."
  [{:keys [euuid from to from-label to-label cardinality path] :as relation-map}]
  (core/map->ERDRelation
   (merge
    {:euuid euuid
     :from from
     :to to
     :cardinality cardinality}
    ;; Only add from-label/to-label if provided
    (when from-label {:from-label from-label})
    (when to-label {:to-label to-label})
    ;; Add default path if not provided (v1.0.0 compatibility)
    (when-not path {:path {:coordinates []}}))))

(defn add-test-attribute
  "Adds an attribute to an entity with defaults for :active flag.
   Accepts attribute map with :euuid, :name, :type, and optionally :constraint, :configuration, :active."
  [entity {:keys [euuid name type constraint configuration active] :as attr-map}]
  (core/add-attribute
   entity
   (merge
    {:euuid euuid
     :name name
     :type type
     :constraint (or constraint "optional")
     :active (if (nil? active) true active)}
    (when configuration {:configuration configuration}))))
