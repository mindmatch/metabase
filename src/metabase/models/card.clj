(ns metabase.models.card
  "Underlying DB model for what is now most commonly referred to as a 'Question' in most user-facing situations. Card
  is a historical name, but is the same thing; both terms are used interchangeably in the backend codebase."
  (:require [clojure.core.memoize :as memoize]
            [clojure.set :as set]
            [clojure.tools.logging :as log]
            [metabase
             [public-settings :as public-settings]
             [query-processor :as qp]
             [util :as u]]
            [metabase.api.common :as api :refer [*current-user-id*]]
            [metabase.models
             [card-label :refer [CardLabel]]
             [collection :as collection]
             [dependency :as dependency]
             [field-values :as field-values]
             [interface :as i]
             [label :refer [Label]]
             [params :as params]
             [permissions :as perms]
             [query :as query]
             [revision :as revision]]
            [metabase.query-processor.middleware.permissions :as qp-perms]
            [metabase.query-processor.util :as qputil]
            [metabase.util.query :as q]
            [puppetlabs.i18n.core :refer [tru]]
            [toucan
             [db :as db]
             [models :as models]]))

(models/defmodel Card :report_card)

;;; -------------------------------------------------- Hydration --------------------------------------------------

(defn dashboard-count
  "Return the number of Dashboards this Card is in."
  {:hydrate :dashboard_count}
  [{:keys [id]}]
  (db/count 'DashboardCard, :card_id id))

(defn labels
  "Return `Labels` for CARD."
  {:hydrate :labels}
  [{:keys [id]}]
  (if-let [label-ids (seq (db/select-field :label_id CardLabel, :card_id id))]
    (db/select Label, :id [:in label-ids], {:order-by [:%lower.name]})
    []))


;;; -------------------------------------------------- Dependencies --------------------------------------------------

(defn card-dependencies
  "Calculate any dependent objects for a given `card`."
  ([_ _ card]
   (card-dependencies card))
  ([{:keys [dataset_query]}]
   (when (and dataset_query
              (= :query (keyword (:type dataset_query))))
     {:Metric  (q/extract-metric-ids (:query dataset_query))
      :Segment (q/extract-segment-ids (:query dataset_query))})))


;;; -------------------------------------------------- Revisions --------------------------------------------------

(defn serialize-instance
  "Serialize a `Card` for use in a `Revision`."
  ([instance]
   (serialize-instance nil nil instance))
  ([_ _ instance]
   (dissoc instance :created_at :updated_at)))


;;; -------------------------------------------------- Lifecycle --------------------------------------------------

(defn populate-query-fields
  "Lift `database_id`, `table_id`, and `query_type` from query definition."
  [{{query-type :type, :as outer-query} :dataset_query, :as card}]
  (merge (when-let [{:keys [database-id table-id]} (and query-type
                                                        (query/query->database-and-table-ids outer-query))]
           {:database_id database-id
            :table_id    table-id
            :query_type  (keyword query-type)})
         card))

(defn- pre-insert [{query :dataset_query, :as card}]
  (u/prog1 card
    ;; for native queries we need to make sure the user saving the card has native query permissions for the DB
    ;; because users can always see native Cards and we don't want someone getting around their lack of permissions
    ;; that way
    (when (and *current-user-id*
               (= (keyword (:type query)) :native))
      (let [database (db/select-one ['Database :id :name], :id (:database query))]
        (qp-perms/throw-if-cannot-run-new-native-query-referencing-db database)))))

(defn- post-insert [card]
  ;; if this Card has any native template tag parameters we need to update FieldValues for any Fields that are
  ;; eligible for FieldValues and that belong to a 'On-Demand' database
  (u/prog1 card
    (when-let [field-ids (seq (params/card->template-tag-field-ids card))]
      (log/info "Card references Fields in params:" field-ids)
      (field-values/update-field-values-for-on-demand-dbs! field-ids))))

(defn- pre-update [{archived? :archived, query :dataset_query, :as card}]
  (u/prog1 card
    ;; if the Card is archived, then remove it from any Dashboards
    (when archived?
      (db/delete! 'DashboardCard :card_id (u/get-id card)))
    ;; if the template tag params for this Card have changed in any way we need to update the FieldValues for
    ;; On-Demand DB Fields
    (when (and (:dataset_query card)
               (:native (:dataset_query card)))
      (let [old-param-field-ids (params/card->template-tag-field-ids (db/select-one [Card :dataset_query]
                                                                       :id (u/get-id card)))
            new-param-field-ids (params/card->template-tag-field-ids card)]
        (when (and (seq new-param-field-ids)
                   (not= old-param-field-ids new-param-field-ids))
          (let [newly-added-param-field-ids (set/difference new-param-field-ids old-param-field-ids)]
            (log/info "Referenced Fields in Card params have changed. Was:" old-param-field-ids
                      "Is Now:" new-param-field-ids
                      "Newly Added:" newly-added-param-field-ids)
            ;; Now update the FieldValues for the Fields referenced by this Card.
            (field-values/update-field-values-for-on-demand-dbs! newly-added-param-field-ids)))))))

(defn- pre-delete [{:keys [id]}]
  (db/delete! 'PulseCard :card_id id)
  (db/delete! 'Revision :model "Card", :model_id id)
  (db/delete! 'DashboardCardSeries :card_id id)
  (db/delete! 'DashboardCard :card_id id)
  (db/delete! 'CardFavorite :card_id id)
  (db/delete! 'CardLabel :card_id id))


(u/strict-extend (class Card)
  models/IModel
  (merge models/IModelDefaults
         {:hydration-keys (constantly [:card])
          :types          (constantly {:dataset_query          :json
                                       :description            :clob
                                       :display                :keyword
                                       :embedding_params       :json
                                       :query_type             :keyword
                                       :result_metadata        :json
                                       :visualization_settings :json
                                       :read_permissions       :json-set})
          :properties     (constantly {:timestamped? true})
          :pre-update     (comp populate-query-fields pre-update)
          :pre-insert     (comp populate-query-fields pre-insert)
          :post-insert    post-insert
          :pre-delete     pre-delete
          :post-select    public-settings/remove-public-uuid-if-public-sharing-is-disabled})

  ;; You can read/write a Card if you can read/write its parent Collection
  i/IObjectPermissions
  perms/IObjectPermissionsForParentCollection

  revision/IRevisioned
  (assoc revision/IRevisionedDefaults
    :serialize-instance serialize-instance)

  dependency/IDependent
  {:dependencies card-dependencies})
