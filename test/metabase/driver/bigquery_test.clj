(ns metabase.driver.bigquery-test
  (:require [expectations :refer :all]
            [metabase
             [driver :as driver]
             [query-processor :as qp]
             [query-processor-test :as qptest]]
            [metabase.driver.bigquery :as bigquery]
            [metabase.models
             [field :refer [Field]]
             [table :refer [Table]]]
            [metabase.query-processor.interface :as qpi]
            [metabase.query-processor.middleware.expand :as ql]
            [metabase.test
             [data :as data]
             [util :as tu]]
            [metabase.test.data.datasets :refer [expect-with-engine do-with-engine]]))

(def ^:private col-defaults
  {:remapped_to nil, :remapped_from nil})

;; Test native queries
(expect-with-engine :bigquery
  [[100]
   [99]]
  (get-in (qp/process-query
            {:native   {:query (str "SELECT [test_data.venues.id] "
                                    "FROM [test_data.venues] "
                                    "ORDER BY [test_data.venues.id] DESC "
                                    "LIMIT 2;")}
             :type     :native
             :database (data/id)})
          [:data :rows]))

;; Test native queries
#_(expect-with-engine :bigquery
  []
  (get-in (qp/process-query
            {:native   {:query (str "SELECT * "
                                    "FROM [test_data.users] ")}
             :type     :native
             :database (data/id)})
          [:data :rows]))
#_(expect-with-engine :bigquery
  [[4 "Simcha Yan" "2014-01-01T08:30:00.000Z"]]
  (tu/with-temporary-setting-values [report-timezone "Europe/Brussels"]
    (-> (data/run-query users
          (ql/filter (ql/between $last_login
                                 "2014-07-02"
                                 "2014-07-03")))
        qptest/rows)))

#_(expect-with-engine :bigquery
  [[4 "Simcha Yan" "2014-01-01T08:30:00.000Z"]]
  (do #_tu/with-temporary-setting-values #_[report-timezone "Europe/Brussels"]
      (with-redefs [clj-time.core/now (constantly (clj-time.core/date-time 2014 07 02 12 0 0))]
        (qp/process-query {:native {:query "SELECT * FROM [test_data.users] WHERE CAST([test_data.users.last_login] AS date) = {{date}};"
                                    :template_tags {:date {:id "foo"
                                                           :name "date"
                                                           :display_name "DATE" :type "date"}}}
                           :type :native
                           :database (data/id)
                           :parameters [{:target ["variable" ["template-tag" "date"]]
                                         :type "date", :value "yesterday"}]}))))



#_(expect-with-engine :bigquery
  [0]
  (with-redefs [clj-time.core/now (constantly (clj-time.core/date-time 2014 07 2 23 0 0))]
    (do #_#_tu/with-temporary-setting-values [report-timezone "Europe/Brussels"]
      (-> (qp/process-query
            {:database (data/id)
             :type :native
             :native     {
                          :query         (format "SELECT COUNT(*) FROM [test_data.users] WHERE {{last_login}}")
                          :template_tags {:last_login {:name "last_login", :display_name "Last Login", :type "dimension", :dimension ["field-id" (data/id :users :last_login)]}}}
             :parameters [{:type "date/relative", :target ["dimension" ["template-tag" "last_login"]], :value "yesterday"}]})
          qptest/rows
          first))))

(expect-with-engine :bigquery
  [0]
  (with-redefs [clj-time.core/now (constantly (clj-time.core/date-time 2014 07 2 23 0 0))]
    (tu/with-temporary-setting-values [report-timezone "America/New_York"]
      (let [#_inner-query
            ;;            outer-query (-> (data/wrap-inner-query inner-query))
            ]
        (data/run-query users
          (ql/filter
           (ql/between (ql/datetime-field $last_login :day)
                       (ql/relative-datetime -2 :day)
                 (ql/relative-datetime -1 :day)))
          (ql/aggregation (ql/count))

          #_              outer-query
          #_{:database   (data/id)
             :type       :query
             :query      (data/query users
                           (ql/aggregation (ql/count)))
             :parameters [{:hash   "abc123"
                           :name   "foo"
                           :type   "date"
                           :target ["dimension" ["field-id" (data/id :users :last_login)]]
                           :value  "yeseterday"}]})))))



;;; table-rows-sample
(expect-with-engine :bigquery
  [[1 "Red Medicine"]
   [2 "Stout Burgers & Beers"]
   [3 "The Apple Pan"]
   [4 "Wurstküche"]
   [5 "Brite Spot Family Restaurant"]]
  (->> (driver/table-rows-sample (Table (data/id :venues))
         [(Field (data/id :venues :id))
          (Field (data/id :venues :name))])
       (sort-by first)
       (take 5)))

;; make sure that BigQuery native queries maintain the column ordering specified in the SQL -- post-processing
;; ordering shouldn't apply (Issue #2821)
(expect-with-engine :bigquery
  {:columns ["venue_id" "user_id" "checkins_id"],
   :cols    (mapv #(merge col-defaults %)
                  [{:name "venue_id",    :display_name "Venue ID",    :base_type :type/Integer}
                   {:name "user_id",     :display_name  "User ID",    :base_type :type/Integer}
                   {:name "checkins_id", :display_name "Checkins ID", :base_type :type/Integer}])}

  (select-keys (:data (qp/process-query
                        {:native   {:query (str "SELECT [test_data.checkins.venue_id] AS [venue_id], "
                                                "       [test_data.checkins.user_id] AS [user_id], "
                                                "       [test_data.checkins.id] AS [checkins_id] "
                                                "FROM [test_data.checkins] "
                                                "LIMIT 2")}
                         :type     :native
                         :database (data/id)}))
               [:cols :columns]))

;; make sure that the bigquery driver can handle named columns with characters that aren't allowed in BQ itself
(expect-with-engine :bigquery
  {:rows    [[113]]
   :columns ["User_ID_Plus_Venue_ID"]}
  (qptest/rows+column-names
    (qp/process-query {:database (data/id)
                       :type     "query"
                       :query    {:source_table (data/id :checkins)
                                  :aggregation  [["named" ["max" ["+" ["field-id" (data/id :checkins :user_id)]
                                                                      ["field-id" (data/id :checkins :venue_id)]]]
                                                  "User ID Plus Venue ID"]]}})))

(defn- aggregation-names [query-map]
  (->> query-map
       :aggregation
       (map :custom-name)))

(defn- pre-alias-aggregations' [query-map]
  (binding [qpi/*driver* (driver/engine->driver :bigquery)]
    (aggregation-names (#'bigquery/pre-alias-aggregations query-map))))

(defn- agg-query-map [aggregations]
  (-> {}
      (ql/source-table 1)
      (ql/aggregation aggregations)))

;; make sure BigQuery can handle two aggregations with the same name (#4089)
(expect
  ["sum" "count" "sum_2" "avg" "sum_3" "min"]
  (pre-alias-aggregations' (agg-query-map [(ql/sum (ql/field-id 2))
                                           (ql/count (ql/field-id 2))
                                           (ql/sum (ql/field-id 2))
                                           (ql/avg (ql/field-id 2))
                                           (ql/sum (ql/field-id 2))
                                           (ql/min (ql/field-id 2))])))

(expect
  ["sum" "count" "sum_2" "avg" "sum_2_2" "min"]
  (pre-alias-aggregations' (agg-query-map [(ql/sum (ql/field-id 2))
                                           (ql/count (ql/field-id 2))
                                           (ql/sum (ql/field-id 2))
                                           (ql/avg (ql/field-id 2))
                                           (assoc (ql/sum (ql/field-id 2)) :custom-name "sum_2")
                                           (ql/min (ql/field-id 2))])))

(expect-with-engine :bigquery
  {:rows [[7929 7929]], :columns ["sum" "sum_2"]}
  (qptest/rows+column-names
    (qp/process-query {:database (data/id)
                       :type     "query"
                       :query    (-> {}
                                     (ql/source-table (data/id :checkins))
                                     (ql/aggregation (ql/sum (ql/field-id (data/id :checkins :user_id)))
                                                     (ql/sum (ql/field-id (data/id :checkins :user_id)))))})))

(expect-with-engine :bigquery
  {:rows [[7929 7929 7929]], :columns ["sum" "sum_2" "sum_3"]}
  (qptest/rows+column-names
    (qp/process-query {:database (data/id)
                       :type     "query"
                       :query    (-> {}
                                     (ql/source-table (data/id :checkins))
                                     (ql/aggregation (ql/sum (ql/field-id (data/id :checkins :user_id)))
                                                     (ql/sum (ql/field-id (data/id :checkins :user_id)))
                                                     (ql/sum (ql/field-id (data/id :checkins :user_id)))))})))

(expect-with-engine :bigquery
  "UTC"
  (tu/db-timezone-id))


;; make sure that BigQuery properly aliases the names generated for Join Tables. It's important to include the name of
;; the dataset along, e.g. `test_data.categories__via__category_id` rather than just
;; `categories__via__category_id`, which is what the other SQL databases do. (#4218)
(expect-with-engine :bigquery
  (str "SELECT count(*) AS [count],"
       " [test_data.categories__via__category_id.name] AS [test_data.categories__via__category_id.name] "
       "FROM [test_data.venues] "
       "LEFT JOIN [test_data.categories] [test_data.categories__via__category_id]"
       " ON [test_data.venues.category_id] = [test_data.categories__via__category_id.id] "
       "GROUP BY [test_data.categories__via__category_id.name] "
       "ORDER BY [test_data.categories__via__category_id.name] ASC")
  ;; normally for test purposes BigQuery doesn't support foreign keys so override the function that checks that and
  ;; make it return `true` so this test proceeds as expected
  (with-redefs [qpi/driver-supports? (constantly true)]
    (tu/with-temp-vals-in-db 'Field (data/id :venues :category_id) {:fk_target_field_id (data/id :categories :id)
                                                                    :special_type       "type/FK"}
      (let [results (qp/process-query
                     {:database (data/id)
                      :type     "query"
                      :query    {:source-table (data/id :venues)
                                 :aggregation  [:count]
                                 :breakout     [[:fk-> (data/id :venues :category_id) (data/id :categories :name)]]}})]
        (get-in results [:data :native_form :query] results)))))
