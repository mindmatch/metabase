(ns metabase.models.query.permissions-test
  (:require [expectations :refer :all]
            [metabase.api.common :refer [*current-user-permissions-set*]]
            [metabase.models
             [card :as card :refer :all]
             [database :as database]
             [interface :as mi]
             [permissions :as perms]]
            [metabase.models.query.permissions :as query-perms]
            [metabase.query-processor.middleware.expand :as ql]
            [metabase.test.data :as data]
            [metabase.util :as u]
            [toucan.util.test :as tt]))

;;; ---------------------------------------------- Permissions Checking ----------------------------------------------

(expect
  false
  (tt/with-temp Card [card {:dataset_query {:database (data/id), :type "native"}}]
    (binding [*current-user-permissions-set* (delay #{})]
      (mi/can-read? card))))

(expect
  (tt/with-temp Card [card {:dataset_query {:database (data/id), :type "native"}}]
    (binding [*current-user-permissions-set* (delay #{(perms/adhoc-native-query-path (data/id))})]
      (mi/can-read? card))))

;; in order to *write* a native card user should need native readwrite access
(expect
  false
  (tt/with-temp Card [card {:dataset_query {:database (data/id), :type "native"}}]
    (binding [*current-user-permissions-set* (delay #{(perms/adhoc-native-query-path (data/id))})]
      (mi/can-write? card))))

(expect
  (tt/with-temp Card [card {:dataset_query {:database (data/id), :type "native"}}]
    (binding [*current-user-permissions-set* (delay #{(perms/adhoc-native-query-path (data/id))})]
      (mi/can-write? card))))


;;; check permissions sets for queries
;; native read
(defn- native [query]
  {:database 1
   :type     :native
   :native   {:query query}})

(expect
  #{"/db/1/native/read/"}
  (query-perms/perms-set (native "SELECT count(*) FROM toucan_sightings;")))

;; native write
(expect
  #{"/db/1/native/"}
  (query-perms/perms-set (native "SELECT count(*) FROM toucan_sightings;")))


(defn- mbql [query]
  {:database (data/id)
   :type     :query
   :query    query})

;; MBQL w/o JOIN
(expect
  #{(perms/object-path (data/id) "PUBLIC" (data/id :venues))}
  (query-perms/perms-set (mbql (ql/query
                                (ql/source-table (data/id :venues))))))

;; MBQL w/ JOIN
(expect
  #{(perms/object-path (data/id) "PUBLIC" (data/id :checkins))
    (perms/object-path (data/id) "PUBLIC" (data/id :venues))}
  (query-perms/perms-set (mbql (ql/query
                                (ql/source-table (data/id :checkins))
                                (ql/order-by (ql/asc (ql/fk-> (data/id :checkins :venue_id) (data/id :venues :name))))))))

;; MBQL w/ nested MBQL query
(defn- query-with-source-card [card]
  {:database database/virtual-id, :type "query", :query {:source_table (str "card__" (u/get-id card))}})

(expect
  #{(perms/object-path (data/id) "PUBLIC" (data/id :venues))}
  (tt/with-temp Card [card {:dataset_query {:database (data/id)
                                            :type     :query
                                            :query    {:source-table (data/id :venues)}}}]
    (query-perms/perms-set (query-with-source-card card))))

;; MBQL w/ nested MBQL query including a JOIN
(expect
  #{(perms/object-path (data/id) "PUBLIC" (data/id :checkins))
    (perms/object-path (data/id) "PUBLIC" (data/id :users))}
  (tt/with-temp Card [card {:dataset_query {:database (data/id)
                                            :type     :query
                                            :query    {:source-table (data/id :checkins)
                                                       :order-by     [[:asc [:fk-> (data/id :checkins :user_id) (data/id :users :id)]]]}}}]
    (query-perms/perms-set (query-with-source-card card))))

;; MBQL w/ nested NATIVE query
(expect
  #{(perms/adhoc-native-query-path (data/id))}
  (tt/with-temp Card [card {:dataset_query {:database (data/id)
                                            :type     :native
                                            :native   {:query "SELECT * FROM CHECKINS"}}}]
    (query-perms/perms-set (query-with-source-card card))))

;; You should still only need native READ permissions if you want to save a Card based on another Card you can already
;; READ.
(expect
  #{(perms/adhoc-native-query-path (data/id))}
  (tt/with-temp Card [card {:dataset_query {:database (data/id)
                                            :type     :native
                                            :native   {:query "SELECT * FROM CHECKINS"}}}]
    (query-perms/perms-set (query-with-source-card card))))

;; However if you just pass in the same query directly as a `:source-query` you will still require READWRITE
;; permissions to save the query since we can't verify that it belongs to a Card that you can view.
(expect
  #{(perms/adhoc-native-query-path (data/id))}
  (query-perms/perms-set {:database (data/id)
                          :type     :query
                          :query    {:source-query {:native "SELECT * FROM CHECKINS"}}}))

;; invalid/legacy card should return perms for something that doesn't exist so no one gets to see it
(expect
  #{"/db/0/"}
  (query-perms/perms-set (mbql {:filter [:WOW 100 200]})))
