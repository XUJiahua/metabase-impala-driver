(ns metabase.driver.impala
  (:require [clojure
             [set :as set]
             [string :as str]]
            [java-time :as t]
            [clojure.java.jdbc :as jdbc]
            [honeysql
             [core :as hsql]
             [helpers :as h]]
            [metabase.driver :as driver]
            [metabase.driver.sql
             [query-processor :as sql.qp]
             [util :as sql.u]]
            [metabase.driver.sql-jdbc
             [common :as sql-jdbc.common]
             [connection :as sql-jdbc.conn]
             [execute :as sql-jdbc.execute]
             [sync :as sql-jdbc.sync]]
            [metabase.driver.sql.util.unprepare :as unprepare]
            [metabase.mbql.util :as mbql.u]
            [metabase.models.table :refer [Table]]
            [metabase.models.field :refer [Field]]
            [toucan.db :as db]
            [metabase.query-processor
             [store :as qp.store]
             [util :as qputil]]
            [metabase.util.honeysql-extensions :as hx]
            [metabase.util.date-2 :as u.date])
  (:import [java.sql ResultSet Types]
           [java.time LocalDateTime OffsetDateTime ZonedDateTime]))

(driver/register! :impala, :parent :sql-jdbc)

;;; ------------------------------------------ Custom HoneySQL Clause Impls ------------------------------------------

(def ^:private source-table-alias
  "Default alias for all source tables. (Not for source queries; those still use the default SQL QP alias of `source`.)"
  "t1")

;; use `source-table-alias` for the source Table, e.g. `t1.field` instead of the normal `schema.table.field`
(defmethod sql.qp/->honeysql [:impala (class Field)]
  [driver field]
  (binding [sql.qp/*table-alias* (or sql.qp/*table-alias* source-table-alias)]
    ((get-method sql.qp/->honeysql [:sql-jdbc (class Field)]) driver field)))

(defmethod sql.qp/apply-top-level-clause [:impala :page] [_ _ honeysql-form {{:keys [items page]} :page}]
  (let [offset (* (dec page) items)]
    (if (zero? offset)
      ;; if there's no offset we can simply use limit
      (h/limit honeysql-form items)
      ;; if we need to do an offset we have to do nesting to generate a row number and where on that
      (let [over-clause (format "row_number() OVER (%s)"
                                (first (hsql/format (select-keys honeysql-form [:order-by])
                                                    :allow-dashed-names? true
                                                    :quoting :mysql)))]
        (-> (apply h/select (map last (:select honeysql-form)))
            (h/from (h/merge-select honeysql-form [(hsql/raw over-clause) :__rownum__]))
            (h/where [:> :__rownum__ offset])
            (h/limit items))))))

(defmethod sql.qp/apply-top-level-clause [:impala :source-table]
  [driver _ honeysql-form {source-table-id :source-table}]
  (let [{table-name :name, schema :schema} (qp.store/table source-table-id)]
    (h/from honeysql-form [(sql.qp/->honeysql driver (hx/identifier :table schema table-name))
                           (sql.qp/->honeysql driver (hx/identifier :table-alias source-table-alias))])))


;;; ------------------------------------------- Other Driver Method Impls --------------------------------------------

(defn- impala
  "Create a database specification for a Impala database."
  [{:keys [host port db jdbc-flags]
    :or   {host "localhost", port 21050, db "default", jdbc-flags ""}
    :as   opts}]
  (merge
    {:classname   "com.cloudera.impala.jdbc.Driver"
     :subprotocol "impala"
     :subname     (str "//" host ":" port "/" db jdbc-flags)}
    (dissoc opts :host :port :jdbc-flags)))

(defmethod sql-jdbc.conn/connection-details->spec :impala
  [_ details]
  (-> details
      (update :port (fn [port]
                      (if (string? port)
                        (Integer/parseInt port)
                        port)))
      (set/rename-keys {:dbname :db})
      impala
      (sql-jdbc.common/handle-additional-options details)))

(defmethod sql-jdbc.sync/database-type->base-type :impala
  [_ database-type]
  (condp re-matches (name database-type)
    #"TINYINT" :type/Integer
    #"SMALLINT" :type/Integer
    #"INT" :type/Integer
    #"BIGINT" :type/BigInteger
    #"FLOAT" :type/Float
    #"DOUBLE" :type/Float
    #"DECIMAL.*" :type/Decimal
    #"TIMESTAMP" :type/DateTime
    #"STRING.*" :type/Text
    #"VARCHAR.*" :type/Text
    #"CHAR.*" :type/Text
    #"BOOLEAN" :type/Boolean
    #"ARRAY.*" :type/Array
    #"MAP.*" :type/Dictionary
    #".*" :type/*))

(doseq [feature [:basic-aggregations
                 :binning
                 :expression-aggregations
                 :expressions
                 :native-parameters
                 :nested-queries
                 :standard-deviation-aggregations]]
  (defmethod driver/supports? [:impala feature] [_ _] true))

;; only define an implementation for `:foreign-keys` if none exists already. In test extensions we define an alternate
;; implementation, and we don't want to stomp over that if it was loaded already
(when-not (get (methods driver/supports?) [:impala :foreign-keys])
  (defmethod driver/supports? [:impala :foreign-keys] [_ _] true))

(defmethod sql.qp/quote-style :impala [_] :mysql)

;; impala only support TIMESTAMP without zone
;; impala doesn't support "timestamp" keyword from default implementation
(defmethod unprepare/unprepare-value [:impala LocalDateTime]
  [_ t]
  (format "to_timestamp('%s', 'yyyy-MM-dd HH:mm:ss')" (t/format "yyyy-MM-dd HH:mm:ss" t)))

(defmethod unprepare/unprepare-value [:impala OffsetDateTime]
  [_ t]
  (format "to_utc_timestamp('%s', '%s')" (u.date/format-sql (t/local-date-time t)) (t/zone-offset t)))

(defmethod unprepare/unprepare-value [:impala ZonedDateTime]
  [_ t]
  (format "to_utc_timestamp('%s', '%s')" (u.date/format-sql (t/local-date-time t)) (t/zone-id t)))

;; reimplement sql.qp/date
;; ref: https://docs.cloudera.com/documentation/enterprise/6/6.3/topics/impala_datetime_functions.html
;; ref: https://docs.cloudera.com/documentation/enterprise/6/6.3/topics/impala_functions.html
;; impala does not support date_format

;; use from_timestamp instead
(defn- date-format [format-str expr]
  (hsql/call :from_timestamp expr (hx/literal format-str)))

(defn- str-to-date [format-str expr]
  (hx/->timestamp
    (hsql/call :from_unixtime
               (hsql/call :unix_timestamp
                          expr (hx/literal format-str)))))

(defn- trunc-with-format [format-str expr]
  (str-to-date format-str (date-format format-str expr)))

(defmethod sql.qp/date [:impala :default] [_ _ expr] (hx/->timestamp expr))
(defmethod sql.qp/date [:impala :minute] [_ _ expr] (trunc-with-format "yyyy-MM-dd HH:mm" (hx/->timestamp expr)))
(defmethod sql.qp/date [:impala :minute-of-hour] [_ _ expr] (hsql/call :minute (hx/->timestamp expr)))
(defmethod sql.qp/date [:impala :hour] [_ _ expr] (trunc-with-format "yyyy-MM-dd HH" (hx/->timestamp expr)))
(defmethod sql.qp/date [:impala :hour-of-day] [_ _ expr] (hsql/call :hour (hx/->timestamp expr)))
(defmethod sql.qp/date [:impala :day] [_ _ expr] (trunc-with-format "yyyy-MM-dd" (hx/->timestamp expr)))
(defmethod sql.qp/date [:impala :day-of-month] [_ _ expr] (hsql/call :dayofmonth (hx/->timestamp expr)))
(defmethod sql.qp/date [:impala :day-of-year] [_ _ expr] (hsql/call :dayofyear (hx/->timestamp expr)))
(defmethod sql.qp/date [:impala :week-of-year] [_ _ expr] (hsql/call :weekofyear (hx/->timestamp expr)))
(defmethod sql.qp/date [:impala :month] [_ _ expr] (hsql/call :trunc (hx/->timestamp expr) (hx/literal :MM)))
(defmethod sql.qp/date [:impala :month-of-year] [_ _ expr] (hsql/call :month (hx/->timestamp expr)))
; SELECT quarter(now())
;(defmethod sql.qp/date [:impala :quarter-of-year] [_ _ expr] (hsql/call :quarter (hx/->timestamp expr)))
;; impala 2.5 do not support quarter function, use below instead
; SELECT floor((month(trunc(now(), 'Q')) - 1) / 3) + 1
(defmethod sql.qp/date [:impala :quarter-of-year]
  [_ _ expr]
  (hsql/call :+
             (hsql/call :floor
                        (hsql/call :/
                                   (hsql/call :-
                                              (hsql/call :month
                                                         (hsql/call :trunc (hx/->timestamp expr) (hx/literal :Q)))
                                              1) 3)) 1))

(defmethod sql.qp/date [:impala :year] [_ _ expr] (hsql/call :trunc (hx/->timestamp expr) (hx/literal :year)))

(defmethod sql.qp/date [:impala :day-of-week] [_ _ expr] (hsql/call :dayofweek (hx/->timestamp expr)))

(defmethod sql.qp/date [:impala :week]
  [_ _ expr]
  (hsql/call :date_sub
             (hx/+ (hx/->timestamp expr)
                   (hsql/raw "interval 1 day"))
             (hsql/call :dayofweek (hx/->timestamp expr))))

(defmethod sql.qp/date [:impala :quarter]
  [_ _ expr]
  (hsql/call :add_months
             (hsql/call :trunc (hx/->timestamp expr) (hx/literal :year))
             (hx/* (hx/- (hsql/call :quarter (hx/->timestamp expr))
                         1)
                   3)))

(defmethod sql.qp/->honeysql [:impala :replace]
  [driver [_ arg pattern replacement]]
  (hsql/call :regexp_replace
             (sql.qp/->honeysql driver arg)
             (sql.qp/->honeysql driver pattern)
             (sql.qp/->honeysql driver replacement)))

(defmethod sql.qp/->honeysql [:impala :regex-match-first]
  [driver [_ arg pattern]]
  (hsql/call :regexp_extract (sql.qp/->honeysql driver arg) (sql.qp/->honeysql driver pattern)))

(defmethod sql.qp/->honeysql [:impala :median]
  [driver [_ arg]]
  (hsql/call :percentile (sql.qp/->honeysql driver arg) 0.5))

(defmethod sql.qp/->honeysql [:impala :percentile]
  [driver [_ arg p]]
  (hsql/call :percentile (sql.qp/->honeysql driver arg) (sql.qp/->honeysql driver p)))

;; see interval expressions
;; https://docs.cloudera.com/documentation/enterprise/latest/topics/impala_datetime_functions.html#datetime_functions
(defmethod sql.qp/add-interval-honeysql-form :impala
  [_ hsql-form amount unit]
  (hx/+ (hx/->timestamp hsql-form) (hsql/raw (format "INTERVAL %d %s" (int amount) (name unit)))))

;; ignore the schema when producing the identifier
(defn qualified-name-components
  "Return the pieces that represent a path to `field`, of the form `[table-name parent-fields-name* field-name]`."
  [{field-name :name, table-id :table_id}]
  [(db/select-one-field :name Table, :id table-id) field-name])

(defmethod sql.qp/field->identifier :impala
  [_ field]
  (apply hsql/qualify (qualified-name-components field)))

(defmethod sql-jdbc.execute/read-column-thunk [:impala Types/TIMESTAMP]
  [_ ^ResultSet rs rsmeta ^Integer i]
  (fn []
    (when-let [t (.getTimestamp rs i)]
      (t/zoned-date-time (t/local-date-time t) (t/zone-id "UTC")))))
