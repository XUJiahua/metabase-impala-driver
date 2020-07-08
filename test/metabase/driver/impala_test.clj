(ns metabase.driver.impala-test
  (:require [clojure.test :refer :all]
            [java-time :as t]
            [metabase.driver.impala :refer :all]
            [metabase.driver.sql.util.unprepare :as unprepare]
            [metabase.driver.sql-jdbc.execute :as sql-jdbc.execute])
  (:import (java.sql PreparedStatement DriverManager)))

(deftest a-test
  (testing "FIXME, I fail."
    (is (= 1 1))))

;(run-tests 'metabase.driver.impala-test)

(unprepare/unprepare-value :impala (t/local-date))

(unprepare/unprepare-value :impala (t/local-date-time))

(def conn (DriverManager/getConnection "jdbc:impala://localhost:21050/"))

(Class/forName "com.cloudera.impala.jdbc.Driver")

(sql-jdbc.execute/set-parameter :impala (.prepareStatement conn "select ?") 1 (t/local-date))
