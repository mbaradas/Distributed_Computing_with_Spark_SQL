-- Assignment_2 _Spark_Internals.sql
-- Databricks notebook source
-- MAGIC 
-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Spark Internals
-- MAGIC ## Module 2 Assignment
-- MAGIC 
-- MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) In this assignment you:
-- MAGIC * Analyze the effects of caching data
-- MAGIC * Speed up Spark queries by changing default configurations
-- MAGIC 
-- MAGIC For each **bold** question, input its answer in Coursera.

-- COMMAND ----------

-- MAGIC %run ../Includes/Classroom-Setup

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Make sure nothing is already cached by clearing the cache (**NOTE**: This will affect all users on this cluster).

-- COMMAND ----------

CLEAR CACHE

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Effects of Caching

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Count the number of records in our `fireCalls` table.
-- MAGIC 
-- MAGIC ### Question 1
-- MAGIC 
-- MAGIC **How many fire calls are in our table?**

-- COMMAND ----------

SELECT COUNT(*)
FROM fireCalls

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Now speed up your query by caching the data, then counting!

-- COMMAND ----------

CACHE TABLE fireCalls

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Look in the Spark UI, how many partitions is our data?

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## ![Spark Logo Tiny](https://files.training.databricks.com/images/105/logo_spark_tiny.png) Changing Spark Configurations
-- MAGIC 
-- MAGIC We cached the data to speed up our computation, but let's see if we can get it even faster by changing some default Spark Configurations.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Let's count the number of each type of unit. Group by the `Unit Type`, count the number of calls for each type, and display the unit types with the most calls first.
-- MAGIC 
-- MAGIC 
-- MAGIC ### Question 2
-- MAGIC 
-- MAGIC **Which "Unit Type" is the most common?**

-- COMMAND ----------

DESCRIBE fireCalls

-- COMMAND ----------

SELECT *
FROM fireCalls
LIMIT 10

-- COMMAND ----------

SELECT `Unit Type`,
  COUNT(`Unit Type`) AS count_unit_type
FROM fireCalls
GROUP BY `Unit Type`
ORDER BY count_unit_type DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Question 3
-- MAGIC 
-- MAGIC **What type of transformation, wide or narrow, did the `GROUP BY` and `ORDER BY` queries result in? **
-- MAGIC 
-- MAGIC Let's change the `spark.sql.shuffle.partitions` configuration from its default value of `200` and set it to `2`.

-- COMMAND ----------

SET spark.sql.shuffle.partitions=2

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Copy and run the code from earlier to get the unit type counts from earlier. Now how long does this query take?

-- COMMAND ----------

SELECT `Unit Type`,
  COUNT(`Unit Type`) AS count_unit_type
FROM fireCalls
GROUP BY `Unit Type`
ORDER BY count_unit_type DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Why was it faster? Check the 2 stages in the UI and see how many tasks were launched. Which number of tasks corresponds to your shuffle partition number?
-- MAGIC 
-- MAGIC 
-- MAGIC ### Question 4
-- MAGIC 
-- MAGIC **How many tasks were in the last stage of the last job?**

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
