// Databricks notebook source
// MAGIC %sh
// MAGIC pwd
// MAGIC ls
// MAGIC date

// COMMAND ----------

// MAGIC %md
// MAGIC ## Read the data and save it in delta format
// MAGIC

// COMMAND ----------

// Assuming SparkSession is already created as `spark`

// Create a sample DataFrame
val data = Seq(("John", 25), ("Alice", 30), ("Bob", 35))
val df = spark.createDataFrame(data).toDF("name", "age")


// Save DataFrame as a Delta table in delta format
df.write.format("delta").save("/delta_table")
// Show DataFrame
df.show()


// COMMAND ----------

// MAGIC %md
// MAGIC ## Read Delta table into a DataFrame from file
// MAGIC

// COMMAND ----------

// Read Delta table into a DataFrame
val deltaDF = spark.read.format("delta").load("/delta_table")

// Read data from CSV from metastore to Spark DataFrame
val hiveDF = spark.table("hive_metastore.default.sample_data")

// Show DataFrame
hiveDF.show()

// COMMAND ----------

// MAGIC %md
// MAGIC ##Get data from SQL

// COMMAND ----------

// Retrieve data from the table using SQL syntax where the name contains less than 5 characters.
val filteredDF = spark.sql("SELECT id, name, age FROM max_test.default.sample_data WHERE LENGTH(name) < 5")

// Show DataFrame
filteredDF.show()

// COMMAND ----------

// MAGIC %md
// MAGIC
