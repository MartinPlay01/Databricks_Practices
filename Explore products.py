# Databricks notebook source
# MAGIC %md
# MAGIC ## products.csv Dataset of AdventureWorks
# MAGIC #### Uploading Data from: 
# MAGIC https://raw.githubusercontent.com/MicrosoftLearning/dp-203-azure-data-engineer/master/Allfiles/labs/23/adventureworks/products.csv

# COMMAND ----------

df1 = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/xlab-txz-998@xtremelabs.us/products_1_.csv")

# COMMAND ----------

display(df1)

# COMMAND ----------

df1.write.saveAsTable("products")

# COMMAND ----------

df1.count()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT ProductName, ListPrice
# MAGIC FROM products
# MAGIC WHERE Category = 'Touring Bikes'

# COMMAND ----------

display(df1)

# COMMAND ----------

df1.printSchema()

# COMMAND ----------

df2 = df1.select("Category").distinct()
display(df2)

# COMMAND ----------


from pyspark.sql.functions import col,sum
df3 = df1.select("Category", col("ListPrice").cast("double").alias("ListPrice"))
df4 = df3.groupBy("Category").agg(sum("ListPrice").alias("aggListPrice"))#.agg("ListPrice").count()
display(df4)
