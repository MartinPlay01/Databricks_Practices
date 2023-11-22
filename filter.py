# Databricks notebook source
# filter by a dataframe column
##display(df.filter(col("dialfrom").like("%BE%")))

# COMMAND ----------

# MAGIC %sh
# MAGIC pip install ciso8601
# MAGIC pip install phonenumbers
# MAGIC pip install msal
# MAGIC pip install adal

# COMMAND ----------

import os
import sys
import math
import pyarrow
import re
import ciso8601
import phonenumbers
import msal

from pyspark.sql.types import StringType, IntegerType, StructType, StructField
from pyspark.sql.functions import concat, lit, lower, col, udf, create_map, struct, regexp_replace, expr, when, year, dayofmonth, month, hour, lpad, length, split, to_date, struct, translate, upper, regexp_replace, substring, explode
from pyspark.sql import SparkSession
from datetime import date, datetime, timedelta
from delta.tables import *
from itertools import chain

# COMMAND ----------

import re
import ciso8601
import phonenumbers
import adal
import pandas as pd

from pyspark.sql.types import StringType, IntegerType, StructType
from pyspark.sql.functions import concat, lit, lower, col, udf, create_map, struct, regexp_replace, expr, when, year, dayofmonth, month, hour, lpad, length, split, to_date, struct, translate, upper, regexp_replace, substring
from itertools import chain
from pyspark.sql import SparkSession
from delta.tables import *
from datetime import datetime, date, timedelta

# COMMAND ----------

# MAGIC %md
# MAGIC ### Filtering duplicates records for a column in a table
# MAGIC

# COMMAND ----------

dbutils.fs.ls("/databricks-datasets/wine-quality/winequality-red.csv")


# COMMAND ----------

from pyspark.sql.functions import *
checkCodo = spark.read.csv("/databricks-datasets/wine-quality/winequality-red.csv", header=True, sep=";")
#display(checkCodo)
display(checkCodo.select("*").groupby("pH").count().filter(col("count")>1))
