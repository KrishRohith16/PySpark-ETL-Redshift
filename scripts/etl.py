# Importing Modules :
import pandas as pd
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import re
print("imported modules")

# Creating SparkSession :
spark = SparkSession.builder.appName("ETL Pipeline").getOrCreate()
print("Created SparkSession")


# ETL Process Begin :
# *** Extracting inconsistent data :
df = spark.read.csv(r"E:\Data Engineer ETL Project\raw_data\employee_data_raw_inconsistent.csv", header=True, inferSchema=True)
print("Extracted raw dataframe")

#-------------------------------------------------------------------------------------------------------------------------

# Analyzing data before cleaning :

# Viewing Dataframe Structure by limiting 10 rows :
print(df.show(10, truncate=False))

# Counting total number of records in the dataframe :
print(df.count())

#-------------------------------------------------------------------------------------------------------------------------
