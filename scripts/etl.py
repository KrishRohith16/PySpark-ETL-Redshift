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
