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

# Transforming or Cleaning :

# Converting columns names to lowercase and avoid spaces :
df = df.toDF(*[c.lower().strip() for c in df.columns])

# Standardizing emp_id before removing duplicates :
df = df.withColumn("emp_id", regexp_replace("emp_id", r"\D", ""))
df = df.withColumn("emp_id", col("emp_id").cast(IntegerType()))

# Checking duplicates records :
print(df.groupBy("emp_id").count().filter("count > 1").show())

# Dropping duplicates by emp_id :
df = df.dropDuplicates(["emp_id"])

# Cleaning full_name :
df = df.withColumn("full_name", initcap(trim(col("full_name"))))

# Cleaning gender :
df = df.withColumn("gender",
                       when((lower(trim(col("gender")))=="m")|(lower(trim(col("gender")))=="male"), "Male")\
                      .when((lower(trim(col("gender")))=="f")|(lower(trim(col("gender")))=="female"), "Female")\
                      .otherwise("Unknown")
                       )

# Cleaning dob :
def parse_date_udf(val):
    if not val or val.strip() == "":
        return None
    val = val.strip()
    formats = [
         "%Y-%m-%d", "%d-%m-%Y", "%m-%d-%Y",
        "%d %B %Y", "%d %b %Y", "%Y %b %d", "%Y %B %d",
        "%B %d %Y", "%b %d %Y", "%B %d, %Y", "%b %d, %Y",
        "%d/%m/%Y", "%m/%d/%Y", "%Y/%m/%d", "%Y/%d/%m",
        "%d.%m.%Y", "%d %m %Y", "%Y.%m.%d", "%Y %m %d",
        "%d-%b-%Y", "%d-%B-%Y", "%b-%d-%Y", "%B-%d-%Y",
        "%d %B, %Y", "%d %b, %Y", "%d %B %y", "%d %b %y",
        "%m/%d/%y" ,"%y-%m-%d", "%d-%m-%y","%d.%m.%y", "%y.%m.%d", # added 2-digit year US format
    ]
    for fmt in formats:
        try:
            return datetime.strptime(val, fmt).date()
        except ValueError:
            continue
    return None

parse_date_udf = udf(parse_date_udf, DateType())

df = df.withColumn("dob", parse_date_udf(col("dob"))) \
               .withColumn("dob", when(col("dob").isNull(), datetime(1800, 1, 1).date()).otherwise(col("dob")))

# Cleaning join_date :
df = df.withColumn("join_date", parse_date_udf(col("join_date"))) \
               .withColumn("join_date", when(col("join_date").isNull(), datetime(1800, 1, 1).date()).otherwise(col("join_date")))

# Cleaning department :
df = df.withColumn("department",
                   when(((trim(col("department"))== "")|(col("department").isNull())), "Unknown")\
                    .when((lower(trim(col("department")))=="hr"), "Human Resource")\
                    .otherwise(initcap(trim(col("department"))))
                   )

# Cleaning job_title :
df = df.withColumn("job_title",
                   when(((trim(col("job_title"))== "")|(col("job_title").isNull())), "Unknown")\
                   .otherwise(initcap(trim(col("job_title"))))
                   )

# Cleaning manager_id :
df = df.withColumn("manager_id", regexp_replace("manager_id", r"\D", ""))\
                        .withColumn("manager_id",
                                    when((trim(col("manager_id"))=="") | (col("manager_id").isNull()), "NULL").otherwise(trim(col("manager_id")))
                                    )

# Cleaning location :
df = df.withColumn("location",
                    when(((trim(col("location"))== "")|(col("location").isNull())), "Unknown")\
                    .otherwise(initcap(trim(col("location"))))
                    )

# Cleaning status :
df = df.withColumn("status",
                   when(((col("last_working_day").isNotNull()) & (col("status").isNull())), "Abscond")\
                   .otherwise(initcap(trim(col("status"))))
                   )

# Cleaning salary :
df.withColumn("salary",
                  when((col("salary").rlike(r"\D")), regexp_replace("salary", r"\s", "")).otherwise(col("salary"))
                  )

valid_sal_recs = df.filter(~((col("salary").isNull()) | (trim(col("salary"))=="") | (col("salary")<=0)))

dept_avg_sal = valid_sal_recs.groupBy("department", "job_title").agg(round(avg("salary"), 2).alias("avg_salary"))

df = df.join(dept_avg_sal, on=["department", "job_title"], how="left")

df = df.withColumn("salary",
                    when(((col("salary").isNull())|(col("salary")<=0)), col("avg_salary")).otherwise(round(col("salary"),2))
                   ).drop("avg_salary")

# Cleaning email :
df = df.withColumn("email",
                    when(col("email").isNull(), "Unknown")\
                    .when(~col("email").rlike(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"), "Invalid")\
                    .otherwise(lower(trim(col("email"))))
                   )

# Cleaning phone :
df = df.withColumn("phone", regexp_replace(col("phone"), r"x\d+$", ""))
df = df.withColumn("phone", regexp_replace(col("phone"), r"\D", ""))
df = df.withColumn("phone", expr("substring(phone, length(phone) - 9, 10)"))
df = df.withColumn(
    "phone",
     concat(
        substring(col("phone"), 1, 3),
        lit("-"),
        substring(col("phone"), 4, 3),
        lit("-"),
        substring(col("phone"), 7, 4))
    )

# Cleaning employment_type :
df = df.withColumn("employment_type",
                       when(((trim(col("employment_type"))== "")|(col("employment_type").isNull())), "Unknown")\
                      .otherwise(initcap(trim(col("employment_type"))))
                   )

# Cleaning last_working_day :
df = df.withColumn("last_working_day",
                       when(((col("last_working_day").isNull()) | (col("last_working_day")=="")), lit("None"))\
                       .otherwise(trim(col("last_working_day")))
                   )

df = df.withColumn("last_working_day", parse_date_udf(col("last_working_day"))) \
               .withColumn("last_working_day", when(col("last_working_day").isNull(), lit("None")).otherwise(col("last_working_day")))
# ------------------------------------------------------------------------------------------------------------------------------------------

# Re-ordering columns :
# To ensure a consistent schema for downstream processes and data integration.
df = df.select("emp_id", "full_name", "gender", "dob", "join_date", "department", "job_title", "manager_id",\
               "location", "status", "employment_type", "salary", "email", "phone", "last_working_day")

# Previewing Datatypes :
print(df.printSchema())

# Converting datatypes for all columns :
df = df\
    .withColumn("emp_id", col("emp_id").cast(IntegerType()))\
    .withColumn("full_name", col("full_name").cast(StringType()))\
    .withColumn("gender", col("gender").cast(StringType()))\
    .withColumn("dob", to_date(col("dob"), "yyyy-mm-dd"))\
    .withColumn("join_date", to_date(col("join_date"), "yyyy-mm-dd"))\
    .withColumn("department", col("department").cast(StringType()))\
    .withColumn("job_title", col("job_title").cast(StringType()))\
    .withColumn("manager_id", col("manager_id").cast(IntegerType()))\
    .withColumn("location", col("location").cast(StringType()))\
    .withColumn("status", col("status").cast(StringType()))\
    .withColumn("salary", col("salary").cast(FloatType()))\
    .withColumn("email", col("email").cast(StringType()))\
    .withColumn("phone", col("phone").cast(StringType()))\
    .withColumn("employment_type", col("employment_type").cast(StringType()))\
    .withColumn("last_working_day", to_date(col("last_working_day"), "yyyy-mm-dd"))
