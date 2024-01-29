from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.sql.types import BooleanType
import time
import pandas as pd

# Create a Spark session
spark = SparkSession.builder.appName("LabEvents").config("spark.executor.memory", "4G").config("spark.executor.cores", "2") \
    .config("spark.executor.instances", "4") \
    .config("spark.driver.memory", "2G").getOrCreate()

# Record start time

results_list = []

# Read LABEVENTS.csv and PATIENTS.csv
sequence = [100000,2500000,5000000,7500000,10000000,12500000,15000000,17500000,20000000]
for n in sequence:
    
    lab_events = spark.read.csv("D:/Spring 2024/BDHS/Project/data/LABEVENTS.csv/LABEVENTS.csv", header=True).limit(n)
    patients = spark.read.csv("D:/Spring 2024/BDHS/Project/data/PATIENTS.csv/PATIENTS.csv", header=True, inferSchema=True)
    
    start_time = time.time()

    # Perform left join on SUBJECT_ID
    res = lab_events.join(patients, lab_events.SUBJECT_ID == patients.SUBJECT_ID, "left")

    # Transformation
    res = res.drop("HADM_ID", "ROW_ID_x", "CHARTTIME", "VALUE", "ROW_ID_y", "DOD_HOSP")
    res = res.dropna(subset="VALUENUM")

    res = res.withColumn("DOB", res["DOB"].cast("timestamp"))
    res = res.withColumn("DOD", res["DOD"].cast("timestamp"))
    res = res.withColumn("DOD_SSN", res["DOD_SSN"].cast("timestamp"))

    # Replace values in the 'EXPIRE_FLAG' column
    res = res.withColumn("EXPIRE_FLAG", col("EXPIRE_FLAG").cast(BooleanType()))
    res = res.withColumn("EXPIRE_FLAG", when(col("EXPIRE_FLAG") == 1, "Yes").otherwise("No"))

    # Show the resulting DataFrame
    res.show()

    # Record end time
    end_time = time.time()

    # Print execution time
    print("Execution Time: {} seconds".format(end_time - start_time))
    results_list.append({'n': n, 'execution_time': end_time - start_time})

results_df = pd.DataFrame(results_list)

results_df.to_csv("pyspark_results.csv", index=False)

