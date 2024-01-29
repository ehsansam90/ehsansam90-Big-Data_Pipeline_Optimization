import pandas as pd
import time
import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.sql.types import BooleanType



def process_data(input_file, output_file, patients, worker_name):
    print(input_file,patients, "testtt")
    spark = SparkSession.builder.appName("LabEvents").config("spark.executor.memory", "4G").config("spark.executor.cores", "2") \
    .config("spark.executor.instances", "4") \
    .config("spark.driver.memory", "2G").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    

    # Read data using Spark DataFrame
    lab_events = spark.read.csv(input_file, header=True, inferSchema=True)
    patients = spark.read.csv(patients, header=True, inferSchema=True)
    
    # print(patients.columns)
    # print(lab_events.columns)
    start_time = time.time()
    # Merge and transform
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
    #res.show()

    end_time = time.time()

    # Save results to CSV
    container_results_dir = "/app/results"
    os.makedirs(container_results_dir, exist_ok=True)
    container_output_file = os.path.join(container_results_dir, f"{worker_name}.csv")

    # res.toPandas().to_csv(container_output_file, index=False)
    print("Successfully saved results")

    timestamp_file_path = f"/app/timestamps/{worker_name}.txt"
    os.makedirs(os.path.dirname(timestamp_file_path), exist_ok=True)
    with open(timestamp_file_path, 'a') as timestamp_file:
                timestamp_file.write(" "+ str(end_time-start_time))

    #Stop Spark session
    spark.stop()

if __name__ == "__main__":
    input_file = sys.argv[1]
    output_file = sys.argv[2]
    patients = sys.argv[3]
    worker_name = sys.argv[4]
    process_data(input_file, output_file, patients, worker_name)
