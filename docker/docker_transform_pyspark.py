import pandas as pd
import time
import sys
import os
from pyspark.sql import SparkSession

def process_data(input_file, output_file, patients, worker_name):
    spark = SparkSession.builder.appName("ProcessData").getOrCreate()

    start_time = time.time()

    # Read data using Spark DataFrame
    lab_events = spark.read.csv(input_file, header=True, inferSchema=True)
    patients = spark.read.csv(patients, header=True, inferSchema=True)

    # Merge and transform
    res = lab_events.join(patients, lab_events["SUBJECT_ID"] == patients["SUBJECT_ID"], "left") \
                    .drop("HADM_ID", "ROW_ID_x", "CHARTTIME", "VALUE", "ROW_ID_y", "DOD_HOSP") \
                    .na.drop(subset='VALUENUM') \
                    .withColumn("DOB", lab_events["DOB"].cast("timestamp")) \
                    .withColumn("DOD", lab_events["DOD"].cast("timestamp")) \
                    .withColumn("DOD_SSN", lab_events["DOD_SSN"].cast("timestamp")) \
                    .withColumn("EXPIRE_FLAG", lab_events["EXPIRE_FLAG"].cast("string").\
                                when(lab_events["EXPIRE_FLAG"] == 1, "Yes").otherwise("No"))

    end_time = time.time()

    # Save results to CSV
    container_results_dir = "/app/results"
    os.makedirs(container_results_dir, exist_ok=True)
    container_output_file = os.path.join(container_results_dir, f"{worker_name}.csv")

    res.toPandas().to_csv(container_output_file, index=False)
    print("Successfully saved results")

    timestamp_file_path = f"/app/timestamps/{worker_name}.txt"
    os.makedirs(os.path.dirname(timestamp_file_path), exist_ok=True)
    with open(timestamp_file_path, 'w') as timestamp_file:
        timestamp_file.write(str(end_time - start_time))

    # Stop Spark session
    spark.stop()

if __name__ == "__main__":
    input_file = sys.argv[1]
    output_file = sys.argv[2]
    patients = sys.argv[3]
    worker_name = sys.argv[4]
    process_data(input_file, output_file, patients, worker_name)
