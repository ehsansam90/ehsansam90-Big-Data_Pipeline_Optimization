import pandas as pd
import time
import sys
import os

def process_data(input_file, output_file,patients,worker_name):
    results_df = pd.DataFrame(columns=['n', 'execution_time'])
    
    start_time = time.time()

    # Read data
    lab_events = pd.read_csv(input_file)
    patients = pd.read_csv(patients)

    # Merge and transform
    res = pd.merge(lab_events, patients, on="SUBJECT_ID", how="left")
    res = res.drop(columns=['HADM_ID', 'ROW_ID_x', 'CHARTTIME', 'VALUE', 'ROW_ID_y', 'DOD_HOSP'])
    res.dropna(subset='VALUENUM',inplace=True)
    res['DOB'] = pd.to_datetime(res['DOB'])
    res['DOD'] = pd.to_datetime(res['DOD'])
    res['DOD_SSN'] = pd.to_datetime(res['DOD_SSN'])
    res['EXPIRE_FLAG'] = res['EXPIRE_FLAG'].replace({1: "Yes", 0: "No"})

    end_time = time.time()

    # Add results to the DataFrame
    # results_df = pd.concat([results_df, pd.DataFrame({'n': [10], 'execution_time': [end_time - start_time]})], ignore_index=True)

    # # Save results to CSV
    # results_df.to_csv(output_file, index=False)
    container_results_dir = "/app/results"
    os.makedirs(container_results_dir, exist_ok=True)
    container_output_file = os.path.join(container_results_dir, f"{worker_name}.csv")
    
    
    print(res.head())       
    res.to_csv(container_output_file, index=False)
    print("successfully saved results")
    
    timestamp_file_path = f"/app/timestamps/{worker_name}.txt"
    os.makedirs(os.path.dirname(timestamp_file_path), exist_ok=True)
    with open(timestamp_file_path, 'w') as timestamp_file:
                timestamp_file.write(" "+ str(end_time-start_time))
        
    

    # Print the results DataFrame
    

if __name__ == "__main__":
    input_file = sys.argv[1]
    output_file = sys.argv[2]
    patients = sys.argv[3]
    worker_name = sys.argv[4]
    process_data(input_file, output_file, patients, worker_name)
