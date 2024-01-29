import pandas as pd
import time

results_df = pd.DataFrame(columns=['n', 'execution_time'])
sequence = [100000,2500000,5000000,7500000,10000000,12500000,15000000,17500000,20000000]
for n in sequence:
    start_time = time.time()
    print(n)

    # Read data
    lab_events = pd.read_csv("D:/Spring 2024/BDHS/Project/data/LABEVENTS.csv/LABEVENTS.csv", nrows=n)
    patients = pd.read_csv("D:/Spring 2024/BDHS/Project/data/PATIENTS.csv/PATIENTS.csv")

    # Merge and transform
    res = pd.merge(lab_events, patients, on="SUBJECT_ID", how="left")
    res = res.drop(columns=['HADM_ID', 'ROW_ID_x', 'CHARTTIME', 'VALUE', 'ROW_ID_y', 'DOD_HOSP'])
    res.dropna(subset='VALUENUM')
    res['DOB'] = pd.to_datetime(res['DOB'])
    res['DOD'] = pd.to_datetime(res['DOD'])
    res['DOD_SSN'] = pd.to_datetime(res['DOD_SSN'])
    res['EXPIRE_FLAG'] = res['EXPIRE_FLAG'].replace({1: "Yes", 0: "No"})

    end_time = time.time()

    # Add results to the DataFrame
    results_df = pd.concat([results_df, pd.DataFrame({'n': [n], 'execution_time': [end_time - start_time]})], ignore_index=True)

# Save results to CSV
results_df.to_csv("pandas_results.csv", index=False)

# Print the results DataFrame
print(results_df)



