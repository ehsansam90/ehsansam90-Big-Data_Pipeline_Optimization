import pandas as pd
import os

def split_csv(input_file, output_dir, num_threads):
    # Read the original CSV file
    sequence = [100000,2500000,5000000,7500000,10000000,12500000,15000000,17500000,20000000]

    for n in sequence:
        df = pd.read_csv(input_file, nrows=n)

        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)

        # Calculate the number of rows per partition
        rows_per_partition = len(df) // num_threads

        for thread_id in range(num_threads):
            # Calculate start and end indices for each partition
            start_idx = thread_id * rows_per_partition
            end_idx = start_idx + rows_per_partition

            # Extract the partition
            partition_df = df.iloc[start_idx:end_idx]
            output_path = os.path.join(output_dir,str(n))

            if not os.path.exists(output_path):
                os.makedirs(output_path)

            # Write the partition to a new CSV file
            output_file = os.path.join(output_path, f"{os.path.splitext(os.path.basename(input_file))[0]}_thread_{thread_id + 1}.csv")
            partition_df.to_csv(output_file, index=False)
            

if __name__ == "__main__":
    # Specify the input CSV file, output directory, and number of threads
    input_csv = "D:/Spring 2024/BDHS/Project/data/LABEVENTS.csv/LABEVENTS.csv"
    output_directory = "lab_event_spllited_several_4/"
    num_threads = 4

    # Split the CSV file into partitions
    split_csv(input_csv, output_directory, num_threads)
