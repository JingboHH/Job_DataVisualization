import os
import pandas as pd

# Initialize an empty DataFrame to combine all CSVs
combined_df = pd.DataFrame()

# Get the current working directory (where the script is located)
current_dir = os.getcwd()

# Walk through all subdirectories and files in the current directory
for subdir, _, files in os.walk(current_dir):
    for file in files:
        if file.endswith(".csv"):  # Check if the file is a CSV
            file_path = os.path.join(subdir, file)  # Get the full file path
            job_name = os.path.basename(subdir)  # Extract folder name
            # Only process folders starting with "GPU"
            if job_name.startswith("GPU"):
                try:
                    # Read the CSV file
                    df = pd.read_csv(file_path)
                    # Add the "Job Name" column
                    df["Job Name"] = job_name
                    # Append the DataFrame to the combined DataFrame
                    combined_df = pd.concat([combined_df, df], ignore_index=True)
                except Exception as e:
                    print(f"Error reading {file_path}: {e}")

# Save the combined DataFrame to a new CSV file
output_file = "integrated_data_with_gpu_job_name.csv"
try:
    combined_df.to_csv(output_file, index=False)
    print(f"Combined data saved to {output_file}")
except Exception as e:
    print(f"Error saving combined data: {e}")
