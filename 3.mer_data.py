import pandas as pd

# Load the CSV files
inte_data_file = "inte_data.csv"
slurm_metrics_file = "slurm_metrics.csv"

# Read inte_data.csv with the correct delimiter
inte_data = pd.read_csv(inte_data_file, delimiter=";")  # Use the correct delimiter
slurm_metrics = pd.read_csv(slurm_metrics_file)

# Check column names again after correcting the delimiter
print("inte_data columns after delimiter fix:", inte_data.columns)

# Rename columns to ensure consistency
inte_data.rename(columns=lambda x: x.strip(), inplace=True)
slurm_metrics.rename(columns=lambda x: x.strip(), inplace=True)

# Check if "Job ID" exists in both DataFrames
if "Job ID" not in inte_data.columns:
    print("Error: 'Job ID' not found in inte_data.csv")
if "Job ID" not in slurm_metrics.columns:
    print("Error: 'Job ID' not found in slurm_metrics.csv")

# Merge the DataFrames
try:
    merged_data = pd.merge(inte_data, slurm_metrics, on="Job ID", how="left")
    # Save the merged DataFrame to a new CSV file
    output_file = "merged_inte_data.csv"
    merged_data.to_csv(output_file, index=False)
    print(f"Data merged and saved to {output_file}")
except KeyError as e:
    print(f"KeyError during merge: {e}")
