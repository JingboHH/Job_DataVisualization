import os
import re
import pandas as pd
from collections import Counter

# Define the folder containing the slurm files
slurm_folder = "/Users/jingbohe/model_experiments/slurm_log"

# Initialize a list to store the extracted data
data = []

# Regular expressions to extract the metrics
batch_size_pattern = r"Instantaneous batch size per device\s*=\s*(\d+)"
grad_accum_pattern = r"Gradient Accumulation steps\s*=\s*(\d+)"

# Loop through all slurm files in the folder
for file_name in os.listdir(slurm_folder):
    if file_name.startswith("slurm-") and file_name.endswith(".out"):
        file_path = os.path.join(slurm_folder, file_name)
        try:
            # Extract the Job ID from the file name
            job_id = re.search(r"slurm-(\d+)", file_name).group(1)
            
            # Read the content of the slurm file
            with open(file_path, "r") as f:
                content = f.read()
            
            # Extract all occurrences of the metrics
            batch_sizes = re.findall(batch_size_pattern, content)
            grad_accum_steps = re.findall(grad_accum_pattern, content)
            
            # Find the mode (most common value) for each metric
            batch_size_mode = Counter(batch_sizes).most_common(1)[0][0] if batch_sizes else "N/A"
            grad_accum_mode = Counter(grad_accum_steps).most_common(1)[0][0] if grad_accum_steps else "N/A"
            
            # Append the extracted data
            data.append({
                "Job ID": job_id,
                "Batch Size per Device": batch_size_mode,
                "Gradient Accumulation Steps": grad_accum_mode
            })
        
        except Exception as e:
            print(f"Error processing {file_name}: {e}")

# Convert the data into a DataFrame
df = pd.DataFrame(data)

# Save the results to a CSV file
output_file = "slurmlog_metrics.csv"
df.to_csv(output_file, index=False)

print(f"Metrics extracted and saved to {output_file}")
