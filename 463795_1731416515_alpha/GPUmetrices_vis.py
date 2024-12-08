import json
import numpy as np
import matplotlib.pyplot as plt

with open('data.json', 'r') as file: 
    json_data = json.load(file)

acc_mem_used = json_data['acc_mem_used']
acc_temp = json_data['acc_temp']
acc_power = json_data['acc_power']
acc_used = json_data['acc_used']

# Function to plot data for a specific metric and core/accelerator ID
def plot_metric(data, metric, target_id, time_unit="minutes"):
    # Ensure the metric exists in the JSON data
    if metric not in data:
        print(f"Metric '{metric}' not found in the data.")
        return

    # Get the relevant data for the metric
    metric_data = data[metric]
    timestep = int(metric_data["accelerator"]["timestep"])  # Get timestep in seconds
    unit = metric_data["accelerator"]["unit"]["base"]  # Confirm temperature unit (e.g., °C)

    
    # Find the series for the specified target_id
    series = None
    for entry in metric_data["accelerator"]["series"]:
        if entry["id"] == target_id:
            series = entry
            break
    
    if series is None:
        print(f"No data found for ID {target_id} in metric '{metric}'.")
        return

    # Extract data and time points
    values = series["data"]
    time = np.arange(0, len(values) * timestep, timestep)

    # Convert time to minutes if needed
    if time_unit == "minutes":
        time = time / 60
        time_label = "Time (minutes)"
    else:
        time_label = "Time (seconds)"

    # Plotting
    plt.figure(figsize=(20, 8))
    plt.plot(time, values, linestyle='-', label=f"{metric} - GPU {series['id']}")
    plt.xlabel("Time (minutes)")
    plt.ylabel(f"{metric} ({unit})")
    plt.title(f"{metric} over time for id {target_id}")
    plt.legend()
    plt.grid(True)
    plt.show()

plot_metric(json_data, 'acc_mem_used', target_id="4")
plot_metric(json_data, 'acc_mem_used', target_id="5")
plot_metric(json_data, 'acc_mem_used', target_id="6")
plot_metric(json_data, 'acc_mem_used', target_id="7")
