import json
import csv
import os
import re
from datetime import datetime

# Function to format bytes into GB with two decimal places
def format_bytes_gb(size_in_bytes):
    if size_in_bytes is None:
        return 'N/A'
    size_in_gb = size_in_bytes / (1024 ** 3)
    return f"{size_in_gb:.2f} GB"

# Function to format bandwidth bytes into GB/s with two decimal places
def format_bandwidth_gbs(bandwidth_in_bytes_per_sec):
    if bandwidth_in_bytes_per_sec is None:
        return 'N/A'
    bandwidth_in_gbs = bandwidth_in_bytes_per_sec / (1024 ** 3)
    return f"{bandwidth_in_gbs:.2f} GB/s"

# Function to format floating-point numbers to two decimal places
def format_float(value):
    if value is None or value == 'N/A':
        return 'N/A'
    return f"{float(value):.2f}"

# Function to format percentages to two decimal places
def format_percentage(value):
    if value is None or value == 'N/A':
        return 'N/A'
    return f"{float(value) * 100:.2f}%"

# Function to format durations into HH:MM:SS
def format_duration(seconds):
    if seconds is None:
        return 'N/A'
    hours = int(seconds) // 3600
    minutes = (int(seconds) % 3600) // 60
    sec = int(seconds) % 60
    return f"{hours:02}:{minutes:02}:{sec:02}"

# Function to convert UNIX timestamp to human-readable format
def format_timestamp(timestamp):
    if timestamp is None:
        return 'N/A'
    return datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')

# Function to extract data from a single meta.json file
def extract_data(file_path):
    # Load JSON data from the file
    with open(file_path, 'r') as f:
        data = json.load(f)
    
    # Initialize a dictionary to hold all the metrics
    metrics = {}
    # Job Overview
    metrics['Job ID'] = data.get('jobId')
    metrics['Job Name'] = data.get('metaData', {}).get('jobName')
    metrics['User'] = data.get('user')
    metrics['Project'] = data.get('project')
    metrics['Cluster'] = data.get('cluster')
    metrics['Partition'] = data.get('partition')
    metrics['Job State'] = data.get('jobState')
    metrics['Start Time'] = format_timestamp(data.get('startTime'))
    metrics['Duration'] = format_duration(data.get('duration'))
    metrics['Walltime Requested'] = format_duration(data.get('walltime'))
    
    # Resource Allocation
    resources = data.get('resources', [{}])[0]
    metaData = data.get('metaData', {})
    jobScript = metaData.get('jobScript', '')
    
    # Extract values from jobScript using regular expressions
    cpus_per_task = re.search(r'#SBATCH --cpus-per-task=(\d+)', jobScript)
    cpus_per_task = int(cpus_per_task.group(1)) if cpus_per_task else 'N/A'
    
    ntasks_per_node = re.search(r'#SBATCH --ntasks-per-node=(\d+)', jobScript)
    ntasks_per_node = int(ntasks_per_node.group(1)) if ntasks_per_node else 'N/A'
    
    mem_per_cpu = re.search(r'#SBATCH --mem-per-cpu=(\d+)([A-Za-z]+)', jobScript)
    if mem_per_cpu:
        mem_per_cpu_value = int(mem_per_cpu.group(1))
        mem_per_cpu_unit = mem_per_cpu.group(2)
    else:
        mem_per_cpu_value = None
        mem_per_cpu_unit = None
    
    if mem_per_cpu_value and mem_per_cpu_unit:
        unit_power = {'K': 1, 'M':2, 'G':3, 'T':4}
        mem_per_cpu_bytes = mem_per_cpu_value * (1024 ** unit_power.get(mem_per_cpu_unit[0].upper(), 0))
    else:
        mem_per_cpu_bytes = None
    
    total_memory = mem_per_cpu_value * data.get('numHwthreads') if mem_per_cpu_value else None
    
    gres_gpu = re.search(r'#SBATCH --gres=gpu:([a-zA-Z0-9:]+)', jobScript)
    if gres_gpu:
        gpu_info = gres_gpu.group(1)
        if ':' in gpu_info:
            # Handles cases like gpu:tesla:2
            gpu_type, gpu_count = gpu_info.split(':')[0], int(gpu_info.split(':')[1])
        else:
            gpu_count = int(gpu_info)
    else:
        gpu_count = data.get('numAcc', 'N/A')
    
    metrics['Nodes Used'] = data.get('numNodes')
    metrics['Node Hostnames'] = resources.get('hostname', 'N/A')
    metrics['Hardware Threads (CPUs)'] = data.get('numHwthreads')
    metrics['CPUs per Task'] = cpus_per_task
    metrics['Tasks per Node'] = ntasks_per_node
    metrics['Total CPUs Allocated'] = data.get('numHwthreads')
    if mem_per_cpu_value and mem_per_cpu_unit:
        metrics['Memory per CPU'] = f"{mem_per_cpu_value} {mem_per_cpu_unit}"
        total_memory_bytes = total_memory * (1024 ** {'K':1, 'M':2, 'G':3, 'T':4}.get(mem_per_cpu_unit[0].upper(), 0))
        metrics['Total Memory Allocated'] = format_bytes_gb(total_memory_bytes)
    else:
        metrics['Memory per CPU'] = 'N/A'
        metrics['Total Memory Allocated'] = 'N/A'
    metrics['GPUs Allocated'] = gpu_count
    accelerators = resources.get('accelerators', [])
    metrics['Accelerators'] = ', '.join(map(str, accelerators)) if accelerators else 'N/A'
    
    # Performance Metrics (average values only)
    statistics = data.get('statistics', {})
    
    metrics['CPU Load Avg'] = format_float(statistics.get('cpu_used', {}).get('avg', 'N/A'))
    metrics['IPC Avg'] = format_float(statistics.get('ipc', {}).get('avg', 'N/A'))
    metrics['FLOPS Avg'] = format_float(statistics.get('flops_any', {}).get('avg', 'N/A'))
    
    mem_bw_avg = statistics.get('mem_bw', {}).get('avg')
    metrics['Memory Bandwidth Avg'] = format_bandwidth_gbs(mem_bw_avg)
    
    net_bw_avg = statistics.get('net_bw', {}).get('avg')
    metrics['Network Bandwidth Avg'] = format_bandwidth_gbs(net_bw_avg)
    
    mem_used_avg = statistics.get('mem_used', {}).get('avg')
    metrics['Memory Used Avg'] = format_bytes_gb(mem_used_avg) if mem_used_avg else 'N/A'
    
    metrics['GPU Utilization Avg'] = format_percentage(statistics.get('acc_used', {}).get('avg', 'N/A'))
    
    acc_mem_used_avg = statistics.get('acc_mem_used', {}).get('avg')
    metrics['GPU Memory Used Avg'] = format_bytes_gb(acc_mem_used_avg) if acc_mem_used_avg else 'N/A'
    
    metrics['GPU Power Avg'] = format_float(statistics.get('acc_power', {}).get('avg', 'N/A'))
    metrics['GPU Temperature Avg'] = format_float(statistics.get('acc_temp', {}).get('avg', 'N/A'))
    
    return metrics

def main():
    import argparse
    import glob

    # Parse command-line arguments
    parser = argparse.ArgumentParser(description='Extract job metrics from meta.json files and output to a CSV file.')
    parser.add_argument('parent_directory', help='Parent directory containing job folders with meta.json files.')
    parser.add_argument('-o', '--output', default='job_metrics.csv', help='Output CSV file name.')
    args = parser.parse_args()

    parent_directory = args.parent_directory
    output_csv = args.output

    # Find all meta.json files in subdirectories
    meta_files = []
    for root, dirs, files in os.walk(parent_directory):
        for file in files:
            if file == 'meta.json':
                meta_files.append(os.path.join(root, file))

    if not meta_files:
        print("No meta.json files found in the specified directory.")
        return

    # List to hold all extracted metrics
    all_metrics = []

    # Extract metrics from each meta.json file
    for meta_file in meta_files:
        try:
            metrics = extract_data(meta_file)
            all_metrics.append(metrics)
        except Exception as e:
            print(f"Error processing {meta_file}: {e}")

    if not all_metrics:
        print("No metrics extracted from meta.json files.")
        return

    # Get all field names (headers) from the first metrics dictionary
    fieldnames = list(all_metrics[0].keys())

    # Write all metrics to the CSV file
    with open(output_csv, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        writer.writeheader()
        for metrics in all_metrics:
            writer.writerow(metrics)
    
    print(f"Metrics have been written to {output_csv}")

if __name__ == "__main__":
    main()
