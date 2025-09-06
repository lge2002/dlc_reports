import os
import json
from glob import glob
from datetime import datetime

# Directories to search for report JSON files
report_dirs = {
    'NRLDC': 'downloads/NRLDC',
    'SRLDC': 'downloads/SRLDC',
    'WRLDC': 'downloads/WRLDC',
    'POSOCO': 'downloads/POSOCO'
}

# Get today's date string in the format used in folder names (YYYY-MM-DD)
today_str = datetime.now().strftime('%Y-%m-%d')


# Collect latest JSON file for each region for today
merged_array = []

for region, report_dir in report_dirs.items():
    latest_json = None
    latest_time = None
    try:
        # Find all subdirs for today
        today_subdirs = [d for d in os.listdir(report_dir) if today_str in d]
        if today_subdirs:
            # Sort subdirs by timestamp in name (assuming format: report_YYYY-MM-DD_HH-MM-SS)
            today_subdirs.sort(reverse=True)
            latest_subdir = today_subdirs[0]
            full_subdir = os.path.join(report_dir, latest_subdir)
            # Find all JSON files in this subdirectory
            json_files = glob(os.path.join(full_subdir, '*.json'))
            if json_files:
                # Sort JSON files by modified time
                json_files.sort(key=lambda x: os.path.getmtime(x), reverse=True)
                latest_json_path = json_files[0]
                try:
                    with open(latest_json_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    merged_array.append(data)
                except Exception as e:
                    print(f"Error reading {latest_json_path}: {e}")
                    # Add a message to the merged array
                    merged_array.append({
                        "region": region,
                        "status": "missing",
                        "message": f"Error reading file: {str(e)}"
                    })
            else:
                print(f"No JSON files found in {full_subdir} for {region}")
                # Add a message to the merged array
                merged_array.append({
                    "region": region,
                    "status": "missing",
                    "message": "No JSON files found."
                })
        else:
            print(f"No subdirs for today in {report_dir} for {region}")
            # Add a message to the merged array
            merged_array.append({
                "region": region,
                "status": "missing",
                "message": "No directory found for today's date."
            })
    except FileNotFoundError:
        print(f"Directory not found: {report_dir}")
        # Add a message to the merged array
        merged_array.append({
            "region": region,
            "status": "missing",
            "message": f"Report directory not found: {report_dir}"
        })

# Create output folder with date and time
output_dir = os.path.join('downloads', 'overall_json')
os.makedirs(output_dir, exist_ok=True)
timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
output_path = os.path.join(output_dir, f'merged_reports_{timestamp}.json')

with open(output_path, 'w', encoding='utf-8') as f:
    json.dump(merged_array, f, indent=2, ensure_ascii=False)

print(f"Merged latest reports for {today_str} into {output_path}")
