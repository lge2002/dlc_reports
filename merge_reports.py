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

# Define empty templates with keys and None values for missing data cases
empty_templates = {
    'NRLDC': {
        "nrldc_table_2A": [{
            "state": None,
            "thermal": None,
            "hydro": None,
            "gas_naptha_diesel": None,
            "solar": None,
            "wind": None,
            "other_biomass_co_gen_etc": None,
            "total": None,
            "drawal_sch": None,
            "act_drawal": None,
            "ui": None,
            "requirement": None,
            "shortage": None,
            "consumption": None
        }],
        "nrldc_table_2C": [{
            "state": None,
            "max_demand": None,
            "time_max": None,
            "shortage_during": None,
            "req_max_demand": None,
            "max_req_day": None,
            "time_max_req": None,
            "shortage_max_req": None,
            "demand_met_max_req": None,
            "min_demand_met": None,
            "time_min_demand": None,
            "ace_max": None,
            "time_ace_max": None,
            "ace_min": None,
            "time_ace_min": None
        }]
    },
    'SRLDC': {
        "srldc_table_2A": [{
            "state": None,
            "thermal": None,
            "hydro": None,
            "gas_naptha_diesel": None,
            "solar": None,
            "wind": None,
            "others": None,
            "net_sch": None,
            "drawal": None,
            "ui": None,
            "availability": None,
            "demand_met": None,
            "shortage": None
        }],
        "srldc_table_2C": [{
            "state": None,
            "max_demand": None,
            "time": None,
            "shortage_max_demand": None,
            "req_max_demand": None,
            "demand_max_req": None,
            "time_max_req": None,
            "shortage_max_req": None,
            "max_req_day": None,
            "ace_min": None,
            "time_ace_min": None,
            "ace_max": None,
            "time_ace_max": None
        }]
    },
    'WRLDC': {
        "wrldc_table_2A": [{
            "state": None,
            "thermal": None,
            "hydro": None,
            "gas": None,
            "wind": None,
            "solar": None,
            "others": None,
            "total": None,
            "net_sch": None,
            "drawal": None,
            "ui": None,
            "availability": None,
            "requirement": None,
            "shortage": None,
            "consumption": None
        }],
        "wrldc_table_2C": [{
            "state": None,
            "max_demand_day": None,
            "time": None,
            "shortage_max_demand": None,
            "req_max_demand": None,
            "ace_max": None,
            "time_ace_max": None,
            "ace_min": None,
            "time_ace_min": None
        }]
    },
    "POSOCO": {
        "posoco_table_a": [
            {
                "demand_evening_peak": None,
                "peak_shortage": None,
                "energy": None,
                "hydro": None,
                "wind": None,
                "solar": None,
                "energy_shortage": None,
                "max_demand_day": None,
                "time_of_max_demand": None
            }
        ],
        "posoco_table_g": [
            {
                "coal": None,
                "lignite": None,
                "hydro": None,
                "nuclear": None,
                "gas_naptha_diesel": None,
                "res_total": None,
                "total": None
        }]
    }
}

today_str = datetime.now().strftime('%Y-%m-%d')
merged_array = []

for region, report_dir in report_dirs.items():
    try:
        today_subdirs = [d for d in os.listdir(report_dir) if today_str in d]
        if today_subdirs:
            today_subdirs.sort(reverse=True)
            latest_subdir = today_subdirs[0]
            full_subdir = os.path.join(report_dir, latest_subdir)
            json_files = glob(os.path.join(full_subdir, '*.json'))
            if json_files:
                json_files.sort(key=lambda x: os.path.getmtime(x), reverse=True)
                latest_json_path = json_files[0]
                try:
                    with open(latest_json_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    merged_array.append(data)
                except Exception as e:
                    print(f"Error reading {latest_json_path}: {e}")
                    # Append empty template if file read fails
                    empty_data = empty_templates.get(region)
                    if empty_data:
                        merged_array.append(empty_data)
                    else:
                        merged_array.append({"region": region, "status": "missing", "message": f"Error reading file: {str(e)}"})
            else:
                print(f"No JSON files found in {full_subdir} for {region}")
                empty_data = empty_templates.get(region)
                if empty_data:
                    merged_array.append(empty_data)
                else:
                    merged_array.append({"region": region, "status": "missing", "message": "No JSON files found."})
        else:
            print(f"No subdirs for today in {report_dir} for {region}")
            empty_data = empty_templates.get(region)
            if empty_data:
                merged_array.append(empty_data)
            else:
                merged_array.append({"region": region, "status": "missing", "message": "No directory found for today's date."})
    except FileNotFoundError:
        print(f"Directory not found: {report_dir}")
        empty_data = empty_templates.get(region)
        if empty_data:
            merged_array.append(empty_data)
        else:
            merged_array.append({"region": region, "status": "missing", "message": f"Report directory not found: {report_dir}"})

# Save merged JSON
output_dir = os.path.join('downloads', 'overall_json')
os.makedirs(output_dir, exist_ok=True)
timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
output_path = os.path.join(output_dir, f'merged_reports_{timestamp}.json')

with open(output_path, 'w', encoding='utf-8') as f:
    json.dump(merged_array, f, indent=2, ensure_ascii=False)

print(f"Merged latest reports for {today_str} into {output_path}")
