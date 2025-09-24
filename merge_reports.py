import os
import json
import requests # Import the requests library
from glob import glob
from datetime import datetime

# --- YOUR EXISTING CODE (UNCHANGED) ---

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
            "state": None, "thermal": None, "hydro": None, "gas_naptha_diesel": None,
            "solar": None, "wind": None, "other_biomass_co_gen_etc": None, "total": None,
            "drawal_sch": None, "act_drawal": None, "ui": None, "requirement": None,
            "shortage": None, "consumption": None
        }],
        "nrldc_table_2C": [{
            "state": None, "max_demand": None, "time_max": None, "shortage_during": None,
            "req_max_demand": None, "max_req_day": None, "time_max_req": None,
            "shortage_max_req": None, "demand_met_max_req": None, "min_demand_met": None,
            "time_min_demand": None, "ace_max": None, "time_ace_max": None, "ace_min": None,
            "time_ace_min": None
        }]
    },
    'SRLDC': {
        "srldc_table_2A": [{
            "state": None, "thermal": None, "hydro": None, "gas_naptha_diesel": None,
            "solar": None, "wind": None, "others": None, "net_sch": None, "drawal": None,
            "ui": None, "availability": None, "demand_met": None, "shortage": None
        }],
        "srldc_table_2C": [{
            "state": None, "max_demand": None, "time": None, "shortage_max_demand": None,
            "req_max_demand": None, "demand_max_req": None, "time_max_req": None,
            "shortage_max_req": None, "max_req_day": None, "ace_min": None,
            "time_ace_min": None, "ace_max": None, "time_ace_max": None
        }]
    },
    'WRLDC': {
        "wrldc_table_2A": [{
            "state": None, "thermal": None, "hydro": None, "gas": None, "wind": None,
            "solar": None, "others": None, "total": None, "net_sch": None, "drawal": None,
            "ui": None, "availability": None, "requirement": None, "shortage": None,
            "consumption": None
        }],
        "wrldc_table_2C": [{
            "state": None, "max_demand_day": None, "time": None, "shortage_max_demand": None,
            "req_max_demand": None, "ace_max": None, "time_ace_max": None, "ace_min": None,
            "time_ace_min": None
        }]
    },
    "POSOCO": {
        "posoco_table_a": [{
            "demand_evening_peak": None, "peak_shortage": None, "energy": None, "hydro": None,
            "wind": None, "solar": None, "energy_shortage": None, "max_demand_day": None,
            "time_of_max_demand": None
        }],
        "posoco_table_g": [{
            "coal": None, "lignite": None, "hydro": None, "nuclear": None,
            "gas_naptha_diesel": None, "res_total": None, "total": None
        }]
    }
}

today_str = datetime.now().strftime('%Y-%m-%d')
merged_data = {}

for region, report_dir in report_dirs.items():
    region_data = None
    try:
        # Use list comprehension for cleaner code
        today_subdirs = [d for d in os.listdir(report_dir) if today_str in d]
        if not today_subdirs:
            print(f"No subdirs for today in {report_dir} for {region}")
            region_data = empty_templates.get(region, {})
        else:
            latest_subdir = sorted(today_subdirs, reverse=True)[0]
            full_subdir = os.path.join(report_dir, latest_subdir)
            json_files = glob(os.path.join(full_subdir, '*.json'))

            if not json_files:
                print(f"No JSON files found in {full_subdir} for {region}")
                region_data = empty_templates.get(region, {})
            else:
                latest_json_path = sorted(json_files, key=os.path.getmtime, reverse=True)[0]
                try:
                    with open(latest_json_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        inner_data = data.get(region, data) # Simplified check
                        
                        template = empty_templates.get(region, {})
                        for table_key, template_value in template.items():
                            table_content = inner_data.get(table_key)
                            if not table_content or not any(table_content):
                                print(f"⚠️ Missing or empty table '{table_key}' for {region}. Applying template.")
                                inner_data[table_key] = template_value
                        
                        region_data = inner_data

                except Exception as e:
                    print(f"Error reading or validating {latest_json_path}: {e}")
                    region_data = empty_templates.get(region, {})

    except (FileNotFoundError, NotADirectoryError):
        print(f"Directory not found or is not a directory: {report_dir}")
        region_data = empty_templates.get(region, {})
    
    merged_data[region] = region_data

# --- UPDATED SECTION: PUSH DATA TO API ---

# 1. Define the base API URL and construct the full URL with today's date
BASE_API_URL = "http://172.16.7.118:8003/api/tamilnadu/wind/api.grid.php"
api_url_with_date = f"{BASE_API_URL}?date={today_str}"

# 2. Set up the request headers. Since no token was mentioned, we only need Content-Type.
headers = {
    "Content-Type": "application/json"
}

# 3. Send the POST request with the merged data
try:
    print(f"\nAttempting to push data to: {api_url_with_date}...")
    
    # The `json` parameter automatically converts the Python dict to a JSON string
    response = requests.post(api_url_with_date, headers=headers, json=merged_data, timeout=30)

    # 4. Check the response from the server
    if response.status_code in [200, 201]:
        print(f"✅ Successfully pushed data to API. Status Code: {response.status_code}")
    else:
        print(f"Failed to push data. Status Code: {response.status_code}")
        print("Error Response:", response.text)

except requests.exceptions.RequestException as e:
    print(f"🚨 An error occurred while trying to connect to the API: {e}")
    print("Please check your network connection and ensure the server at 172.16.7.118 is accessible.")


# --- OPTIONAL: SAVE MERGED JSON LOCALLY ---

save_locally = True # Set to False if you only want to push to the API

if save_locally:
    output_dir = os.path.join('downloads', 'overall_json')
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    output_path = os.path.join(output_dir, f'merged_reports_{timestamp}.json')

    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(merged_data, f, indent=2, ensure_ascii=False)

    print(f"\nMerged latest reports for {today_str} saved to {output_path}")