import os
import json
import re  # <-- ADDED for date extraction
import requests
from glob import glob
from datetime import datetime, timedelta

# --- HELPER FUNCTION TO EXTRACT DATE ---
def extract_date_from_filename(filename):
    """
    Extracts a date from a string by trying multiple common formats.
    Returns the date as a 'YYYY-MM-DD' string or None.
    """
    # Pattern: (regex, (year_group, month_group, day_group))
    patterns = [
        (r'(\d{4})-(\d{2})-(\d{2})', (1, 2, 3)),  # YYYY-MM-DD
        (r'(\d{2})-(\d{2})-(\d{4})', (3, 2, 1)),  # DD-MM-YYYY
        (r'daily(\d{2})(\d{2})(\d{2})', (3, 2, 1)), # dailyDDMMYY
        (r'(\d{2})(\d{2})(\d{4})', (3, 2, 1)),    # DDMMYYYY
    ]
    for pattern, (y_idx, m_idx, d_idx) in patterns:
        match = re.search(pattern, filename)
        if match:
            try:
                day = match.group(d_idx)
                month = match.group(m_idx)
                year = match.group(y_idx)
                if len(year) == 2:
                    year = f'20{year}'
                # Validate the date
                datetime(int(year), int(month), int(day))
                return f'{year}-{month}-{day}'
            except (ValueError, IndexError):
                continue
    return None

# --- YOUR EXISTING CODE (WITH UPDATES) ---

# Directories to search for report JSON files
report_dirs = {
    'NRLDC': 'downloads/NRLDC',
    'SRLDC': 'downloads/SRLDC',
    'WRLDC': 'downloads/WRLDC',
    'POSOCO': 'downloads/POSOCO'
}

# UPDATED: Reordered templates to have 'date' as the first key
empty_templates = {
    'NRLDC': {
        "date": None,
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
        "date": None,
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
        "date": None,
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
        "date": None,
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

# --- CORRECTED DATE LOGIC ---
# The script will search for yesterday's reports
target_date_str = (datetime.now()).strftime('%Y-%m-%d')
pdf_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
merged_data = {}

for region, report_dir in report_dirs.items():
    region_data = None
    try:
        # Search for subdirectories matching yesterday's date
        today_subdirs = [d for d in os.listdir(report_dir) if target_date_str in d]
        if not today_subdirs:
            print(f"No subdirs for {target_date_str} in {report_dir} for {region}")
            region_data = empty_templates.get(region, {})
            region_data['date'] = pdf_date # Use yesterday's date for empty reports
        else:
            latest_subdir = sorted(today_subdirs, reverse=True)[0]
            full_subdir = os.path.join(report_dir, latest_subdir)
            json_files = glob(os.path.join(full_subdir, '*.json'))

            if not json_files:
                print(f"No JSON files found in {full_subdir} for {region}")
                region_data = empty_templates.get(region, {})
                region_data['date'] = pdf_date # Use yesterday's date for empty reports
            else:
                latest_json_path = sorted(json_files, key=os.path.getmtime, reverse=True)[0]

                # --- NEW: Date Finding Logic ---
                report_date = None
                pdf_files = glob(os.path.join(full_subdir, '*.pdf'))
                if pdf_files:
                    # Sort PDFs by modification time (newest first) to get the latest one
                    latest_pdf_path = sorted(pdf_files, key=os.path.getmtime, reverse=True)[0]
                    pdf_name = os.path.basename(latest_pdf_path)
                    report_date = extract_date_from_filename(pdf_name)
                    print(f"Found latest PDF '{pdf_name}', extracted date: {report_date}")

                if not report_date:
                    report_date = extract_date_from_filename(latest_subdir)
                    print(f"No date from PDF, trying subdir '{latest_subdir}', extracted date: {report_date}")

                if not report_date:
                    report_date = target_date_str
                    print(f"No date found, using fallback date: {report_date}")
                # --- END of Date Finding Logic ---

                try:
                    with open(latest_json_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        inner_data = data.get(region, data)

                        # Restructure the dictionary to put 'date' first
                        restructured_data = {'date': report_date, **inner_data}

                        template = empty_templates.get(region, {})
                        for table_key, template_value in template.items():
                            if table_key == 'date':
                                continue

                            table_content = restructured_data.get(table_key)
                            if not table_content or not any(table_content):
                                print(f"⚠️ Missing or empty table '{table_key}' for {region}. Applying template.")
                                restructured_data[table_key] = template_value

                        region_data = restructured_data

                except Exception as e:
                    print(f"Error reading or validating {latest_json_path}: {e}")
                    region_data = empty_templates.get(region, {})
                    region_data['date'] = pdf_date # Use yesterday's date for empty reports

    except (FileNotFoundError, NotADirectoryError):
        print(f"Directory not found or is not a directory: {report_dir}")
        region_data = empty_templates.get(region, {})
        region_data['date'] = pdf_date # Use yesterday's date for empty reports

    merged_data[region] = region_data

# --- PUSH DATA TO API (UNCHANGED) ---
BASE_API_URL = "http://172.16.7.118:8003/api/tamilnadu/wind/api.grid.php"
api_url_with_date = f"{BASE_API_URL}?date={pdf_date}"
print("this is ur api_url_with_date:", api_url_with_date)

headers = {
    "Content-Type": "application/json"
}

try:
    print(f"\nAttempting to push data to: {api_url_with_date}...")
    response = requests.post(api_url_with_date, headers=headers, json=merged_data, timeout=30)

    if response.status_code in [200, 201]:
        print(f"✅ Successfully pushed data to API. Status Code: {response.status_code}")
    else:
        print(f"Failed to push data. Status Code: {response.status_code}")
        print("Error Response:", response.text)

except requests.exceptions.RequestException as e:
    print(f"🚨 An error occurred while trying to connect to the API: {e}")
    print("Please check your network connection and ensure the server at 172.16.7.118 is accessible.")

# --- SAVE MERGED JSON LOCALLY (UNCHANGED) ---
save_locally = True
if save_locally:
    output_dir = os.path.join('downloads', 'overall_json')
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    output_path = os.path.join(output_dir, f'merged_reports_{timestamp}.json')

    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(merged_data, f, indent=2, ensure_ascii=False)

    print(f"\nMerged latest reports for {pdf_date} saved to {output_path}")