# merger/management/commands/merge_reports.py

import os
import json
import re 
import requests
from glob import glob
from datetime import datetime, timedelta

# NEW: Import the necessary Django classes
from django.core.management.base import BaseCommand, CommandError

# Your original functions and data structures remain unchanged
def extract_date_from_filename(filename):
    patterns = [
        (r'(\d{4})-(\d{2})-(\d{2})', (1, 2, 3)), 
        (r'(\d{2})-(\d{2})-(\d{4})', (3, 2, 1)), 
        (r'daily(\d{2})(\d{2})(\d{2})', (3, 2, 1)), 
        (r'(\d{2})(\d{2})(\d{4})', (3, 2, 1)),    
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
                datetime(int(year), int(month), int(day))
                return f'{year}-{month}-{day}'
            except (ValueError, IndexError):
                continue
    return None

report_dirs = {
    'NRLDC': 'downloads/NRLDC',
    'SRLDC': 'downloads/SRLDC',
    'WRLDC': 'downloads/WRLDC',
    'POSOCO': 'downloads/POSOCO'
}

empty_templates = {
    # This dictionary is exactly the same as in your script
    'NRLDC': {
        "date": None,
        "nrldc_table_2A": [
            {"state": s["state"], "thermal": None, "hydro": None, "gas_naptha_diesel": None, "solar": None, "wind": None, "other_biomass": None, "total": None, "drawal_sch": None, "act_drawal": None, "ui": None, "requirement": None, "shortage": None, "consumption": None}
            for s in [
                {"state": "PUNJAB"}, {"state": "HARYANA"}, {"state": "RAJASTHAN"}, {"state": "DELHI"}, {"state": "UTTAR PRADESH"}, {"state": "UTTARAKHAND"}, {"state": "HIMACHAL\rPRADESH"}, {"state": "J&K(UT) &\rLadakh(UT)"}, {"state": "CHANDIGARH"}, {"state": "RAILWAYS_NR ISTS"}, {"state": "Region"}
            ]
        ],
        "nrldc_table_2C": [
            {"state": s["state"], "max_demand": None, "time_max": None, "shortage_during": None, "req_max_demand": None, "max_req_day": None, "time_max_req": None, "shortage_max_req": None, "demand_met_max_req": None, "min_demand_met": None, "time_min_demand": None, "ace_max": None, "time_ace_max": None, "ace_min": None, "time_ace_min": None}
            for s in [
                {"state": "PUNJAB"}, {"state": "HARYANA"}, {"state": "RAJASTHAN"}, {"state": "DELHI"}, {"state": "UP"}, {"state": "UTTARAKHA.."}, {"state": "HP"}, {"state": "J&K(UT)&Lad.."}, {"state": "CHANDIGARH"}, {"state": "RAILWAYS_NR\rISTS"}, {"state": "NR"}
            ]
        ]
    },
    'SRLDC': {
        "date": None,
        "srldc_table_2A": [
            {"state": s["state"], "thermal": None, "hydro": None, "gas_naptha_diesel": None, "solar": None, "wind": None, "others": None, "net_sch": None, "drawal": None, "ui": None, "availability": None, "demand_met": None, "shortage": None}
            for s in [
                {"state": "ANDHRA\rPRADESH"}, {"state": "KARNATAKA"}, {"state": "KERALA"}, {"state": "PONDICHERRY"}, {"state": "TAMILNADU"}, {"state": "TELANGANA"}, {"state": "Region"}
            ]
        ],
        "srldc_table_2C": [
            {"state": s["state"], "max_demand": None, "time": None, "shortage_max_demand": None, "req_max_demand": None, "demand_max_req": None, "time_max_req": None, "shortage_max_req": None, "max_req_day": None, "ace_min": None, "time_ace_min": None, "ace_max": None, "time_ace_max": None}
            for s in [
                {"state": "AP"}, {"state": "KAR"}, {"state": "KER"}, {"state": "PONDY"}, {"state": "TN"}, {"state": "TG"}, {"state": "Region"}
            ]
        ]
    },
    'WRLDC': {
        "date": None,
        "wrldc_table_2A": [
            {"state": s["state"], "thermal": None, "hydro": None, "gas": None, "wind": None, "solar": None, "others": None, "total": None, "net_sch": None, "drawal": None, "ui": None, "availability": None, "requirement": None, "shortage": None, "consumption": None}
            for s in [
                {"state": "BALCO"}, {"state": "CHHATTISGARH"}, {"state": "DNHDDPDCL"}, {"state": "AMNSIL"}, {"state": "GOA"}, {"state": "GUJARAT"}, {"state": "MAHARASHTRA"}, {"state": "RIL JAMNAGAR"}, {"state": "Region"}
            ]
        ],
        "wrldc_table_2C": [
            {"state": s["state"], "max_demand_day": None, "time": None, "shortage_max_demand": None, "req_max_demand": None, "ace_max": None, "time_ace_max": None, "ace_min": None, "time_ace_min": None}
            for s in [
                {"state": "AMNSIL"}, {"state": "BALCO"}, {"state": "CHHATTISGARH"}, {"state": "DNHDDPDCL"}, {"state": "GOA"}, {"state": "GUJARAT"}, {"state": "MAHARASHTRA"}, {"state": "RIL JAMNAGAR"}, {"state": "WR"}
            ]
        ]
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


# NEW: All of your script's logic is now inside the 'Command' class
class Command(BaseCommand):
    help = 'Merges the latest JSON reports from all sources and pushes to an API.'

    # NEW: The main execution logic goes into the handle() method
    def handle(self, *args, **options):
        
        # Use yesterday's date for API URL and pushed data
        yesterday_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        BASE_API_URL = "http://172.16.7.118:8003/api/tamilnadu/wind/api.grid.php"
        api_url_with_date = f"{BASE_API_URL}?date={yesterday_date}"

        merged_data = {}

        # Pick JSON files from current date
        today_str = datetime.now().strftime('%Y-%m-%d')
        for region, report_dir in report_dirs.items():
            region_data = None
            try:
                # List all subdirectories
                all_subdirs = [d for d in os.listdir(report_dir) if os.path.isdir(os.path.join(report_dir, d))]
                # Find subdir with today's date
                today_subdir = None
                for d in all_subdirs:
                    if today_str in d:
                        today_subdir = d
                        break
                if today_subdir:
                    full_subdir = os.path.join(report_dir, today_subdir)
                    json_files = glob(os.path.join(full_subdir, '*.json'))
                    json_file_name = None
                    if json_files:
                        # Pick the newest JSON file in today's subdir
                        json_file_name = sorted(json_files, key=os.path.getmtime, reverse=True)[0]
                    if not json_file_name:
                        self.stdout.write(self.style.WARNING(f"No JSON files found in {full_subdir} for {region}, using empty template."))
                        region_data = empty_templates.get(region, {})
                    else:
                        try:
                            with open(json_file_name, 'r', encoding='utf-8') as f:
                                data = json.load(f)
                                inner_data = data.get(region, data)
                                # Always set date to today for merged data
                                restructured_data = {'date': today_str, **inner_data}
                                template = empty_templates.get(region, {})
                                for table_key, template_value in template.items():
                                    if table_key == 'date':
                                        continue
                                    table_content = restructured_data.get(table_key)
                                    if not table_content or not any(table_content):
                                        self.stdout.write(self.style.WARNING(f"⚠️ Missing or empty table '{table_key}' for {region}, applying empty template."))
                                        restructured_data[table_key] = template_value
                                region_data = restructured_data
                                self.stdout.write(self.style.SUCCESS(f"✅ Merged data for {region} from {json_file_name}"))
                        except Exception as e:
                            self.stdout.write(self.style.ERROR(f"Error reading or validating {json_file_name} for {region}: {e}, using empty template."))
                            region_data = empty_templates.get(region, {})
                else:
                    self.stdout.write(self.style.WARNING(f"No subdir for today ({today_str}) in {report_dir} for {region}, using empty template."))
                    region_data = empty_templates.get(region, {})
            except (FileNotFoundError, NotADirectoryError):
                self.stdout.write(self.style.ERROR(f"Directory not found or is not a directory: {report_dir}, using empty template."))
                region_data = empty_templates.get(region, {})
            # Always set date to yesterday for merged data
            if region_data:
                region_data['date'] = yesterday_date
            merged_data[region] = region_data

        headers = {
            "Content-Type": "application/json"
        }

        try:
            self.stdout.write(f"\nAttempting to push data to: {api_url_with_date}...")
            response = requests.post(api_url_with_date, headers=headers, json=merged_data, timeout=30)

            if response.status_code in [200, 201]:
                self.stdout.write(self.style.SUCCESS(f"✅ Successfully pushed data to API. Status Code: {response.status_code}"))
                self.stdout.write(f"Response text: {response.text}") 
            else:
                self.stdout.write(self.style.ERROR(f"Failed to push data. Status Code: {response.status_code}"))
                self.stdout.write(f"Error Response: {response.text}")
        except requests.exceptions.RequestException as e:
            # Use CommandError to signal a critical failure to Django
            raise CommandError(f"🚨 An error occurred while trying to connect to the API: {e}")

        save_locally = True
        if save_locally:
            output_dir = os.path.join('downloads', 'overall_json')
            os.makedirs(output_dir, exist_ok=True)
            timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
            output_path = os.path.join(output_dir, f'merged_reports_{timestamp}.json')
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(merged_data, f, indent=2, ensure_ascii=False)
            self.stdout.write(self.style.SUCCESS(f"\nMerged latest reports for {yesterday_date} saved to {output_path}"))