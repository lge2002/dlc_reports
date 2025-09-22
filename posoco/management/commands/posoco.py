from django.core.management.base import BaseCommand
import os
import requests
import tabula
import json
from datetime import datetime
from posoco.models import PosocoTableA, PosocoTableG

# --- Constants ---
API_URL = "https://webapi.grid-india.in/api/v1/file"
BASE_URL = "https://webcdn.grid-india.in/"
SAVE_DIR = "downloads/POSOCO"

payload = {
    "_source": "GRDW",
    "_type": "DAILY_PSP_REPORT",
    "_fileDate": "2025-26",  # Note: This might need adjustment for the current year.
    "_month": "09"
}

# --- Helper Functions ---
def make_report_dir(base_dir):
    """Create a timestamped subfolder inside POSOCO/."""
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    report_dir = os.path.join(base_dir, f"report_{timestamp}")
    os.makedirs(report_dir, exist_ok=True)
    return report_dir, timestamp


def fetch_latest_pdf(api_url, base_url, payload, report_dir, timestamp):
    """Fetches and downloads the latest PDF report."""
    try:
        # API call
        response = requests.post(api_url, json=payload)
        response.raise_for_status()
        data = response.json()

        if "retData" not in data or not data["retData"]:
            print("⚠️ No files found in response")
            return None

        pdf_files = [f for f in data["retData"] if f.get("MimeType") == "application/pdf"]

        if not pdf_files:
            print("⚠️ No PDF files available")
            return None

        latest_file = pdf_files[0]
        file_path = latest_file.get("FilePath")
        if not file_path:
            print("⚠️ Missing FilePath for latest PDF")
            return None

        date_str = datetime.now().strftime("%Y-%m-%d")
        pdf_name = f"posoco_psp_report_{date_str}.pdf"
        local_path = os.path.join(report_dir, pdf_name)

        download_url = base_url.rstrip("/") + "/" + file_path.lstrip("/")
        print(f"⬇️ Downloading latest PDF: {download_url}")
        file_response = requests.get(download_url, stream=True)
        file_response.raise_for_status()

        with open(local_path, "wb") as f:
            for chunk in file_response.iter_content(1024):
                f.write(chunk)
        print(f"✅ Saved latest PDF: {local_path}")
        return local_path

    except requests.exceptions.RequestException as e:
        print(f"❌ An error occurred during download: {e}")
        return None


def extract_tables_from_pdf(pdf_file, report_dir, timestamp):
    """Extracts tables, renames headings, and saves as JSON."""
    # Dictionary to map long original names to short, clean names
    KEY_MAPPING = {
        # Table A keys
        "Demand Met during Evening Peak hrs(MW) (at 20:00 hrs; from RLDCs)": "demand_evening_peak",
        "Peak Shortage (MW)": "peak_shortage",
        "Energy Met (MU)": "energy",
        "Hydro Gen (MU)": "hydro",
        "Wind Gen (MU)": "wind",
        "Solar Gen (MU)*": "solar",
        "Energy Shortage (MU)": "energy_shortage",
        "Maximum Demand Met During the Day (MW) (From NLDC SCADA)": "max_demand_day",
        "Time Of Maximum Demand Met": "time_of_max_demand",
        # Table G keys
        "Coal": "coal",
        "Lignite": "lignite",
        "Hydro": "hydro",
        "Nuclear": "nuclear",
        "Gas, Naptha & Diesel": "gas_naptha_diesel",
        "RES (Wind, Solar, Biomass & Others)": "res_total",
        "Total": "total"
    }

    tables = tabula.read_pdf(pdf_file, pages="all", multiple_tables=True, lattice=True)
    final_json = {"POSOCO": {"posoco_table_a": [], "posoco_table_g": []}}

    # Process Table A (assuming it's the 2nd table found)
    if len(tables) > 1 and not tables[1].empty:
        df1 = tables[1].dropna(how="all").reset_index(drop=True)
        table_a_dict = {}
        for _, row in df1.iterrows():
            row_dict = row.to_dict()
            original_key = row_dict.pop("Unnamed: 0", None)
            if original_key:
                # Clean up the key by removing newline/carriage return characters
                clean_key = ' '.join(original_key.replace('\r', ' ').split())
                # Use the mapping to get the short name, fall back to original if not found
                short_key = KEY_MAPPING.get(clean_key, clean_key)
                table_a_dict[short_key] = row_dict
        final_json["POSOCO"]["posoco_table_a"].append(table_a_dict)

    # Process Table G (assuming it's the 8th table found)
    if len(tables) > 7 and not tables[7].empty:
        df7 = tables[7].dropna(how="all").reset_index(drop=True)
        table_g_dict = {}
        for _, row in df7.iterrows():
            row_dict = row.to_dict()
            original_key = row_dict.pop("Unnamed: 0", None)
            if original_key:
                clean_key = original_key.strip()
                short_key = KEY_MAPPING.get(clean_key, clean_key)
                table_g_dict[short_key] = row_dict
        final_json["POSOCO"]["posoco_table_g"].append(table_g_dict)

    # Save the final JSON to a file
    date_str = datetime.now().strftime("%Y-%m-%d")
    json_name = f"posoco_report_tables_{date_str}.json"
    output_json = os.path.join(report_dir, json_name)

    with open(output_json, "w", encoding="utf-8") as f:
        json.dump(final_json, f, indent=4, ensure_ascii=False)

    print(f"✅ JSON with shortened keys saved successfully at: {output_json}")
    
    return final_json


def save_to_db(final_json):
    """Saves the processed JSON data to the Django database."""
    today = datetime.now().date()
    
    try:
        # Save data from Table A
        for row_dict in final_json["POSOCO"]["posoco_table_a"]:
            for category, values in row_dict.items():
                PosocoTableA.objects.update_or_create(
                    category=category,
                    report_date=today,
                    defaults={
                        'nr': values.get("NR"),
                        'wr': values.get("WR"),
                        'sr': values.get("SR"),
                        'er': values.get("ER"),
                        'ner': values.get("NER"),
                        'total': values.get("TOTAL"),
                    }
                )

        # Save data from Table G
        for row_dict in final_json["POSOCO"]["posoco_table_g"]:
            for fuel, values in row_dict.items():
                PosocoTableG.objects.update_or_create(
                    fuel_type=fuel,
                    report_date=today,
                    defaults={
                        'nr': values.get("NR"),
                        'wr': values.get("WR"),
                        'sr': values.get("SR"),
                        'er': values.get("ER"),
                        'ner': values.get("NER"),
                        'all_india': values.get("All India"),
                        'share_percent': values.get("% Share"),
                    }
                )
        print("✅ Data saved to database successfully")
    except Exception as e:
        print(f"❌ An error occurred while saving to the database: {e}")

# --- Django Management Command ---
class Command(BaseCommand):
    help = "Downloads the latest NLDC PSP PDF, extracts key tables with shortened headings, and saves them to a file and the database."

    def handle(self, *args, **options):
        self.stdout.write("🚀 Starting POSOCO report download and processing...")
        report_dir, timestamp = make_report_dir(SAVE_DIR)
        pdf_path = fetch_latest_pdf(API_URL, BASE_URL, payload, report_dir, timestamp)
        
        if pdf_path:
            final_json = extract_tables_from_pdf(pdf_path, report_dir, timestamp)
            if final_json and (final_json["POSOCO"]["posoco_table_a"] or final_json["POSOCO"]["posoco_table_g"]):
                save_to_db(final_json)
            else:
                self.stdout.write(self.style.WARNING("Could not extract any data from the PDF to save."))
        else:
            self.stdout.write(self.style.ERROR("Failed to download PDF. Aborting process."))
        
        self.stdout.write(self.style.SUCCESS("✅ Process finished."))