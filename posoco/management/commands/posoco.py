from django.core.management.base import BaseCommand
import os
import requests
import tabula
import json
from datetime import datetime
from posoco.models import PosocoTableA, PosocoTableG
from datetime import datetime

API_URL = "https://webapi.grid-india.in/api/v1/file"
BASE_URL = "https://webcdn.grid-india.in/"
SAVE_DIR = "downloads/POSOCO"  

payload = {
    "_source": "GRDW",
    "_type": "DAILY_PSP_REPORT",
    "_fileDate": "2025-26",  
    "_month": "09"           
}


def make_report_dir(base_dir):
    """Create a timestamped subfolder inside POSOCO/."""
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    report_dir = os.path.join(base_dir, f"report_{timestamp}")
    os.makedirs(report_dir, exist_ok=True)
    return report_dir, timestamp


def fetch_latest_pdf(api_url, base_url, payload, report_dir, timestamp):
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

    if file_response.status_code == 200:
        with open(local_path, "wb") as f:
            for chunk in file_response.iter_content(1024):
                f.write(chunk)
        print(f"✅ Saved latest PDF: {local_path}")
        return local_path
    else:
        print(f"❌ Failed to download {download_url} ({file_response.status_code})")
        return None


def extract_tables_from_pdf(pdf_file, report_dir, timestamp):
    tables = tabula.read_pdf(pdf_file, pages="all", multiple_tables=True, lattice=True)

    final_json = {"POSOCO": {"posoco_table_a": [], "posoco_table_g": []}}


    if len(tables) > 1 and not tables[1].empty:
        df1 = tables[1].dropna(how="all")
        df1.reset_index(drop=True, inplace=True)
        table_a_dict = {}
        for _, row in df1.iterrows():
            row_dict = row.to_dict()
            key = row_dict.pop("Unnamed: 0", None)
            if key:
                table_a_dict[key.strip()] = row_dict
        final_json["POSOCO"]["posoco_table_a"].append(table_a_dict)


    if len(tables) > 7 and not tables[7].empty:
        df7 = tables[7].dropna(how="all")
        df7.reset_index(drop=True, inplace=True)
        table_g_dict = {}
        for _, row in df7.iterrows():
            row_dict = row.to_dict()
            key = row_dict.pop("Unnamed: 0", None)
            if key:
                table_g_dict[key.strip()] = row_dict
        final_json["POSOCO"]["posoco_table_g"].append(table_g_dict)


    date_str = datetime.now().strftime("%Y-%m-%d")
    json_name = f"posoco_report_tables_{date_str}.json"
    output_json = os.path.join(report_dir, json_name)

    with open(output_json, "w", encoding="utf-8") as f:
        json.dump(final_json, f, indent=4, ensure_ascii=False)

    print(f"✅ JSON saved successfully at: {output_json}")
    
    return final_json


def save_to_db(final_json):
    today = datetime.now().date()


    for row_dict in final_json["POSOCO"]["posoco_table_a"]:
        for category, values in row_dict.items():
            PosocoTableA.objects.create(
                category=category,
                nr=values.get("NR"),
                wr=values.get("WR"),
                sr=values.get("SR"),
                er=values.get("ER"),
                ner=values.get("NER"),
                total=values.get("TOTAL"),
                report_date=today,
            )


    for row_dict in final_json["POSOCO"]["posoco_table_g"]:
        for fuel, values in row_dict.items():
            PosocoTableG.objects.create(
                fuel_type=fuel,
                nr=values.get("NR"),
                wr=values.get("WR"),
                sr=values.get("SR"),
                er=values.get("ER"),
                ner=values.get("NER"),
                all_india=values.get("All India"),
                share_percent=values.get("% Share"),
                report_date=today,
            )
    print("✅ Data saved into database successfully")


class Command(BaseCommand):
    help = "Downloads the latest NLDC PSP PDF, extracts Table 1 and Table 7, and saves them in a timestamped POSOCO directory."

    def handle(self, *args, **options):
        report_dir, timestamp = make_report_dir(SAVE_DIR)
        pdf_path = fetch_latest_pdf(API_URL, BASE_URL, payload, report_dir, timestamp)
        
        if pdf_path:
            final_json = extract_tables_from_pdf(pdf_path, report_dir, timestamp)
            if final_json:
                save_to_db(final_json)