from django.core.management.base import BaseCommand
import datetime
import requests
import io
import tabula
import pandas as pd
import json


class Command(BaseCommand):
    help = "Downloads the latest NLDC PSP PDF, extracts Table 1 and Table 7, and saves them as JSON."

    def get_latest_pdf(self, days=3):
        for i in range(days):
            date_to_check = datetime.date.today() - datetime.timedelta(days=i)
            year = date_to_check.strftime("%Y")
            month = date_to_check.strftime("%m")
            file_date = date_to_check.strftime("%d.%m.%y")
            # Updated URL pattern without suffix _327
            url = f"https://webcdn.grid-india.in/files/grdw/{year}/{month}/{file_date}_NLDC_PSP.pdf"
            print(url)
            resp = requests.get(url)
            if resp.status_code == 200:
                self.stdout.write(self.style.SUCCESS(f"Downloaded PDF for {file_date}"))
                return resp.content
        self.stdout.write(self.style.ERROR(f"Could not download PDF for the last {days} days."))
        return None
    

    def extract_tables_from_pdf(self, pdf_bytes, table_indices=(1, 7)):
        pdf_io = io.BytesIO(pdf_bytes)
        tables = tabula.read_pdf(pdf_io, pages="all", multiple_tables=True, lattice=True)
        tables_json = {}
        for idx in table_indices:
            if idx < len(tables) and not tables[idx].empty:
                df = tables[idx].dropna(how="all")
                df.reset_index(drop=True, inplace=True)
                tables_json[f"Table_{idx}"] = df.to_dict(orient="records")
        return tables_json

    def handle(self, *args, **options):
        pdf_bytes = self.get_latest_pdf()
        if pdf_bytes:
            tables_json = self.extract_tables_from_pdf(pdf_bytes)
            output_file = "output.json"
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(tables_json, f, indent=4, ensure_ascii=False)
            self.stdout.write(self.style.SUCCESS(f"✅ JSON saved successfully at: {output_file}"))
