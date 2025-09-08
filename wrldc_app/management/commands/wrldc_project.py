import requests
import datetime
import os
from tabula.io import read_pdf
import pandas as pd
import json
import logging
from django.core.management.base import BaseCommand, CommandError
from wrldc_app.models import Wrldc2AData, Wrldc2CData
import numpy as np

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class Command(BaseCommand):
    help = 'Download the new report and extract tables 2(A) and 2(C) to a single JSON file and save to DB'

    # List of expected state names for validation
    NEW_STATES = [
        "BALCO", "CHHATTISGARH", "DNHDDPDCL", "AMNSIL", "GOA", "GUJARAT",
        "MADHYA PRADESH", "MAHARASHTRA", "RIL JAMNAGAR", "REGION", "WR"
    ]
    # For Table 2C, states might be abbreviated or slightly different
    NEW_STATES_2C = [
        "BALCO", "CHHATTISGARH", "DNHDDPDCL", "AMNSIL", "GOA", "GUJARAT",
        "MADHYA PRADESH", "MAHARASHTRA", "RIL JAMNAGAR", "WR"
    ]

    def _safe_value(self, value, is_numeric=False):
        """
        Keeps dash '-' as-is, returns None for real empty values,
        converts numbers properly.
        """
        if pd.isna(value) or value is None:
            return None

        s_val = str(value).strip()

        # If it's a dash or similar marker, keep it as-is
        if s_val in ['-', '--']:
            return s_val

        # If it's empty/NaN/None, return None
        if s_val.lower() in ('nan', 'none', ''):
            return None

        # For numeric columns, try to convert to float/int
        if is_numeric:
            s_val = s_val.replace(',', '')  # remove commas
            try:
                return float(s_val)
            except ValueError:
                return s_val  

        # For string columns
        return s_val

    def _cleanup_dataframe(self, df, numeric_cols, string_cols):

        df_cleaned = df.copy()

        # Clean string columns
        for col in string_cols:
            if col in df_cleaned.columns:
                df_cleaned[col] = df_cleaned[col].apply(
                    lambda x: self._safe_value(x, is_numeric=False)
                )

        # Clean numeric columns
        for col in numeric_cols:
            if col in df_cleaned.columns:
                df_cleaned[col] = df_cleaned[col].apply(
                    lambda x: self._safe_value(x, is_numeric=True)
                )

        return df_cleaned


    def extract_subtable_by_markers(self, df, start_marker, end_marker=None, header_row_count=0, debug_table_name="Unknown Table"):
        """
        Extracts a sub-table from a DataFrame based on start and optional end markers.
        This function is now simpler and just finds the raw data frame section.
        """
        start_idx = None
        end_idx = None

        for i, row in df.iterrows():
            row_str_series = row.astype(str).str.strip().str.replace(r'\s+', ' ', regex=True)
            if row_str_series.str.contains(start_marker, regex=True, na=False, case=False).any():
                start_idx = i
                break

        if start_idx is None:
            self.stdout.write(self.style.WARNING(f"⚠️ Start marker '{start_marker}' not found for {debug_table_name}."))
            return pd.DataFrame(), None

        # Start from the row immediately after the header rows
        data_start_idx = start_idx + header_row_count
        if data_start_idx >= len(df):
            self.stdout.write(self.style.WARNING(f"⚠️ Data start index is out of bounds for {debug_table_name}. Returning empty DataFrame."))
            return pd.DataFrame(), None

        if end_marker:
            for i in range(data_start_idx, len(df)):
                row_str_series = df.iloc[i].astype(str).str.strip().str.replace(r'\s+', ' ', regex=True)
                if row_str_series.str.contains(end_marker, regex=True, na=False, case=False).any():
                    end_idx = i
                    break

        if end_idx is not None:
            raw_sub_df = df.iloc[data_start_idx:end_idx].copy().reset_index(drop=True)
        else:
            raw_sub_df = df.iloc[data_start_idx:].copy().reset_index(drop=True)

        # Drop empty columns and rows
        raw_sub_df = raw_sub_df.dropna(axis=0, how='all').dropna(axis=1, how='all')
        # Reset index after dropping rows
        return raw_sub_df.reset_index(drop=True), None

    def extract_tables_from_pdf(self, pdf_path, output_dir, report_date):
        self.stdout.write("🔍 Extracting tables from PDF...")

        try:
            tables = read_pdf(
                pdf_path,
                pages='all',
                multiple_tables=True,
                pandas_options={'header': None},
                lattice=True
            )
        except Exception as e:
            raise CommandError(f"❌ Tabula extraction failed: {e}")

        if not tables:
            raise CommandError("❌ No tables found in the PDF.")

        self.stdout.write(self.style.SUCCESS(f"✅ Found {len(tables)} potential tables. Starting table extraction..."))

        all_content_df = pd.concat(tables, ignore_index=True)
        all_content_df_cleaned = all_content_df.dropna(axis=0, how='all')
        
        combined_json_data = {}

        # --- Extract Table 2(A) using the robust subtable function and new marker ---
        # Using a flexible regex that looks for the table number and key English phrases
        start_marker_2A = r"2\(A\)\s*.*LOAD DETAILS.*IN MU"
        # The end marker should be for table 2B, looking for "Demand Met in MW"
        end_marker_2A = r"2\(B\).*Demand Met in MW"
        
        expected_cols_2A = [
            'State', 'Thermal', 'Hydro', 'Gas', 'Wind', 'Solar', 'Others',
            'Total', 'Net SCH', 'Drawal', 'UI', 'Availability', 'Requirement', 'Shortage', 'Consumption'
        ]

        sub_2A_raw, _ = self.extract_subtable_by_markers(
            all_content_df_cleaned,
            start_marker=start_marker_2A,
            end_marker=end_marker_2A,
            header_row_count=2, # Header is typically 2 rows
            debug_table_name="Table 2(A)"
        )


        if not sub_2A_raw.empty:
            self.stdout.write(self.style.NOTICE("\n--- RAW DataFrame for Table 2(A) (before processing) ---"))
            self.stdout.write(str(sub_2A_raw))
            self.stdout.write(self.style.NOTICE("---------------------------------------------------------"))


            if len(sub_2A_raw.columns) >= len(expected_cols_2A):
                sub_2A_data = sub_2A_raw.iloc[:, :len(expected_cols_2A)].copy()
                sub_2A_data.columns = expected_cols_2A
            else:
                self.stdout.write(self.style.WARNING(f"⚠️ Column count mismatch for Table 2A. Expected {len(expected_cols_2A)}, got {len(sub_2A_raw.columns)}. This might cause data misalignment."))
                sub_2A_data = sub_2A_raw.copy()
            
            # --- START DEDICATED DATA CLEANING STEP FOR TABLE 2A ---
            numeric_cols_2A = [
                'Thermal', 'Hydro', 'Gas', 'Wind', 'Solar', 'Others',
                'Total', 'Net SCH', 'Drawal', 'UI', 'Availability', 'Requirement',
                'Shortage', 'Consumption'
            ]
            string_cols_2A = ['State']
            
            sub_2A_cleaned = self._cleanup_dataframe(sub_2A_data, numeric_cols_2A, string_cols_2A)
            # --- END DEDICATED DATA CLEANING STEP ---
            
            self.stdout.write(self.style.SUCCESS("\n--- Cleaned and filtered data for Table 2(A) ---"))
            self.stdout.write(str(sub_2A_cleaned))
            self.stdout.write(self.style.SUCCESS("------------------------------------------------------"))


            column_mapping_2A = {
                'State': 'state', 'Thermal': 'thermal', 'Hydro': 'hydro',
                'Gas': 'gas', 'Wind': 'wind', 'Solar': 'solar', 'Others': 'others',
                'Total': 'total', 'Net SCH': 'net_sch', 'Drawal': 'drawal',
                'UI': 'ui', 'Availability': 'availability', 'Requirement': 'requirement',
                'Shortage': 'shortage', 'Consumption': 'consumption'
            }


            sub_2A_renamed = sub_2A_cleaned.rename(columns=column_mapping_2A)
            normalized_states = [s.strip().upper() for s in self.NEW_STATES]
            sub_2A_filtered = sub_2A_renamed[
                sub_2A_renamed['state'].astype(str).str.strip().str.upper().isin(normalized_states)
            ].copy()
            
            sub_2A_final = sub_2A_filtered.dropna(subset=['state']).copy()
            self.stdout.write(f"States found for Table 2A after filtering: {sub_2A_final['state'].tolist()}")


            combined_json_data['wrldc_table_2A'] = sub_2A_final.to_dict(orient='records')
            self.stdout.write(self.style.SUCCESS(f"✅ Table 2(A) extracted for combined JSON."))


            for index, row_data in sub_2A_final.iterrows():
                try:
                    Wrldc2AData.objects.update_or_create(
                        report_date=report_date,
                        state=row_data['state'],
                        defaults={
                            'thermal': row_data.get('thermal'),
                            'hydro': row_data.get('hydro'),
                            'gas': row_data.get('gas'),
                            'solar': row_data.get('solar'),
                            'wind': row_data.get('wind'),
                            'others': row_data.get('others'),
                            'total': row_data.get('total'),
                            'net_sch': row_data.get('net_sch'),
                            'drawal': row_data.get('drawal'),
                            'ui': row_data.get('ui'),
                            'availability': row_data.get('availability'),
                            'requirement': row_data.get('requirement'),
                            'shortage': row_data.get('shortage'),
                            'consumption': row_data.get('consumption'),
                        }
                    )
                except Exception as e:
                    self.stdout.write(self.style.ERROR(f"❌ Error saving Table 2A row to DB (State: {row_data.get('state')}): {e}"))
            self.stdout.write(self.style.SUCCESS(f"✅ Table 2(A) data saved to database."))
        else:
            self.stdout.write(self.style.WARNING("⚠️ Table 2(A) not found or extraction failed."))


        # --- Extract Table 2(C) with a more robust, manual column assignment approach ---
        sub_2C_raw, _ = self.extract_subtable_by_markers(
            all_content_df_cleaned,
            start_marker=r"2\(C\)\s*/\s*State's Demand Met in MW.*",
            end_marker=r"3\(A\)\s*StateEntities\s*Generation:",
            header_row_count=2,
            debug_table_name="Table 2(C)"
        )


        if not sub_2C_raw.empty:
            self.stdout.write(self.style.NOTICE("\n--- RAW DataFrame for Table 2(C) (before processing) ---"))
            self.stdout.write(str(sub_2C_raw))
            self.stdout.write(self.style.NOTICE("---------------------------------------------------------"))


            manual_columns = [
                'state',
                'max_demand_met_of_the_day',
                'time',
                'shortage_during_max_demand',
                'requirement_at_max_demand',
                'ace_max',
                'time_ace_max',
                'ace_min',
                'time_ace_min'
            ]


            if len(sub_2C_raw.columns) >= len(manual_columns):
                sub_2C_data = sub_2C_raw.iloc[:, :len(manual_columns)].copy()
            else:
                sub_2C_data = sub_2C_raw.copy()


            if len(sub_2C_data.columns) != len(manual_columns):
                self.stdout.write(self.style.WARNING(f"⚠️ Column count mismatch for Table 2C. Expected {len(manual_columns)}, got {len(sub_2C_data.columns)}. This might cause data misalignment."))


            sub_2C_data.columns = manual_columns[:len(sub_2C_data.columns)]


            # --- START DEDICATED DATA CLEANING STEP FOR TABLE 2C ---
            numeric_cols_2C = [
                'max_demand_met_of_the_day', 'shortage_during_max_demand',
                'requirement_at_max_demand', 'ace_max', 'ace_min'
            ]
            string_cols_2C = ['state', 'time', 'time_ace_max', 'time_ace_min']
            
            sub_2C_data_cleaned = self._cleanup_dataframe(sub_2C_data, numeric_cols_2C, string_cols_2C)
            # --- END DEDICATED DATA CLEANING STEP ---


            # Print the cleaned data before saving
            self.stdout.write(self.style.SUCCESS("\n--- Cleaned and filtered data for Table 2(C) ---"))
            self.stdout.write(str(sub_2C_data_cleaned))
            self.stdout.write(self.style.SUCCESS("------------------------------------------------------"))


            normalized_states_2C = [s.strip().upper() for s in self.NEW_STATES_2C]
            sub_2C_filtered = sub_2C_data_cleaned[
                sub_2C_data_cleaned['state'].astype(str).str.strip().str.upper().isin(normalized_states_2C)
            ].copy()


            # Final check to drop any rows that ended up with a None state after cleaning
            sub_2C_final = sub_2C_filtered.dropna(subset=['state']).copy()
            sub_2C_final['state'] = sub_2C_final['state'].str.strip().str.replace('\r', ' ', regex=False).str.upper()
            sub_2C_final = sub_2C_final.sort_values(by='state').reset_index(drop=True)


            self.stdout.write(f"States found for Table 2C after filtering: {sub_2C_final['state'].tolist()}")


            combined_json_data['wrldc_table_2C'] = sub_2C_final.to_dict(orient='records')
            self.stdout.write(self.style.SUCCESS(f"✅ Table 2(C) extracted for combined JSON."))


            for index, row_data in sub_2C_final.iterrows():
                try:
                    Wrldc2CData.objects.update_or_create(
                        report_date=report_date, state=row_data['state'],
                        defaults={
                            'max_demand_met_of_the_day': row_data.get('max_demand_met_of_the_day'),
                            'time': row_data.get('time'),
                            'shortage_during_max_demand': row_data.get('shortage_during_max_demand'),
                            'requirement_at_max_demand': row_data.get('requirement_at_max_demand'),
                            'ace_max': row_data.get('ace_max'),
                            'time_ace_max': row_data.get('time_ace_max'),
                            'ace_min': row_data.get('ace_min'),
                            'time_ace_min': row_data.get('time_ace_min'),
                        }
                    )
                except Exception as e:
                    self.stdout.write(self.style.ERROR(f"❌ Error saving Table 2C row to DB (State: {row_data.get('state')}): {e}"))
            self.stdout.write(self.style.SUCCESS(f"✅ Table 2(C) data saved to database."))
        else:
            self.stdout.write(self.style.WARNING("⚠️ Table 2(C) not found or extraction failed."))


        if combined_json_data:
            # Save JSON file with today's date
            date_str = datetime.datetime.now().strftime('%Y-%m-%d')
            combined_json_path = os.path.join(output_dir, f'wrdc_report_tables_{date_str}.json')
            with open(combined_json_path, 'w', encoding='utf-8') as f:
                json.dump(combined_json_data, f, indent=4, ensure_ascii=False)
            self.stdout.write(self.style.SUCCESS(f"✅ Combined tables saved to: {combined_json_path}"))
        else:
            self.stdout.write(self.style.WARNING("⚠️ No tables were successfully extracted to create a combined JSON file."))

    def download_latest_pdf(self, new_base_url, base_download_dir="downloads"):
        project_name = "WRLDC"  # Added project name here
        base_download_dir = os.path.join(base_download_dir, project_name)
        os.makedirs(base_download_dir, exist_ok=True)
        
        pdf_path = None
        report_date = None
        report_dir = None

        today = datetime.datetime.now(datetime.timezone.utc).astimezone(datetime.timezone(datetime.timedelta(hours=5, minutes=30)))
        dates_to_try = [today, today - datetime.timedelta(days=1)]

        for current_date in dates_to_try:
            year = current_date.year
            month_name = current_date.strftime('%B')
            day = current_date.day

            directory_path_on_server = f"{year}/{month_name}/"
            file_name_on_server = f"WRLDC_PSP_Report_{day:02d}-{current_date.month:02d}-{year}.pdf"

            full_url = f"{new_base_url}{directory_path_on_server}{file_name_on_server}"
            # Use current date and time for the directory name
            now_str = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
            report_dir = os.path.join(base_download_dir, f"report_{now_str}")
            os.makedirs(report_dir, exist_ok=True)
            self.stdout.write(f"📁 Checking/Created report directory: {report_dir}")

            local_pdf_filename = file_name_on_server
            local_file_path = os.path.join(report_dir, local_pdf_filename)

            if os.path.exists(local_file_path):
                self.stdout.write(self.style.NOTICE(f"📄 PDF already exists locally for {current_date.strftime('%d-%m-%Y')} at {local_file_path}. Skipping download."))
                pdf_path = local_file_path
                report_date = current_date.date()
                return pdf_path, report_date, report_dir

            self.stdout.write(f"🌐 Attempting to download from: {full_url}")
            logging.info(f"Attempting to download from: {full_url}")

            try:
                response = requests.get(full_url, stream=True)
                response.raise_for_status()
                with open(local_file_path, 'wb') as pdf_file:
                    for chunk in response.iter_content(chunk_size=8192):
                        pdf_file.write(chunk)
                self.stdout.write(self.style.SUCCESS(f"✅ Successfully downloaded: {local_pdf_filename} to {report_dir}"))
                logging.info(f"Successfully downloaded: {local_file_path} to {report_dir}")
                pdf_path = local_file_path
                report_date = current_date.date()
                return pdf_path, report_date, report_dir

            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 404:
                    self.stdout.write(self.style.WARNING(f"⚠️ File not found for {current_date.strftime('%d-%m-%Y')} at {full_url}. Trying next date if available."))
                    logging.warning(f"File not found for {current_date.strftime('%d-%m-%Y')} at {full_url}. Trying next date if available.")
                    if os.path.exists(report_dir) and not os.listdir(report_dir):
                        os.rmdir(report_dir)
                else:
                    self.stdout.write(self.style.ERROR(f"❌ HTTP Error {e.response.status_code} while downloading {file_name_on_server}: {e}"))
                    logging.error(f"HTTP Error {e.response.status_code} while downloading {file_name_on_server}: {e}")
            except requests.exceptions.RequestException as e:
                self.stdout.write(self.style.ERROR(f"❌ An unexpected error occurred during download: {e}"))
                logging.error(f"An unexpected error occurred during download: {e}")

        self.stdout.write(self.style.ERROR("❌ Failed to download the latest report after trying all attempts."))
        logging.error("Failed to download the latest report after trying all attempts.")
        return None, None, None

    def handle(self, *args, **options):
        if "JAVA_HOME" not in os.environ:
            self.stdout.write(self.style.WARNING("JAVA_HOME environment variable not set. tabula-py may fail."))

        new_url = "https://reporting.wrldc.in:8081/PSP/"

        pdf_path, report_date, report_output_dir = self.download_latest_pdf(new_url)

        if pdf_path is None:
            self.stdout.write(self.style.WARNING("No PDF report was successfully downloaded or found locally. Exiting."))
            return

        self.extract_tables_from_pdf(pdf_path, report_output_dir, report_date)
        self.stdout.write(self.style.SUCCESS(f"Finished processing. Files saved in: {report_output_dir}")) 
