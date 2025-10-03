# ===================================================================
# ORIGINAL IMPORTS (with new ones added for the dashboard)
# ===================================================================
import requests
import datetime
import os
import pandas as pd
import json
import logging
import traceback  # Added for detailed error logging
from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone  # Added for timestamping
from nrldc_app.models import Nrldc2AData, Nrldc2CData
from report_dashboard.models import AutomationJob  # Added to control the dashboard
from tabula.io import read_pdf


class Command(BaseCommand):
    # ===================================================================
    # YOUR ORIGINAL CONTENT (UNCHANGED)
    # ===================================================================
    help = 'Download today\'s NRDC report and extract tables 2(A) and 2(C) to a single JSON file and save to DB'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        log_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'logs')
        os.makedirs(log_dir, exist_ok=True)
        log_file = os.path.join(log_dir, 'nrldc.log')

        self.logger = logging.getLogger('nrldc_logger')
        self.logger.setLevel(logging.INFO)

        if not self.logger.hasHandlers():
            fh = logging.FileHandler(log_file, encoding='utf-8')
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            fh.setFormatter(formatter)
            self.logger.addHandler(fh)

    def write(self, message, level='info', style=None):
        # Using style argument to avoid conflict with self.style
        output_func = self.stdout.write if style is None else style
        output_func(message)
        if level == 'info':
            self.logger.info(message)
        elif level == 'warning':
            self.logger.warning(message)
        elif level == 'error':
            self.logger.error(message)

    def extract_subtable_by_markers(self, df, start_marker, end_marker=None, header_row_count=0, debug_table_name="Unknown Table"):
        start_idx = None
        end_idx = None

        for i, row in df.iterrows():
            if row.astype(str).str.strip().str.contains(start_marker, regex=True, na=False, case=False).any():
                start_idx = i
                break

        if start_idx is None:
            self.write(f"⚠️ Start marker '{start_marker}' not found for {debug_table_name}.", level='warning', style=self.style.WARNING)
            return None

        if end_marker:
            for i in range(start_idx + 1, len(df)):
                if df.iloc[i].astype(str).str.strip().str.contains(end_marker, regex=True, na=False, case=False).any():
                    end_idx = i
                    break

        if end_idx is not None:
            raw_sub_df = df.iloc[start_idx:end_idx].copy().reset_index(drop=True)
        else:
            raw_sub_df = df.iloc[start_idx:].copy().reset_index(drop=True)

        data_start_row_in_raw_sub_df = 1 + header_row_count

        if header_row_count > 0 and len(raw_sub_df) >= data_start_row_in_raw_sub_df:
            headers_df = raw_sub_df.iloc[1 : data_start_row_in_raw_sub_df]

            new_columns = []
            if header_row_count == 1:
                new_columns = headers_df.iloc[0].astype(str).str.strip().tolist()
            elif header_row_count == 2:
                raw_top_header = headers_df.iloc[0].astype(str).str.replace('\n', ' ', regex=False).str.strip().fillna('')
                raw_bottom_header = headers_df.iloc[1].astype(str).str.replace('\n', ' ', regex=False).str.strip().fillna('')

                if debug_table_name == "Table 2(A)":
                    new_columns = [
                        'State', 'Thermal', 'Hydro', 'Gas/Naptha/Diesel', 'Solar', 'Wind',
                        'Others(Biomass/Co-gen etc.)', 'Total', 'Drawal Sch (Net MU)',
                        'Act Drawal (Net MU)', 'UI (Net MU)', 'Requirement (Net MU)',
                        'Shortage (Net MU)', 'Consumption (Net MU)'
                    ]
                elif debug_table_name == "Table 2(C)":
                    new_columns = [
                        'State', 'Maximum Demand Met of the day', 'Time',
                        'Shortage during maximum demand', 'Requirement at maximum demand',
                        'Maximum requirement of the day', 'Time.1',
                        'Shortage during maximum requirement',
                        'Demand Met at maximum Requirement', 'Min Demand Met', 'Time.2',
                        'ACE_MAX', 'ACE_MIN', 'Time.3', 'Time.4'
                    ]
                else:
                    self.write(f"⚠️ Custom header combination logic not defined for {debug_table_name}.", level='warning', style=self.style.WARNING)
                    for idx in range(raw_top_header.shape[0]):
                        t_col = raw_top_header.iloc[idx].strip()
                        b_col = raw_bottom_header.iloc[idx].strip()
                        if not t_col and not b_col: new_columns.append(f"Unnamed_{idx}")
                        elif not b_col: new_columns.append(t_col)
                        elif not t_col: new_columns.append(b_col)
                        elif not b_col.startswith(t_col): new_columns.append(f"{t_col} {b_col}".strip())
                        else: new_columns.append(b_col)
            else:
                self.write(f"⚠️ Unsupported header_row_count: {header_row_count}", level='warning', style=self.style.WARNING)
                new_columns = None

            if new_columns is not None:
                expected_data_cols = raw_sub_df.shape[1]
                if len(new_columns) < expected_data_cols:
                    new_columns.extend([f"Unnamed_Col_{i}" for i in range(len(new_columns), expected_data_cols)])
                elif len(new_columns) > expected_data_cols:
                    new_columns = new_columns[:expected_data_cols]
                sub_df_data = raw_sub_df.iloc[data_start_row_in_raw_sub_df:].copy()
                sub_df_data.columns = new_columns
                sub_df_data = sub_df_data.loc[:, ~sub_df_data.columns.duplicated()]
                sub_df_data.columns = sub_df_data.columns.astype(str).str.strip().str.replace(r'\s*\r\s*', ' ', regex=True)
                sub_df_data = sub_df_data.dropna(axis=0, how='all')
                return sub_df_data.dropna(axis=1, how='all')
            else:
                return raw_sub_df.iloc[data_start_row_in_raw_sub_df:].dropna(axis=1, how='all')
        else:
            return raw_sub_df.iloc[1:].dropna(axis=1, how='all')

    def _safe_float(self, value):
        if isinstance(value, str):
            value = value.strip()
            if ':' in value: return None
            value = value.replace(',', '')
            if not value or value.lower() in ['n/a', '-', 'null', 'nan']: return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    def _safe_string(self, value):
        if pd.isna(value) or value is None: return None
        return str(value).strip() if value is not None else None

    def extract_tables_from_pdf(self, pdf_path, output_dir, report_date):
        self.write("🔍 Extracting tables from PDF...")
        try:
            tables = read_pdf(pdf_path, pages='all', multiple_tables=True, pandas_options={'header': None}, lattice=True)
        except Exception as e:
            raise CommandError(f"❌ Tabula extraction failed: {e}")

        if not tables: raise CommandError("❌ No tables found in the PDF.")
        self.write(f"✅ Found {len(tables)} tables.", style=self.style.SUCCESS)

        all_content_df = pd.concat(tables, ignore_index=True)
        all_content_df_cleaned = all_content_df.dropna(axis=0, how='all')
        combined_json_data = {}

        # Extract Table 2(A)
        sub_2A = self.extract_subtable_by_markers(all_content_df_cleaned, r".*2\s*\(A\)\s*State's\s*Load\s*Deails.*", r"2\s*\(B\).*", header_row_count=2, debug_table_name="Table 2(A)")
        if sub_2A is not None:
            column_mapping_2A = { 'State': 'state', 'Thermal': 'thermal', 'Hydro': 'hydro', 'Gas/Naptha/Diesel': 'gas_naptha_diesel', 'Solar': 'solar', 'Wind': 'wind', 'Others(Biomass/Co-gen etc.)': 'other_biomass', 'Total': 'total', 'Drawal Sch (Net MU)': 'drawal_sch', 'Act Drawal (Net MU)': 'act_drawal', 'UI (Net MU)': 'ui', 'Requirement (Net MU)': 'requirement', 'Shortage (Net MU)': 'shortage', 'Consumption (Net MU)': 'consumption' }
            sub_2A_renamed = sub_2A.rename(columns=column_mapping_2A)
            sub_2A_filtered = sub_2A_renamed[[col for col in column_mapping_2A.values() if col in sub_2A_renamed.columns]]
            combined_json_data['nrldc_table_2A'] = sub_2A_filtered.to_dict(orient='records')
            self.write("✅ Table 2(A) extracted for combined JSON.", style=self.style.SUCCESS)
            for _, row_data in sub_2A_filtered.iterrows():
                try:
                    _, created = Nrldc2AData.objects.update_or_create(report_date=report_date, state=self._safe_string(row_data.get('state')), defaults={ 'thermal': self._safe_float(row_data.get('thermal')), 'hydro': self._safe_float(row_data.get('hydro')), 'gas_naptha_diesel': self._safe_float(row_data.get('gas_naptha_diesel')), 'solar': self._safe_float(row_data.get('solar')), 'wind': self._safe_float(row_data.get('wind')), 'other_biomass': self._safe_float(row_data.get('other_biomass')), 'total': self._safe_float(row_data.get('total')), 'drawal_sch': self._safe_float(row_data.get('drawal_sch')), 'act_drawal': self._safe_float(row_data.get('act_drawal')), 'ui': self._safe_float(row_data.get('ui')), 'requirement': self._safe_float(row_data.get('requirement')), 'shortage': self._safe_float(row_data.get('shortage')), 'consumption': self._safe_float(row_data.get('consumption')) })
                except Exception as e:
                    self.write(f"❌ Error saving Table 2A row to DB (State: {row_data.get('state')}): {e}", level='error', style=self.style.ERROR)
            self.write("✅ Table 2(A) data saved to database.", style=self.style.SUCCESS)
        else:
            self.write("⚠️ Table 2(A) not found or extraction failed.", level='warning', style=self.style.WARNING)

        # Extract Table 2(C)
        sub_2C = self.extract_subtable_by_markers(all_content_df_cleaned, r"2\s*\(C\)\s*State's\s*Demand\s*Met\s*in\s*MWs.*", r"3\s*\(A\).*", header_row_count=2, debug_table_name="Table 2(C)")
        if sub_2C is not None:
            column_mapping_2C = { 'State': 'state', 'Maximum Demand Met of the day': 'max_demand', 'Time': 'time_max', 'Shortage during maximum demand': 'shortage_during', 'Requirement at maximum demand': 'req_max_demand', 'Maximum requirement of the day': 'max_req_day', 'Time.1': 'time_max_req', 'Shortage during maximum requirement': 'shortage_max_req', 'Demand Met at maximum Requirement': 'demand_met_max_req', 'Min Demand Met': 'min_demand_met', 'Time.2': 'time_min_demand', 'ACE_MAX': 'ace_max', 'ACE_MIN': 'time_ace_max', 'Time.3': 'ace_min', 'Time.4': 'time_ace_min' }
            sub_2C_renamed = sub_2C.rename(columns=column_mapping_2C)
            sub_2C_filtered = sub_2C_renamed[[col for col in column_mapping_2C.values() if col in sub_2C_renamed.columns]]
            combined_json_data['nrldc_table_2C'] = sub_2C_filtered.to_dict(orient='records')
            self.write("✅ Table 2(C) extracted for combined JSON.", style=self.style.SUCCESS)
            for _, row_data in sub_2C_filtered.iterrows():
                try:
                    _, created = Nrldc2CData.objects.update_or_create(report_date=report_date, state=self._safe_string(row_data.get('state')), defaults={ 'max_demand': self._safe_float(row_data.get('max_demand')), 'time_max': self._safe_string(row_data.get('time_max')), 'shortage_during': self._safe_float(row_data.get('shortage_during')), 'req_max_demand': self._safe_float(row_data.get('req_max_demand')), 'max_req_day': self._safe_float(row_data.get('max_req_day')), 'time_max_req': self._safe_string(row_data.get('time_max_req')), 'shortage_max_req': self._safe_float(row_data.get('shortage_max_req')), 'demand_met_max_req': self._safe_float(row_data.get('demand_met_max_req')), 'min_demand_met': self._safe_float(row_data.get('min_demand_met')), 'time_min_demand': self._safe_string(row_data.get('time_min_demand')), 'ace_max': self._safe_float(row_data.get('ace_max')), 'ace_min': self._safe_float(row_data.get('ace_min')), 'time_ace_max': self._safe_string(row_data.get('time_ace_max')), 'time_ace_min': self._safe_string(row_data.get('time_ace_min')) })
                except Exception as e:
                    self.write(f"❌ Error saving Table 2C row to DB (State: {self._safe_string(row_data.get('state'))}): {e}", level='error', style=self.style.ERROR)
            self.write("✅ Table 2(C) data saved to database.", style=self.style.SUCCESS)
        else:
            self.write("⚠️ Table 2(C) not found or extraction failed.", level='warning', style=self.style.WARNING)

        if combined_json_data:
            json_name = f"nrldc_{report_date.strftime('%d%m%Y')}.json"
            json_path = os.path.join(output_dir, json_name)
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(combined_json_data, f, indent=4, ensure_ascii=False)
            self.write(f"✅ Combined tables saved to: {json_path}", style=self.style.SUCCESS)
        else:
            self.write("⚠️ No tables were successfully extracted to create a combined JSON.", level='warning', style=self.style.WARNING)

    # ===================================================================
    # NEW CODE: This function is added to accept the --date argument
    # ===================================================================
    def add_arguments(self, parser):
        parser.add_argument(
            '--date',
            type=str,
            help='Run the script for a specific date in YYYY-MM-DD format.'
        )

    # ===================================================================
    # UPDATED `handle` METHOD
    # This now wraps your original logic to update the dashboard.
    # ===================================================================
    def handle(self, *args, **options):
        script_name = 'nrldc_report'
        job, _ = AutomationJob.objects.get_or_create(script_name=script_name)

        # 1. SET STATUS TO RUNNING at the start
        job.status = AutomationJob.Status.RUNNING
        job.last_run_time = timezone.now()
        job.log_message = "Starting process..."
        job.save()

        try:
            # --- This is where YOUR ORIGINAL `handle` logic begins ---
            
            # Determine the date to run for (from --date argument or today)
            run_date_str = options.get('date')
            if run_date_str:
                target_date = datetime.datetime.strptime(run_date_str, '%Y-%m-%d').date()
            else:
                target_date = datetime.date.today()
            
            target_date_str = target_date.strftime("%Y-%m-%d")
            project_name = "NRLDC"
            
            self.write(f"Starting NRLDC process for date: {target_date_str}", style=self.style.NOTICE)

            if Nrldc2AData.objects.filter(report_date=target_date).exists() or \
               Nrldc2CData.objects.filter(report_date=target_date).exists():
                message = f"✅ Pass: Data for {target_date_str} already exists. Skipping."
                self.write(message, style=self.style.SUCCESS)
                # If data exists, it's a success from the dashboard's perspective
                job.status = AutomationJob.Status.SUCCESS
                job.is_data_available_today = (target_date == datetime.date.today())
                job.log_message = f"Data for {target_date_str} already exists."
                job.save()
                return

            url = f"https://nrldc.in/get-documents-list/111?start_date={target_date_str}&end_date={target_date_str}"
            headers = { "User-Agent": "Mozilla/5.0", "Accept": "application/json", "X-Requested-With": "XMLHttpRequest", "Referer": "https://nrldc.in/reports/daily-psp" }

            self.write(f"🌐 Fetching NRDC report metadata for {target_date_str}...")
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            data = response.json()

            if data.get("recordsFiltered", 0) == 0:
                message = f"⚠️ No report available for {target_date_str}."
                self.write(message, level='warning', style=self.style.WARNING)
                raise CommandError(message) # This will be caught and logged to the dashboard

            file_info = data["data"][0]
            download_url = f"https://nrldc.in/download-file?any=Reports%2FDaily%2FDaily%20PSP%20Report%2F{file_info['file_name']}"
            
            output_dir = os.path.join("downloads", project_name, f"report_{target_date.strftime('%Y%m%d')}")
            os.makedirs(output_dir, exist_ok=True)

            pdf_path = os.path.join(output_dir, f"{file_info['title']}.pdf")
            self.write(f"⬇️ Attempting to download PDF to: {pdf_path}")
            
            pdf_response = requests.get(download_url, headers=headers)
            pdf_response.raise_for_status()
            with open(pdf_path, "wb") as f:
                f.write(pdf_response.content)
            self.write(f"✅ Downloaded report to: {pdf_path}", style=self.style.SUCCESS)

            self.extract_tables_from_pdf(pdf_path, output_dir, target_date)

            # --- Your original logic ends here ---

            # 2. IF SUCCESSFUL, UPDATE STATUS
            job.status = AutomationJob.Status.SUCCESS
            job.last_success_time = timezone.now()
            job.is_data_available_today = (target_date == datetime.date.today())
            job.log_message = f"Process completed successfully at {job.last_success_time.strftime('%Y-%m-%d %H:%M:%S')}."
            self.write('Successfully completed the process.', style=self.style.SUCCESS)

        except Exception as e:
            # 3. IF AN ERROR OCCURS, LOG IT TO THE DASHBOARD
            error_message = traceback.format_exc()
            job.status = AutomationJob.Status.FAILED
            job.log_message = str(e)  # Store a concise error for the dashboard UI
            self.write(f'An error occurred: {e}', level='error', style=self.style.ERROR)
            self.logger.error(f"Full traceback:\n{error_message}") # Log full details to file
            
        finally:
            # 4. SAVE THE FINAL STATE OF THE JOB (success or failure)
            job.save()