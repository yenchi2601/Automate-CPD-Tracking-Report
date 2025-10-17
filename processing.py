import os
import io
import zipfile
import calendar
import pandas as pd
import psycopg2
from datetime import datetime
from dotenv import load_dotenv
import smtplib
from email.message import EmailMessage
import logging
from collections import defaultdict
import tempfile
import openpyxl
from openpyxl.utils import get_column_letter
# Load .env for credentials
load_dotenv()

# PostgreSQL connection parameters from .env
DB_HOST = os.getenv("POSTGRES_HOST")
DB_NAME = os.getenv("POSTGRES_DATABASE")
DB_USER = os.getenv("POSTGRES_USERNAME")
DB_PASS = os.getenv("POSTGRES_PASSWORD")

# Email configuration
FROM_EMAIL = os.getenv("FROM_EMAIL")
CC_EMAIL = os.getenv("CC_EMAIL")
cc_recipients = [email.strip() for email in CC_EMAIL.split(",")]
SMTP_SERVER = os.getenv("SMTP_SERVER")

# === Jenkins Workspace Setup ===
workspace = os.path.normpath(os.getenv('WORKSPACE', 'D:/Jobs/Regular/R0001 - CPD Replication'))
os.makedirs(workspace, exist_ok=True)
logging.info(f"Workspace directory: {workspace}")

# Set date range for current and previous quarter
today = datetime.today()
current_quarter = (today.month - 1) // 3 + 1
prev_quarter = current_quarter - 1 if current_quarter > 1 else 4
year = today.year if current_quarter > 1 else today.year - 1

start_month = 3 * (prev_quarter - 1) + 1
end_month = start_month + 2
start_date = datetime(year, start_month, 1)
end_day = calendar.monthrange(year, end_month)[1]
end_date = datetime(year, end_month, end_day)
end_month_name = end_date.strftime("%B")

# Folder names for ZIP structure
folders = {
    "CPD": f"{start_date.date()} to {end_date.date()} CPD",
    "FNA": f"{start_date.date()} to {end_date.date()} FNA",
    "Trials": f"{start_date.date()} to {end_date.date()} Trials"
}

# Output ZIP file name
zip_filename = f"{start_date.date()} to {end_date.date()} CPD_FNA_Trials.zip"

# Sample name-to-email mapping
name_email_map = {
    "ABC, XYZ EEE": "abc@jijd.com.au",
    "HHH, NNN": "hhh.nnn@lhd.com.au"
}

# Create an in-memory ZIP file
zip_buffer = io.BytesIO()
with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zipf:

    def run_query_and_add_to_zip(query: str, group_column: str, subfolder: str, suffix: str):
        # Connect and fetch data
        with psycopg2.connect(
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS,
            host=DB_HOST,
            port="5432"
        ) as conn:
            df = pd.read_sql_query(query, conn)

        # Beautify column headers (e.g. visit_number → Visit Number)
        df.columns = [col.replace("_", " ").title() for col in df.columns]

        # Group by name and write each group to the zip
        for name, data in df.groupby(group_column.replace("_", " ").title()):
            safe_name = name.replace("/", "-").replace("\\", "-")
            filename = f"{subfolder}/{safe_name} - {suffix}.xlsx"

            with io.BytesIO() as excel_buffer:
                with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
                    sheet_name = 'All Data'
                    data.to_excel(writer, index=False, sheet_name=sheet_name)

                    # --- Auto-adjust column widths ---
                    worksheet = writer.sheets[sheet_name]
                    for i, col in enumerate(data.columns, 1):
                        max_length = max(
                            data[col].astype(str).map(len).max(),
                            len(col)
                        ) + 2  # padding
                        worksheet.column_dimensions[get_column_letter(i)].width = max_length
                    # --- End column width adjustment ---

                zipf.writestr(filename, excel_buffer.getvalue())

    # CPD SQL
    cpd_query = f"""    
    WITH site_CTE AS (
        SELECT DISTINCT
            re_event_serial,
            sl_short AS visit_site
        FROM public.reports
            LEFT JOIN case_event ON reports.re_event_serial = case_event.ce_serial
            LEFT JOIN public.sel_table ON sl_code = 'SIT' AND ce_site = sl_key
    ), event_counts AS (
        SELECT
            re_event_serial,
            COUNT(*) AS entry_count
        FROM reports
            INNER JOIN case_event ON reports.re_event_serial = case_event.ce_serial
            INNER JOIN case_staff ON reports.re_serial = case_staff.ct_key
        WHERE 
            ce_start::date BETWEEN '{start_date.date()}' AND '{end_date.date()}'
            AND reports.re_status = 'D'
            AND case_staff.ct_staff_function = 'V'
        GROUP BY re_event_serial
        HAVING COUNT(re_event_serial) >= 2
    )
    SELECT
        st_surname || ', ' || st_firstnames AS full_name,
        ROW_NUMBER() OVER (PARTITION BY reports.re_event_serial ORDER BY case_staff.ct_dor ASC) AS verifier,
        reports.re_event_serial AS visit_number,
        ce_start AS visit_date,
        visit_site,
        ce_description AS visit_description
    FROM reports
        INNER JOIN case_staff ON reports.re_serial = case_staff.ct_key
        INNER JOIN staff ON staff.st_serial = case_staff.ct_staff_serial
        INNER JOIN case_event ON re_event_serial = case_event.ce_serial
        INNER JOIN site_CTE ON reports.re_event_serial = site_CTE.re_event_serial
        INNER JOIN event_counts ON reports.re_event_serial = event_counts.re_event_serial
    WHERE 
        ce_start::date BETWEEN '{start_date.date()}' AND '{end_date.date()}'
        AND reports.re_status = 'D'
        AND case_staff.ct_staff_function = 'V'
    GROUP BY 
        reports.re_event_serial, 
        ce_start, 
        visit_site, 
        ce_description, 
        st_firstnames, 
        st_surname, 
        case_staff.ct_dor
    ORDER BY visit_date DESC;
    """
    run_query_and_add_to_zip(cpd_query, 'full_name', folders["CPD"], 'CPD')

    # FNA SQL
    fna_query = f"""    
    WITH site_cte AS (SELECT * FROM sel_table WHERE sl_code = 'SIT'),
    case_type_cte AS (SELECT * FROM sel_table WHERE sl_code = 'CT'),
    job_class_cte AS (SELECT * FROM sel_table WHERE sl_code = 'JC')

    SELECT
        st_surname || ', ' || st_firstnames AS coding_radiographer_name,
        ce_serial AS visit_number,
        ce_start AS visit_date,
        TRIM(UPPER(case_type_cte.sl_short)) AS case_type,
        TRIM(UPPER(site_cte.sl_short)) AS visit_site,
        TRIM(UPPER(ce_description)) AS visit_description,
        st_job_class,
        TRIM(UPPER(job_class_cte.sl_description)) AS job_class

    FROM case_main
        INNER JOIN case_event ON cs_serial = ce_cs_serial
        INNER JOIN case_procedure ON ce_serial = cx_ce_serial
        INNER JOIN reports ON ce_serial = re_event_serial
        INNER JOIN case_staff ON reports.re_serial = case_staff.ct_key
        INNER JOIN staff ON staff.st_serial = case_staff.ct_staff_serial
        INNER JOIN exams ON cx_key = ex_serial
        INNER JOIN site_cte ON ce_site = site_cte.sl_key
        INNER JOIN case_type_cte ON cs_type = case_type_cte.sl_key
        INNER JOIN job_class_cte ON st_job_class = job_class_cte.sl_key

    WHERE
        ce_start::date BETWEEN '{start_date.date()}' AND '{end_date.date()}'
        AND ex_code IN ('30075', '30075A', '31533', '31533A', '31533B', '31533D', '31533E', '31536', '31536A', 
                        '31536B', '31548', '31548A', '31548B', '31548C', '31548D', '55066', '55071')
        AND st_job_class = 'MC'

    GROUP BY
        visit_number,
        visit_date,
        case_type,
        visit_site,
        visit_description,
        coding_radiographer_name,
        st_job_class,
        job_class
        
    ORDER BY
        coding_radiographer_name,
        visit_number,
        case_type,
        visit_site,
        visit_description,
        st_job_class,
        job_class
        ;
    """
    run_query_and_add_to_zip(fna_query, 'coding_radiographer_name', folders["FNA"], 'FNA Intervention')

    # Trials SQL
    trials_query = f"""    
    WITH 
        ce_site_cte AS (SELECT sl_key, sl_short FROM sel_table WHERE sl_code = 'SIT'),				   
        case_type_cte AS (SELECT sl_key, sl_short FROM sel_table WHERE sl_code = 'CT'),
        job_class_cte AS (SELECT * FROM sel_table WHERE sl_code = 'JC')
        
    SELECT	
        TRIM(UPPER(st_surname || ', ' || st_firstnames)) AS coding_radiographer_name,
        case_event.ce_serial AS visit_number,
        ce_start AS visit_date,
        TRIM(UPPER(case_type_cte.sl_short)) AS case_type,
        TRIM(UPPER(ce_site_cte.sl_short)) AS visit_site,
        TRIM(UPPER(ce_description)) AS visit_description,
        st_job_class,
        TRIM(UPPER(job_class_cte.sl_description)) AS job_class
        
    FROM case_main
        INNER JOIN case_event ON cs_serial = ce_cs_serial
        INNER JOIN reports ON case_event.ce_serial = re_event_serial
        INNER JOIN case_procedure ON ce_serial = cx_ce_serial
        INNER JOIN exams ON cx_key = ex_serial
        INNER JOIN case_staff ON reports.re_serial = case_staff.ct_key
        INNER JOIN staff ON staff.st_serial = case_staff.ct_staff_serial
        
        --CTEs
        INNER JOIN ce_site_cte ON ce_site = ce_site_cte.sl_key
        INNER JOIN case_type_cte ON cs_type = case_type_cte.sl_key
        INNER JOIN job_class_cte ON st_job_class = job_class_cte.sl_key
    WHERE
        ce_start::date BETWEEN '{start_date.date()}' AND '{end_date.date()}'
        AND case_staff.ct_staff_function = 'V'
        AND cs_type IN ('TO', 'TR')

    GROUP BY
        coding_radiographer_name,
        visit_number,
        visit_date,
        case_type,
        visit_site,
        visit_description,
        cs_type,
        st_job_class,
        job_class
    ;
    """
    run_query_and_add_to_zip(trials_query, 'coding_radiographer_name', folders["Trials"], 'Trials')

# Save final ZIP file to disk
with open(zip_filename, "wb") as f:
    f.write(zip_buffer.getvalue())

print("ZIP file created:", zip_filename)

# Compose and send individual emails
html_body = f"""
    <html>
    <head>
        <style>
            body {{
                font-family: Arial, sans-serif;
                line-height: 1.6;
            }}
            h2 {{
                color: #2e6c80;
            }}
        </style>
    </head>
    <body>
        <p>Hi,</p>
        <p>This is an automated email from the Data Team related to regular CPD, US FNA interventional procedures and trial related work if relevant.</p>
        <p><b>Please find attached the file relating to CPD Period:</b> {start_date.date()} to {end_date.date()}</p>
        <p>If you have any questions or need further assistance, please feel free to contact the Cathy Lunnay or Data Team via Teams or email.</p>
        <p>Regards,<br>Data Team</p>
    </body>
    </html>
"""
with zipfile.ZipFile(zip_filename, 'r') as zipf:
    doctor_files = defaultdict(list)
    for file in zipf.namelist():
        for name in name_email_map:
            if name.replace("/", "-") in file:
                doctor_files[name].append(file)
                break

    for doctor_name, files in doctor_files.items():
        email = name_email_map.get(doctor_name)
        if not email:
            continue

        # Create a temporary zip file for this doctor
        with tempfile.NamedTemporaryFile(delete=False, suffix=".zip") as temp_zip_file:
            with zipfile.ZipFile(temp_zip_file, 'w', zipfile.ZIP_DEFLATED) as doctor_zip:
                for file in files:
                    # Read file content from main zip
                    with zipf.open(file) as file_data:
                        content = file_data.read()
                        # Write to doctor's zip with same filename
                        doctor_zip.writestr(os.path.basename(file), content)

            temp_zip_file_path = temp_zip_file.name

        # Prepare email
        msg = EmailMessage()
        msg['Subject'] = f"CPD {today.year} Round {prev_quarter}"
        msg['From'] = FROM_EMAIL
        msg['To'] = email
        msg['Cc'] = ", ".join(cc_recipients)
        msg.set_content(f"Attached CPD/FNA/Trials report(s) for {doctor_name} from {start_date.date()} to {end_date.date()}.")
        msg.add_alternative(html_body, subtype='html')

        # Attach the doctor zip
        with open(temp_zip_file_path, 'rb') as f:
            msg.add_attachment(
                f.read(),
                maintype='application',
                subtype='zip',
                filename=f"{doctor_name.replace('/', '-')}_CPD_FNA_Trials.zip"
            )

        try:
            with smtplib.SMTP(SMTP_SERVER) as server:
                server.send_message(msg)
            print(f"Email sent to {doctor_name} at {email}")
        except Exception as e:
            print(f"Failed to send email to {doctor_name}: {e}")
        # Clean up temp file
        os.remove(temp_zip_file_path)
