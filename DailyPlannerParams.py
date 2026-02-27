import azure.functions as func
import logging
import pyodbc
import pandas as pd
from io import StringIO
import os
from azure.storage.blob import BlobServiceClient
from datetime import datetime

bp = func.Blueprint()


def export_daily_planner_params():
    """
    Core logic: Extract DailyPlannerParams from SQL Server
    and upload to Azure Blob Storage as CSV. Full load every time.
    Returns: (rows_count, blob_name, container_name)
    """
    sql_connection_string = os.environ.get('SQL_CONNECTION_STRING')
    blob_connection_string = os.environ.get('BLOB_CONNECTION_STRING')
    container_name = os.environ.get('BLOB_CONTAINER_NAME', 'ibrttest')
    table_name = "DailyPlannerParams"

    if not sql_connection_string:
        raise ValueError("SQL_CONNECTION_STRING not configured")
    if not blob_connection_string:
        raise ValueError("BLOB_CONNECTION_STRING not configured")

    # Connect to SQL Server and extract data
    logging.info(f'Connecting to SQL Server and extracting data from {table_name}...')
    conn = pyodbc.connect(sql_connection_string)

    query = f"SELECT * FROM {table_name}"
    df = pd.read_sql(query, conn)
    conn.close()

    logging.info(f'Successfully extracted {len(df)} rows from {table_name}')

    # Convert DataFrame to CSV
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_content = csv_buffer.getvalue()

    # Generate blob name with timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    blob_name = f"{table_name}_{timestamp}.csv"

    # Upload to Azure Blob Storage
    logging.info(f'Uploading to Azure Blob Storage: {container_name}/{blob_name}')
    blob_service_client = BlobServiceClient.from_connection_string(blob_connection_string)
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    blob_client.upload_blob(csv_content, overwrite=True)

    logging.info('Data successfully uploaded to blob storage')
    return len(df), blob_name, container_name


# ============================================================
# Timer Trigger - Runs automatically at scheduled time
# Schedule configured via DAILYPLANNER_SCHEDULE in local.settings.json
# Default: every day at 6:00 AM UTC -> "0 0 6 * * *"
# ============================================================
@bp.function_name(name="DailyPlannerParamsTimer")
@bp.timer_trigger(schedule="%DAILYPLANNER_SCHEDULE%", arg_name="timer", run_on_startup=False)
def daily_planner_timer(timer: func.TimerRequest) -> None:
    """Timer-triggered full load of DailyPlannerParams table"""
    logging.info('DailyPlannerParams TIMER triggered.')

    if timer.past_due:
        logging.warning('Timer is past due! Skipping execution.')
        return

    try:
        rows, blob_name, container = export_daily_planner_params()
        logging.info(
            f'SUCCESS! Exported {rows} rows from DailyPlannerParams '
            f'to {container}/{blob_name}'
        )
    except Exception as e:
        logging.error(f'DailyPlannerParams export failed: {str(e)}')


# ============================================================
# HTTP Trigger - Manual trigger (kept for testing/on-demand)
# ============================================================
@bp.function_name(name="DailyPlannerParamsExport")
@bp.route(route="export-dailyplanner", auth_level=func.AuthLevel.FUNCTION)
def daily_planner_http(req: func.HttpRequest) -> func.HttpResponse:
    """HTTP-triggered (manual) full load of DailyPlannerParams table"""
    logging.info('DailyPlannerParams HTTP triggered.')

    try:
        rows, blob_name, container = export_daily_planner_params()
        return func.HttpResponse(
            f"Success! Exported {rows} rows from DailyPlannerParams to {container}/{blob_name}",
            status_code=200
        )
    except pyodbc.Error as db_error:
        return func.HttpResponse(f"Database error: {str(db_error)}", status_code=500)
    except Exception as e:
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)
