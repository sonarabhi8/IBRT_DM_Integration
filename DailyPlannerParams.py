import azure.functions as func
import logging
import pymssql
import pandas as pd
from io import StringIO
import os
from azure.storage.fileshare import ShareServiceClient
from datetime import datetime

bp = func.Blueprint()


def export_daily_planner_params():
    """
    Core logic: Extract DailyPlannerParams from SQL Server
    and upload to Azure File Share as CSV. Full load every time.
    Returns: (rows_count, file_name, share_name)
    """
    sql_connection_string = os.environ.get('SQL_CONNECTION_STRING')
    fileshare_connection_string = os.environ.get('FILESHARE_CONNECTION_STRING')
    fileshare_name = os.environ.get('FILESHARE_NAME', 'ibrt-qa-generatecsv-share')
    table_name = "DailyPlannerParams"

    if not sql_connection_string:
        raise ValueError("SQL_CONNECTION_STRING not configured")
    if not fileshare_connection_string:
        raise ValueError("FILESHARE_CONNECTION_STRING not configured")

    # Parse connection string for pymssql
    conn_params = {}
    for part in sql_connection_string.split(';'):
        if '=' in part:
            key, value = part.split('=', 1)
            key = key.strip()
            if key.upper() == 'SERVER':
                # Handle server with port (format: server,port or server:port)
                if ',' in value:
                    server, port = value.split(',', 1)
                    conn_params['server'] = server.strip()
                    conn_params['port'] = int(port.strip())
                elif ':' in value:
                    server, port = value.split(':', 1)
                    conn_params['server'] = server.strip()
                    conn_params['port'] = int(port.strip())
                else:
                    conn_params['server'] = value.strip()
            elif key.upper() == 'INITIAL CATALOG':
                conn_params['database'] = value.strip()
            elif key.upper() == 'USER ID':
                conn_params['user'] = value.strip()
            elif key.upper() == 'PASSWORD':
                conn_params['password'] = value.strip()

    # Connect to SQL Server and extract data
    logging.info(f'Connecting to SQL Server and extracting data from {table_name}...')
    conn = pymssql.connect(**conn_params)

    query = f"SELECT * FROM {table_name}"
    df = pd.read_sql(query, conn)
    conn.close()

    logging.info(f'Successfully extracted {len(df)} rows from {table_name}')

    # Convert DataFrame to CSV
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_content = csv_buffer.getvalue()

    # Generate file name with timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    file_name = f"{table_name}_{timestamp}.csv"

    # Upload to Azure File Share
    logging.info(f'Uploading to Azure File Share: {fileshare_name}/{file_name}')
    share_service_client = ShareServiceClient.from_connection_string(fileshare_connection_string)
    file_client = share_service_client.get_share_client(fileshare_name).get_file_client(file_name)
    file_client.upload_file(csv_content)
    logging.info('Data successfully uploaded to file share')

    return len(df), file_name, fileshare_name


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
        rows, file_name, share_name = export_daily_planner_params()
        logging.info(
            f'SUCCESS! Exported {rows} rows from DailyPlannerParams '
            f'to {share_name}/{file_name}'
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
        rows, file_name, share_name = export_daily_planner_params()
        return func.HttpResponse(
            f"Success! Exported {rows} rows from DailyPlannerParams to {share_name}/{file_name}",
            status_code=200
        )
    except pymssql.Error as db_error:
        return func.HttpResponse(f"Database error: {str(db_error)}", status_code=500)
    except Exception as e:
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)
