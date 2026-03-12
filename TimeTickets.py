import azure.functions as func
import logging
import pymssql
import pandas as pd
from io import StringIO, BytesIO
import os
import json
import gzip
from azure.storage.fileshare import ShareServiceClient
from datetime import datetime, timedelta

bp = func.Blueprint()

TRACKING_FILE_NAME = "TimeTickets_last_run.json"
CHUNK_SIZE = 100_000  # Rows per chunk - optimal for memory vs speed


def get_last_run_info(share_client):
    """Read the last run tracking file from file share"""
    try:
        file_client = share_client.get_file_client(TRACKING_FILE_NAME)
        data = file_client.download_file().readall()
        return json.loads(data)
    except Exception:
        return None


def save_last_run_info(share_client, run_info):
    """Save the last run tracking file to file share"""
    file_client = share_client.get_file_client(TRACKING_FILE_NAME)
    content = json.dumps(run_info)
    file_client.upload_file(content)


def upload_chunked_to_fileshare(file_client, query, conn, chunk_size):
    """
    Read SQL data in chunks and upload to file share as a single gzipped CSV.
    Collects all compressed data in memory, then uploads once.
    """
    total_rows = 0
    chunk_index = 0
    header_written = False
    all_gz_data = BytesIO()

    logging.info(f'Starting chunked read with chunk_size={chunk_size}')

    for chunk_df in pd.read_sql(query, conn, chunksize=chunk_size):
        rows_in_chunk = len(chunk_df)
        total_rows += rows_in_chunk
        logging.info(
            f'Processing chunk {chunk_index + 1}: {rows_in_chunk} rows '
            f'(total so far: {total_rows})'
        )

        # Convert chunk to CSV string
        csv_buffer = StringIO()
        chunk_df.to_csv(csv_buffer, index=False, header=(not header_written))
        csv_data = csv_buffer.getvalue().encode('utf-8')
        header_written = True

        # Gzip compress the chunk and append
        gz_buffer = BytesIO()
        with gzip.GzipFile(fileobj=gz_buffer, mode='wb') as gz:
            gz.write(csv_data)
        all_gz_data.write(gz_buffer.getvalue())

        chunk_index += 1
        logging.info(f'Chunk {chunk_index} processed ({len(gz_buffer.getvalue())} bytes compressed)')

    # Upload all data to file share
    if total_rows > 0:
        all_gz_data.seek(0)
        file_client.upload_file(all_gz_data.read())
        logging.info(f'All {chunk_index} chunks uploaded. Total rows: {total_rows}')

    return total_rows, chunk_index


def export_time_tickets():
    """
    Core logic: Incremental export of TimeTickets table (8M+ rows).
    - First run: Full load (chunked + compressed)
    - Subsequent runs: Delta load (last 14 days based on createdt)
    - Before 14 days: Raises RuntimeError
    Returns: (total_rows, total_chunks, file_name, share_name, load_type)
    """
    sql_connection_string = os.environ.get('SQL_CONNECTION_STRING')
    fileshare_connection_string = os.environ.get('FILESHARE_CONNECTION_STRING')
    fileshare_name = os.environ.get('FILESHARE_NAME', 'ibrt-qa-generatecsv-share')
    table_name = "TimeTickets"
    delta_days = 14

    if not sql_connection_string:
        raise ValueError("SQL_CONNECTION_STRING not configured")
    if not fileshare_connection_string:
        raise ValueError("FILESHARE_CONNECTION_STRING not configured")

    # Initialize file share client
    share_service_client = ShareServiceClient.from_connection_string(fileshare_connection_string)
    share_client = share_service_client.get_share_client(fileshare_name)

    # Check last run info
    last_run_info = get_last_run_info(share_client)
    now = datetime.now()
    is_first_run = last_run_info is None

    if not is_first_run:
        last_run_date = datetime.strptime(
            last_run_info['last_run_date'], '%Y-%m-%d %H:%M:%S'
        )
        days_since_last_run = (now - last_run_date).days

        if days_since_last_run < delta_days:
            remaining_days = delta_days - days_since_last_run
            raise RuntimeError(
                f"Cannot run yet! Last run was on {last_run_info['last_run_date']} "
                f"({days_since_last_run} days ago). "
                f"Please wait {remaining_days} more day(s) to complete the 14-day interval."
            )

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

    # Connect to SQL Server
    logging.info('Connecting to SQL Server...')
    conn = pymssql.connect(**conn_params)

    if is_first_run:
        logging.info(f'First run detected - performing FULL LOAD of {table_name}')
        query = f"SELECT * FROM {table_name}"
        load_type = "FULL_LOAD"
    else:
        delta_date = (now - timedelta(days=delta_days)).strftime('%Y-%m-%d')
        logging.info(
            f'Performing DELTA LOAD of {table_name} (createdt >= {delta_date})'
        )
        query = f"SELECT * FROM {table_name} WHERE createdt >= '{delta_date}'"
        load_type = "DELTA_LOAD"

    # Generate file name with timestamp and load type
    timestamp = now.strftime('%Y%m%d_%H%M%S')
    file_name = f"{table_name}_{load_type}_{timestamp}.csv.gz"

    # Upload using chunked + compressed approach to file share
    logging.info(f'Uploading to Azure File Share: {fileshare_name}/{file_name}')
    file_client = share_client.get_file_client(file_name)

    total_rows, total_chunks = upload_chunked_to_fileshare(
        file_client, query, conn, CHUNK_SIZE
    )
    conn.close()

    # Save run tracking info
    run_info = {
        'last_run_date': now.strftime('%Y-%m-%d %H:%M:%S'),
        'load_type': load_type,
        'rows_exported': total_rows,
        'chunks_uploaded': total_chunks,
        'file_name': file_name
    }
    save_last_run_info(share_client, run_info)

    logging.info('Data successfully uploaded and tracking info saved.')
    return total_rows, total_chunks, file_name, fileshare_name, load_type


# ============================================================
# Timer Trigger - Runs automatically at scheduled time
# Schedule configured via TIMETICKETS_SCHEDULE in local.settings.json
# Default: every day at 6:30 AM UTC -> "0 30 6 * * *"
# Timer fires daily but the 14-day check will skip if too early
# ============================================================
@bp.function_name(name="TimeTicketsTimer")
@bp.timer_trigger(schedule="%TIMETICKETS_SCHEDULE%", arg_name="timer", run_on_startup=False)
def time_tickets_timer(timer: func.TimerRequest) -> None:
    """Timer-triggered incremental export of TimeTickets table"""
    logging.info('TimeTickets TIMER triggered.')

    if timer.past_due:
        logging.warning('Timer is past due! Skipping execution.')
        return

    try:
        total_rows, total_chunks, file_name, share_name, load_type = export_time_tickets()
        logging.info(
            f'SUCCESS! [{load_type}] Exported {total_rows:,} rows from TimeTickets '
            f'in {total_chunks} chunks to {share_name}/{file_name}'
        )
    except RuntimeError as re:
        logging.warning(f'TimeTickets skipped: {str(re)}')
    except Exception as e:
        logging.error(f'TimeTickets export failed: {str(e)}')


# ============================================================
# HTTP Trigger - Manual trigger (kept for testing/on-demand)
# ============================================================
@bp.function_name(name="TimeTicketsExport")
@bp.route(route="export-timetickets", auth_level=func.AuthLevel.FUNCTION)
def time_tickets_http(req: func.HttpRequest) -> func.HttpResponse:
    """HTTP-triggered (manual) incremental export of TimeTickets table"""
    logging.info('TimeTickets HTTP triggered.')

    try:
        total_rows, total_chunks, file_name, share_name, load_type = export_time_tickets()
        return func.HttpResponse(
            f"Success! [{load_type}] Exported {total_rows:,} rows from TimeTickets "
            f"in {total_chunks} chunks to {share_name}/{file_name}",
            status_code=200
        )
    except RuntimeError as re:
        return func.HttpResponse(str(re), status_code=400)
    except pymssql.Error as db_error:
        return func.HttpResponse(f"Database error: {str(db_error)}", status_code=500)
    except Exception as e:
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)
