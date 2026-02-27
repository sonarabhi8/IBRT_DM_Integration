import azure.functions as func
import logging
import pyodbc
import pandas as pd
from io import StringIO, BytesIO
import os
import json
import gzip
import uuid
import base64
from azure.storage.blob import BlobServiceClient, BlobBlock
from datetime import datetime, timedelta

bp = func.Blueprint()

TRACKING_BLOB_NAME = "TimeTickets_last_run.json"
CHUNK_SIZE = 100_000  # Rows per chunk - optimal for memory vs speed


def get_last_run_info(blob_service_client, container_name):
    """Read the last run tracking file from blob storage"""
    try:
        blob_client = blob_service_client.get_blob_client(
            container=container_name, blob=TRACKING_BLOB_NAME
        )
        data = blob_client.download_blob().readall()
        return json.loads(data)
    except Exception:
        return None


def save_last_run_info(blob_service_client, container_name, run_info):
    """Save the last run tracking file to blob storage"""
    blob_client = blob_service_client.get_blob_client(
        container=container_name, blob=TRACKING_BLOB_NAME
    )
    blob_client.upload_blob(json.dumps(run_info), overwrite=True)


def generate_block_id(index):
    """Generate a unique block ID for block blob upload"""
    block_id = f"block-{index:06d}"
    return base64.b64encode(block_id.encode()).decode()


def upload_chunked_to_blob(blob_client, query, conn, chunk_size):
    """
    Read SQL data in chunks and upload to blob as a single gzipped CSV.
    Uses Block Blob API to upload in parts without holding all data in memory.
    """
    block_list = []
    total_rows = 0
    chunk_index = 0
    header_written = False

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

        # Gzip compress the chunk
        gz_buffer = BytesIO()
        with gzip.GzipFile(fileobj=gz_buffer, mode='wb') as gz:
            gz.write(csv_data)
        gz_data = gz_buffer.getvalue()

        # Upload as a block
        block_id = generate_block_id(chunk_index)
        blob_client.stage_block(block_id=block_id, data=gz_data, length=len(gz_data))
        block_list.append(BlobBlock(block_id=block_id))

        chunk_index += 1
        logging.info(f'Chunk {chunk_index} uploaded ({len(gz_data)} bytes compressed)')

    # Commit all blocks as one blob
    if block_list:
        blob_client.commit_block_list(block_list)
        logging.info(f'All {chunk_index} blocks committed. Total rows: {total_rows}')

    return total_rows, chunk_index


def export_time_tickets():
    """
    Core logic: Incremental export of TimeTickets table (8M+ rows).
    - First run: Full load (chunked + compressed)
    - Subsequent runs: Delta load (last 14 days based on createdt)
    - Before 14 days: Raises RuntimeError
    Returns: (total_rows, total_chunks, blob_name, container_name, load_type)
    """
    sql_connection_string = os.environ.get('SQL_CONNECTION_STRING')
    blob_connection_string = os.environ.get('BLOB_CONNECTION_STRING')
    container_name = os.environ.get('BLOB_CONTAINER_NAME', 'ibrttest')
    table_name = "TimeTickets"
    delta_days = 14

    if not sql_connection_string:
        raise ValueError("SQL_CONNECTION_STRING not configured")
    if not blob_connection_string:
        raise ValueError("BLOB_CONNECTION_STRING not configured")

    # Initialize blob service
    blob_service_client = BlobServiceClient.from_connection_string(blob_connection_string)

    # Check last run info
    last_run_info = get_last_run_info(blob_service_client, container_name)
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

    # Connect to SQL Server
    logging.info('Connecting to SQL Server...')
    conn = pyodbc.connect(sql_connection_string)

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

    # Generate blob name with timestamp and load type
    timestamp = now.strftime('%Y%m%d_%H%M%S')
    blob_name = f"{table_name}_{load_type}_{timestamp}.csv.gz"

    # Upload using chunked + compressed approach
    logging.info(f'Uploading to Azure Blob Storage: {container_name}/{blob_name}')
    blob_client = blob_service_client.get_blob_client(
        container=container_name, blob=blob_name
    )

    total_rows, total_chunks = upload_chunked_to_blob(
        blob_client, query, conn, CHUNK_SIZE
    )
    conn.close()

    # Save run tracking info
    run_info = {
        'last_run_date': now.strftime('%Y-%m-%d %H:%M:%S'),
        'load_type': load_type,
        'rows_exported': total_rows,
        'chunks_uploaded': total_chunks,
        'blob_name': blob_name
    }
    save_last_run_info(blob_service_client, container_name, run_info)

    logging.info('Data successfully uploaded and tracking info saved.')
    return total_rows, total_chunks, blob_name, container_name, load_type


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
        total_rows, total_chunks, blob_name, container, load_type = export_time_tickets()
        logging.info(
            f'SUCCESS! [{load_type}] Exported {total_rows:,} rows from TimeTickets '
            f'in {total_chunks} chunks to {container}/{blob_name}'
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
        total_rows, total_chunks, blob_name, container, load_type = export_time_tickets()
        return func.HttpResponse(
            f"Success! [{load_type}] Exported {total_rows:,} rows from TimeTickets "
            f"in {total_chunks} chunks to {container}/{blob_name}",
            status_code=200
        )
    except RuntimeError as re:
        return func.HttpResponse(str(re), status_code=400)
    except pyodbc.Error as db_error:
        return func.HttpResponse(f"Database error: {str(db_error)}", status_code=500)
    except Exception as e:
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)
