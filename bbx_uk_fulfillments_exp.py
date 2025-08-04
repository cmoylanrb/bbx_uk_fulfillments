import os
import csv
import paramiko
from google.cloud import bigquery
from google.auth import default
from google.oauth2.service_account import Credentials
from datetime import datetime, timezone
import requests
import re
import logging
from tenacity import retry, stop_after_attempt, wait_exponential, wait_fixed
import functions_framework
from google.cloud import secretmanager
import google.cloud.logging
from google.cloud.logging.handlers import CloudLoggingHandler
import tempfile
import io

# Environment detection
def is_cloud_function():
    """Check if running in Google Cloud Functions environment."""
    return os.getenv('K_SERVICE') is not None

# Load .env file only for local development
if not is_cloud_function():
    from dotenv import load_dotenv
    load_dotenv()

# Secret Manager functions
def get_secret_manager_client():
    """Get Secret Manager client with appropriate credentials."""
    try:
        # For Cloud Functions, use default credentials
        client = secretmanager.SecretManagerServiceClient()
        return client
    except Exception as e:
        logging.warning(f"Could not initialize Secret Manager client: {e}")
        return None

def get_secret(secret_name, project_id):
    """Retrieve a secret from Google Cloud Secret Manager or .env file."""
    if is_cloud_function():
        # In Cloud Functions, try Secret Manager first
        client = get_secret_manager_client()
        if client:
            try:
                name = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
                response = client.access_secret_version(request={"name": name})
                return response.payload.data.decode("UTF-8")
            except Exception as e:
                logging.warning(f"Could not retrieve secret {secret_name}: {e}")
    
    # Fallback to environment variable (from .env or Cloud Function env vars)
    env_var = secret_name.replace('-', '_').upper()
    return os.getenv(env_var)

# --- Logging Setup ---
log_level = os.getenv('LOG_LEVEL', 'INFO')

if is_cloud_function():
    # Use Google Cloud Logging in Cloud Functions
    client = google.cloud.logging.Client()
    client.setup_logging(log_level=getattr(logging, log_level))
else:
    # Use basic logging for local development
    logging.basicConfig(
        level=getattr(logging, log_level),
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

# ------------------------------------------------------------------------------
# Configuration Variables
# ------------------------------------------------------------------------------

# BigQuery Configuration
BQ_PROJECT = os.getenv('BQ_PROJECT')
BQ_DATASET = os.getenv('BQ_DATASET')
BQ_CREDPATH = os.getenv('BQ_CREDPATH')
BQ_SCOPES = os.getenv('BQ_SCOPES').split(',') if os.getenv('BQ_SCOPES') else []
BQ_BATCH_SIZE = int(os.getenv('BQ_BATCH_SIZE', '500'))

# Tables / Views
BQ_FULFILLMENT_LINES_VIEW = os.getenv('BQ_FULFILLMENT_LINES_VIEW')
BQ_FULFILLMENT_LIST_VIEW = os.getenv('BQ_FULFILLMENT_LIST_VIEW')
BQ_STATUS_TABLE = os.getenv('BQ_STATUS_TABLE')
BQ_WAVE_TABLE = os.getenv('BQ_WAVE_TABLE')

# Shopify Configuration
SHOPIFY_API_VERSION = os.getenv('SHOPIFY_API_VERSION', '2024-10')
SHOPIFY_ACCESS_TOKEN = get_secret('shopify-access-token', BQ_PROJECT) or os.getenv('SHOPIFY_ACCESS_TOKEN')
SHOPIFY_STORE_URL = os.getenv('SHOPIFY_STORE_URL')
SHOPIFY_API_URL = f"{SHOPIFY_STORE_URL}/admin/api/{SHOPIFY_API_VERSION}/graphql.json"
SHOPIFY_LOCATION_ID = os.getenv('SHOPIFY_LOCATION_ID')
SHOPIFY_FF_MESSAGE = os.getenv('SHOPIFY_FF_MESSAGE')
SHOPIFY_BATCH_SIZE = int(os.getenv('SHOPIFY_BATCH_SIZE', '50'))
SHOPIFY_HEADERS = {
    "Content-Type": "application/json",
    "X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN,
}

# SFTP Configuration
SFTP_HOST = os.getenv('SFTP_HOST')
SFTP_PORT = int(os.getenv('SFTP_PORT', '22'))
SFTP_USER = get_secret('sftp-user', BQ_PROJECT) or os.getenv('SFTP_USER')
SFTP_PASSWORD = get_secret('sftp-password', BQ_PROJECT) or os.getenv('SFTP_PASSWORD')
SFTP_UPLOAD_PATH = os.getenv('SFTP_UPLOAD_PATH')
PROLOG_FNAME_TEMPLATE = os.getenv('PROLOG_FNAME_TEMPLATE')
TSV_OUTPUT_PATH = os.getenv('TSV_OUTPUT_PATH')
MAX_RETRIES = int(os.getenv('MAX_RETRIES', '30'))

# ------------------------------------------------------------------------------
# BigQuery Client Initialization
# ------------------------------------------------------------------------------
# Initialize BigQuery client with appropriate credentials
if is_cloud_function():
    # Use default credentials (for Cloud Functions) but with explicit scopes
    logging.info("Using default credentials for BigQuery client (cloud)...")
    BQ_CREDS, _ = default(scopes=BQ_SCOPES)
    BQ_CLIENT = bigquery.Client(credentials=BQ_CREDS, project=BQ_PROJECT)
else:
    # Use service account file for local development
    logging.info("Using service account file for BigQuery client (local)...")
    BQ_CREDS = Credentials.from_service_account_file(BQ_CREDPATH, scopes=BQ_SCOPES)
    BQ_CLIENT = bigquery.Client(credentials=BQ_CREDS, project=BQ_PROJECT)

def get_file_name(is_box_orders=False):
    def pad(s):
        return f"{s:02}"
    d = datetime.now(timezone.utc)
    date_str = f"{d.year}{pad(d.month)}{pad(d.day)}{pad(d.hour)}{pad(d.minute)}{pad(d.second)}"
    filename = PROLOG_FNAME_TEMPLATE.format(filename_string=date_str, is_box_orders="M" if is_box_orders else "")
    return filename

def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def upsert_order_status(rows, status):
    total_orders = len(rows)
    processed_orders_count = 0
    for batch in chunks(rows, BQ_BATCH_SIZE):
        # Deduplicate based on fulfillment_order_id
        unique_orders = {row['fulfillment_order_id']: row for row in batch}
        unique_orders_count = len(unique_orders)
        logging.info(f"Processing {unique_orders_count} unique orders...")

        values_clause = []
        query_parameters = []

        for index, row in enumerate(unique_orders.values()):
            fulfillment_order_id = row['fulfillment_order_id']
            assigned_location_id = row.get('assigned_location_id', None)
            email = row.get('email', '')
            order_id = row['order_id']
            order_name = row['order_name']
            retries = row.get('retries', 0)

            values_clause.append(f"""
            SELECT 
                @fulfillment_order_id_{index} AS fulfillment_order_id, 
                @assigned_location_id_{index} AS assigned_location_id, 
                @email_{index} AS email, 
                @order_id_{index} AS order_id, 
                @order_name_{index} AS order_name, 
                @status_{index} AS status, 
                @retries_{index} AS retries,
                CURRENT_TIMESTAMP() AS created_at, 
                CURRENT_TIMESTAMP() AS updated_at
            """)

            query_parameters.extend([
                bigquery.ScalarQueryParameter(f"fulfillment_order_id_{index}", "INTEGER", fulfillment_order_id),
                bigquery.ScalarQueryParameter(f"assigned_location_id_{index}", "INTEGER", assigned_location_id),
                bigquery.ScalarQueryParameter(f"email_{index}", "STRING", email),
                bigquery.ScalarQueryParameter(f"order_id_{index}", "INTEGER", order_id),
                bigquery.ScalarQueryParameter(f"order_name_{index}", "STRING", order_name),
                bigquery.ScalarQueryParameter(f"status_{index}", "STRING", status),
                bigquery.ScalarQueryParameter(f"retries_{index}", "INTEGER", retries),
            ])

        merge_query = f"""
        MERGE 
          `{BQ_STATUS_TABLE}` T
        USING (
          {" UNION ALL ".join(values_clause)}
        ) S ON 
            T.fulfillment_order_id = S.fulfillment_order_id
        WHEN MATCHED THEN
          UPDATE SET 
            assigned_location_id = S.assigned_location_id,
            email = S.email,
            order_id = S.order_id,
            order_name = S.order_name,
            status = S.status,
            retries = S.retries,
            updated_at = S.updated_at
        WHEN NOT MATCHED THEN
          INSERT (fulfillment_order_id, assigned_location_id, email, order_id, order_name, status, retries, created_at, updated_at)
          VALUES (S.fulfillment_order_id, S.assigned_location_id, S.email, S.order_id, S.order_name, S.status, S.retries, S.created_at, S.updated_at)
        """
        try:
            job_config = bigquery.QueryJobConfig(query_parameters=query_parameters)
            result = BQ_CLIENT.query(merge_query, job_config=job_config).result()
            processed_orders_count += unique_orders_count
            logging.info(f"Processed {processed_orders_count} out of {total_orders} orders. Batch of {unique_orders_count} fulfillment orders upserted with status {status} in BigQuery")
        except Exception as e:
            logging.error(f"Failed to upsert batch of fulfillment orders: {e}")

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=3))
def insert_order_wave(rows):
    field_names = rows[0].keys()
    for batch in chunks(rows, BQ_BATCH_SIZE):
        rows_to_insert = [{field: row[field] for field in field_names} for row in batch]
        try:
            errors = BQ_CLIENT.insert_rows_json(BQ_WAVE_TABLE, rows_to_insert)
            if errors:
                logging.error(f"Errors inserting batch into BigQuery: {errors}. Failed batch data: {rows_to_insert}")
                return False
            logging.info(f"Successfully inserted batch of {len(rows_to_insert)} rows into {BQ_WAVE_TABLE}.")
        except Exception as e:
            logging.error(f"Exception occurred while inserting batch into BigQuery: {e}. Failed batch data: {rows_to_insert}")
            return False
    return True

@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=2, max=10))
def execute_graphql_query(query, variables, url):
    try:
        response = requests.post(url, json={"query": query, "variables": variables}, headers=SHOPIFY_HEADERS)
        response.raise_for_status()
        data = response.json()
        if "errors" in data:
            logging.warning(f"GraphQL errors: {data['errors']}")
            return None, "retry"
        return data['data'], None
    except requests.exceptions.RequestException as e:
        logging.error(f"Network-related error occurred: {e}")
        raise

def accept_orders_graphql(orders):

    def log_and_update_status(orders, status):
        if orders:
            logging.info(f"{len(orders)} orders marked as {status}.")
            upsert_order_status(orders, status=status)

    # Separate orders that have reached max retries
    orders_to_fail = [order for order in orders if order.get('retries', 0) >= MAX_RETRIES]
    orders_to_process = [order for order in orders if order.get('retries', 0) < MAX_RETRIES]

    # Log and update status for orders that have reached max retries
    log_and_update_status(orders_to_fail, "failed")

    # Process orders
    for batch_orders in chunks(orders_to_process, SHOPIFY_BATCH_SIZE):
        mutations = []
        variables = {}

        # Prepare mutations and variables
        for index, order in enumerate(batch_orders):
            fulfillment_order_id = order['fulfillment_order_id']
            order_id = order['order_id']

            global_fulfillment_order_id = f"gid://shopify/FulfillmentOrder/{fulfillment_order_id}"
            global_order_id = f"gid://shopify/Order/{order_id}"

            # Mutation for fulfillment order acceptance
            mutations.append(f"""
            fulfillmentOrderAcceptFulfillmentRequest{index}: fulfillmentOrderAcceptFulfillmentRequest(id: $fulfillment_id_{index}, message: $message) {{
              fulfillmentOrder {{
                id
                status
                requestStatus
              }}
              userErrors {{
                field
                message
              }}
            }}
            """)

            # Mutation for updating the order
            mutations.append(f"""
            orderUpdate{index}: orderUpdate(input: {{id: $order_id_{index}, poNumber: ""}}) {{
              order {{
                id
              }}
              userErrors {{
                field
                message
              }}
            }}
            """)

            variables[f"fulfillment_id_{index}"] = global_fulfillment_order_id
            variables[f"order_id_{index}"] = global_order_id

        # Execute GraphQL query
        query = f"""
        mutation (
            {" ".join([f"$fulfillment_id_{i}: ID!, $order_id_{i}: ID!" for i in range(len(batch_orders))])}
            $message: String!
        ) {{
            {' '.join(mutations)}
        }}
        """
        variables["message"] = SHOPIFY_FF_MESSAGE
        data, status = execute_graphql_query(query, variables, SHOPIFY_API_URL)

        if status == "retry":
            logging.warning("Retrying batch due to GraphQL errors.")
            for order in batch_orders:
                order['retries'] = order.get('retries', 0) + 1
            log_and_update_status(batch_orders, "retry")
            continue

        if data:
            successful_orders = []
            failed_orders = []
            accepted_not_open_orders = []

            for index, order in enumerate(batch_orders):
                fulfillment_result = data.get(f"fulfillmentOrderAcceptFulfillmentRequest{index}")

                if fulfillment_result and not fulfillment_result.get("userErrors"):
                    successful_orders.append(order)
                elif fulfillment_result and any("not in an open state" in error['message'] for error in fulfillment_result.get("userErrors", [])):
                    accepted_not_open_orders.append(order)
                else:
                    failed_orders.append(order)

            log_and_update_status(successful_orders, "accepted")
            log_and_update_status(accepted_not_open_orders, "accepted_not_open")

            for order in failed_orders:
                order['retries'] = order.get('retries', 0) + 1

            log_and_update_status(failed_orders, "retry")

        else:
            logging.error("GraphQL request failed for batch fulfillment acceptance.")
            log_and_update_status(batch_orders, "failed")

def get_fulfillment_orders():
    query = f"""
        SELECT 
            fulfillment_order_id, 
            order_id, 
            order_name, 
            email, 
            status,
            assigned_location_id,
            retries
        FROM   
            `{BQ_FULFILLMENT_LIST_VIEW}`
        WHERE
            CAST(assigned_location_id AS INTEGER) = {SHOPIFY_LOCATION_ID}
            AND status = 'open'
            AND f_status = 'new'
            -- AND fulfillment_order_id IN ( 6173176430649, 6173206675513 )  -- TEMP
        ORDER BY
            fulfillment_order_id ASC
        ;
        """
    query_job = BQ_CLIENT.query(query)
    orders = [dict(row) for row in query_job.result()]

    logging.info(f"Fetched {len(orders)} new orders.")
    for order in orders:
        logging.debug(f"Order ID: {order['order_id']}, Fulfillment Order ID: {order['fulfillment_order_id']}")

    return orders

def get_accepted_orders(is_box_order=False):
    query = f"""
        SELECT 
            * 
        FROM
            `{BQ_FULFILLMENT_LINES_VIEW}`
        WHERE
            CAST(assigned_location_id AS INTEGER) = {SHOPIFY_LOCATION_ID}
            AND is_box_order IS {'true' if is_box_order else 'false'}
            AND (status LIKE 'accepted%' OR status = 'exported')
        ORDER BY
            fulfillment_order_id ASC,
            sort ASC
        ;
        """
    query_job = BQ_CLIENT.query(query)
    orders = [dict(row) for row in query_job.result()]

    logging.info(f"Fetched {len(orders)} accepted {'box' if is_box_order else 'shop'} order lines for TSV export.")
    for order in orders:
        logging.debug(f"Order ID: {order['order_id']}, Fulfillment Order ID: {order['fulfillment_order_id']}")

    return orders

def export_to_tsv(query_results, is_box_orders=False):
    if not query_results:
        logging.info("No accepted orders to export.")
        return None

    # Create temporary file for Cloud Functions
    with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as temp_file:
        tsv_path = temp_file.name
    
    rows_to_insert = []

    pattern = re.compile(r'^\d{2,3}_')
    field_names = [field for field in query_results[0].keys() if pattern.match(field)]
    addtl_fields = ['status', 'fulfillment_order_id', 'order_id', 'order_name', 'assigned_location_id', 'created_at']
    bq_field_names = [field for field in query_results[0].keys() if pattern.match(field) or field in addtl_fields]

    with open(tsv_path, mode='w', newline='') as tsv_file:
        writer = csv.writer(tsv_file, delimiter='\t')

        for row in query_results:
            filtered_row = [row[field] for field in field_names]
            try:
                writer.writerow(filtered_row)
                row_with_timestamp = {field: row[field] for field in bq_field_names}
                row_with_timestamp['created_at'] = datetime.now(timezone.utc).isoformat()
                rows_to_insert.append(row_with_timestamp)
            except Exception as e:
                logging.error(f"Failed to export row for fulfillment order ID {row['fulfillment_order_id']}: {e}")
                return None  # Exit the function on failure

    # Upsert status as "exported"
    unique_orders_to_update = list({order['fulfillment_order_id']: order for order in query_results}.values())
    logging.info(f"Updating {len(unique_orders_to_update)} status table entries to 'exported'.")
    upsert_order_status(unique_orders_to_update, "exported")

    # Attempt to upload the TSV file to SFTP
    try:
        send_to_sftp(tsv_path, is_box_orders=is_box_orders)
        # Upsert status as "uploaded"
        logging.info(f"Updating {len(unique_orders_to_update)} status table entries to 'uploaded'.")
        upsert_order_status(unique_orders_to_update, "uploaded")
    except Exception as e:
        logging.error(f"Failed to upload TSV file to SFTP: {e}")
        # Do not update the status to allow retry
    finally:
        # Clean up temporary file
        try:
            os.unlink(tsv_path)
        except Exception as e:
            logging.warning(f"Failed to clean up temporary file {tsv_path}: {e}")

    logging.info(f"{'Box' if is_box_orders else 'Shop'} Order TSV file created and uploaded")
    return tsv_path

@retry(stop=stop_after_attempt(5), wait=wait_fixed(5))
def send_to_sftp(tsv_path, is_box_orders=False):
    plogger = paramiko.util.logging.getLogger()
    plogger.setLevel(paramiko.util.logging.INFO)
    try:
        # Generate remote filename using the same pattern as get_file_name
        filename = get_file_name(is_box_orders=is_box_orders)
        remote_filepath = os.path.join(SFTP_UPLOAD_PATH, filename)
        logging.info(f"Attempting to upload file {tsv_path} to SFTP ({remote_filepath})")
        transport = paramiko.Transport((SFTP_HOST, SFTP_PORT))
        transport.connect(username=SFTP_USER, password=SFTP_PASSWORD)
        sftp = paramiko.SFTPClient.from_transport(transport)

        # Upload the file
        sftp.put(tsv_path, remote_filepath)
        logging.info(f"File {tsv_path} successfully uploaded to SFTP ({remote_filepath})")

        # Verify the file
        local_file_size = os.path.getsize(tsv_path)
        remote_file_size = sftp.stat(remote_filepath).st_size

        if local_file_size != remote_file_size:
            raise Exception(f"File size mismatch: local size {local_file_size}, remote size {remote_file_size}")

        logging.info(f"File size verified: {local_file_size} bytes")

    except Exception as e:
        logging.error(f"Error during SFTP upload: {e}")
        raise

    finally:
        if 'sftp' in locals():
            sftp.close()
            logging.info("SFTP connection closed.")
        if 'transport' in locals():
            transport.close()
            logging.info("Transport connection closed.")

# ------------------------------------------------------------------------------
# Cloud Function Entry Point
# ------------------------------------------------------------------------------
@functions_framework.http
def process_fulfillments(request):
    """
    Google Cloud Function entry point for processing fulfillments.
    
    Args:
        request: Flask request object
        
    Returns:
        Flask response object
    """
    try:
        logging.info("Starting BBX fulfillment processing...")

        logging.info("Fetching new orders...")
        orders = get_fulfillment_orders()
        accept_orders_graphql(orders)

        # Get non-box orders
        accepted_orders = get_accepted_orders(is_box_order=False)
        if not accepted_orders:
            logging.info("No accepted non-box orders found.")
        else:
            export_to_tsv(accepted_orders, is_box_orders=False)

        # Get box orders
        accepted_orders = get_accepted_orders(is_box_order=True)
        if not accepted_orders:
            logging.info("No accepted box orders found.")
        else:
            export_to_tsv(accepted_orders, is_box_orders=True)

        logging.info("Process completed successfully.")
        return {
            'status': 'success',
            'message': 'Process completed successfully.',
            'processed_orders': len(orders) if orders else 0,
            'accepted_orders': len(accepted_orders) if accepted_orders else 0
        }, 200
        
    except Exception as e:
        logging.error(f"Error in BBX fulfillment processing: {str(e)}")
        return {
            'status': 'error',
            'message': f'Process failed: {str(e)}'
        }, 500

# ------------------------------------------------------------------------------
# Local Development Entry Point
# ------------------------------------------------------------------------------
def main(request):
    """Local development entry point."""
    return process_fulfillments(request)

if __name__ == "__main__":
    main(True)
