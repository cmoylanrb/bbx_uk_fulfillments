import os
import csv
import uuid
import paramiko
from google.cloud import bigquery
from google.oauth2.service_account import Credentials
from datetime import datetime, timezone
import simplejson as json
import requests
import logging
from tenacity import retry, stop_after_attempt, wait_exponential
import functions_framework
from google.cloud import secretmanager
import google.cloud.logging
from google.cloud.logging.handlers import CloudLoggingHandler

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
    os.environ["GRPC_VERBOSITY"] = "ERROR"

# ------------------------------------------------------------------------------
# Configuration Variables
# ------------------------------------------------------------------------------

# BigQuery Configuration
BQ_PROJECT = os.getenv('BQ_PROJECT')
BQ_DATASET = os.getenv('BQ_DATASET')
BQ_CREDPATH = os.getenv('BQ_CREDPATH')
BQ_SCOPES = [
    'https://www.googleapis.com/auth/cloud-platform', 
    'https://www.googleapis.com/auth/drive'         
]
BQ_BATCH_SIZE = int(os.getenv('BQ_BATCH_SIZE', '20'))

# Tables / Views
BQ_SHIPMENTS_TABLE = f"{BQ_DATASET}.bbx_wh_shipments_uk"
BQ_STATUS_TABLE = f"{BQ_DATASET}.bbx_wh_fulfillments_uk"
BQ_SHIPMENTS_VIEW = f"{BQ_DATASET}.bbx_wh_shipview_uk"

# Shopify Configuration
SHOPIFY_API_VERSION = os.getenv('SHOPIFY_API_VERSION', '2024-10')
SHOPIFY_ACCESS_TOKEN = get_secret('shopify-access-token', BQ_PROJECT) or os.getenv('SHOPIFY_ACCESS_TOKEN')
SHOPIFY_STORE_URL = os.getenv('SHOPIFY_STORE_URL')
SHOPIFY_API_URL = f"{SHOPIFY_STORE_URL}/admin/api/{SHOPIFY_API_VERSION}/graphql.json"
SHOPIFY_LOCATION_ID = os.getenv('SHOPIFY_LOCATION_ID')
SHOPIFY_FF_MESSAGE = os.getenv('SHOPIFY_FF_MESSAGE', 'Accepted.')
SHOPIFY_BATCH_SIZE = int(os.getenv('SHOPIFY_BATCH_SIZE', '20'))
SHOPIFY_HEADERS = {
    "Content-Type": "application/json",
    "X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN,
}

# SFTP Configuration
SFTP_HOST = os.getenv('SFTP_HOST')
SFTP_PORT = int(os.getenv('SFTP_PORT', '22'))
SFTP_USER = os.getenv('SFTP_USER')
SFTP_PASSWORD = get_secret('sftp-password', BQ_PROJECT) or os.getenv('SFTP_PASSWORD')
SFTP_BASE_PATH = os.getenv('SFTP_BASE_PATH', '/Live/imports-in')
SFT_ARCHIVE_PATH = os.getenv('SFTP_ARCHIVE_PATH', '/Live/imports-in/Archive')
PROLOG_FNAME_PREFIX = os.getenv('PROLOG_FNAME_PREFIX', 'CSH')
TSV_OUTPUT_PATH = os.getenv('TSV_OUTPUT_PATH', '/tmp/bbx_uk_imp')

# Initialize BigQuery client
def get_bq_client():
    """Get BigQuery client with appropriate credentials."""
    if is_cloud_function():
        # In Cloud Functions, use default credentials
        return bigquery.Client(project=BQ_PROJECT)
    else:
        # For local development, use service account file
        if BQ_CREDPATH and os.path.exists(BQ_CREDPATH):
            creds = Credentials.from_service_account_file(BQ_CREDPATH, scopes=BQ_SCOPES)
            return bigquery.Client(credentials=creds, project=BQ_PROJECT)
        else:
            # Fallback to default credentials
            return bigquery.Client(project=BQ_PROJECT)

BQ_CLIENT = get_bq_client()

def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def upsert_order_status(orders):
    total_orders = len(orders)
    processed_orders_count = 0
    for batch in chunks(orders, BQ_BATCH_SIZE):
        # Deduplicate based on uniq_id and order_id
        unique_orders = {(order['uniq_id'], order['order_id']): order for order in batch}
        unique_orders_count = len(unique_orders)
        logging.info(f"Updating status for {unique_orders_count} unique orders...")

        values_clause = []
        query_parameters = []

        for index, order in enumerate(unique_orders.values()):
            uniq_id = order['uniq_id']
            order_id = order['order_id']
            status = order['new_status']

            values_clause.append(f"""
            SELECT 
                @uniq_id_{index} AS uniq_id, 
                @order_id_{index} AS order_id, 
                @status_{index} AS status,
                CURRENT_TIMESTAMP() AS updated_at
            """)

            query_parameters.extend([
                bigquery.ScalarQueryParameter(f"uniq_id_{index}", "STRING", uniq_id),
                bigquery.ScalarQueryParameter(f"order_id_{index}", "STRING", order_id),
                bigquery.ScalarQueryParameter(f"status_{index}", "STRING", status),
            ])

        merge_query = f"""
        MERGE 
          `{BQ_SHIPMENTS_TABLE}` T
        USING (
          {" UNION ALL ".join(values_clause)}
        ) S ON 
            T.uniq_id = S.uniq_id AND T.`01_Order_Number` = S.order_id
        WHEN MATCHED THEN
          UPDATE SET 
            status = S.status,
            updated_at = S.updated_at
        """

        try:
            job_config = bigquery.QueryJobConfig(query_parameters=query_parameters)
            result = BQ_CLIENT.query(merge_query, job_config=job_config).result()
            processed_orders_count += unique_orders_count
            logging.info(f"Processed {processed_orders_count} out of {total_orders} orders. Batch of {unique_orders_count} shipments upserted with status from new_status in BigQuery")
        except Exception as e:
            logging.error(f"Failed to upsert batch of shipments: {e}")

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def download_and_archive_files():
    try:
        transport = paramiko.Transport((SFTP_HOST, SFTP_PORT))
        transport.connect(username=SFTP_USER, password=SFTP_PASSWORD)
        sftp = paramiko.SFTPClient.from_transport(transport)

        try:
            # List files matching the prefix
            files = sftp.listdir(SFTP_BASE_PATH)
            matching_files = [f for f in files if f.startswith(PROLOG_FNAME_PREFIX)]
            if not matching_files:
                logging.info("No matching SFTP files found")
                return

            logging.info(f"Found matching SFTP files: {matching_files}")
        except Exception as e:
            logging.error(f"Failed to list files in SFTP directory: {e}")
            raise

        for filename in matching_files:
            try:
                remote_file_path = os.path.join(SFTP_BASE_PATH, filename)
                local_file_path = os.path.join(TSV_OUTPUT_PATH, filename)

                # Download the file
                sftp.get(remote_file_path, local_file_path)
                logging.info(f"Downloaded file {remote_file_path} to {local_file_path}")

                # Compare file sizes
                local_file_size = os.path.getsize(local_file_path)
                remote_file_size = sftp.stat(remote_file_path).st_size

                if local_file_size == remote_file_size:
                    # Move the file to the archive path
                    archive_path = os.path.join(SFT_ARCHIVE_PATH, filename)
                    sftp.rename(remote_file_path, archive_path)
                    logging.info(f"Moved file {filename} to {archive_path}")
                else:
                    logging.error(f"File size mismatch for {filename}: local size {local_file_size}, remote size {remote_file_size}")
            except Exception as e:
                logging.error(f"Failed to process file {filename}: {e}")
                raise
    except Exception as e:
        logging.error(f"Failed to process files from SFTP: {e}")
        raise
    finally:
        if 'sftp' in locals():
            sftp.close()
        if 'transport' in locals():
            transport.close()

def load_files_to_bigquery():
    try:
        # Get the current UTC time
        current_utc_time = datetime.now(timezone.utc).isoformat()

        # List all files in the local directory
        files = os.listdir(TSV_OUTPUT_PATH)
        matching_files = [filename for filename in files if filename.startswith(PROLOG_FNAME_PREFIX)]
        if not matching_files:
            logging.info("No matching local files found.")
            return

        logging.info(f"Found matching local files: {matching_files}")
        for filename in matching_files:
            local_file_path = os.path.join(TSV_OUTPUT_PATH, filename)
            done_file_path = os.path.join(TSV_OUTPUT_PATH, f"_DONE_{filename}")

            # Check if the filename already exists in the BigQuery table
            logging.info(f"Checking local file {filename}...")
            check_query = f"""
            SELECT COUNT(*) AS file_count
            FROM `{BQ_SHIPMENTS_TABLE}`
            WHERE filename = @filename
            """
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("filename", "STRING", filename)
                ]
            )

            try:
                check_job = BQ_CLIENT.query(check_query, job_config=job_config)
                file_count = check_job.result().to_dataframe()["file_count"][0]
            except Exception as e:
                logging.error(f"Failed to check if file {filename} exists in BigQuery: {e}")
                raise

            if file_count > 0:
                logging.info(f"File {filename} already exists in BigQuery table {BQ_SHIPMENTS_TABLE}. Skipping.")
                continue

            try:
                # Generate a unique identifier for the temporary table
                uniq_id = str(uuid.uuid4())

                # Read the TSV file into a list of dictionaries
                with open(local_file_path, 'r') as file:
                    reader = csv.DictReader(
                        file,
                        delimiter='\t',
                        fieldnames=[
                            "01_Order_Number",
                            "02_UPC",
                            "03_Shipped_Qty",
                            "04_Item_Description",
                            "05_Item_Unit_Price",
                            "06_Line_From_Order",
                            "07_Ship_Date",
                            "08_Order_Status",
                            "09_Order_Subtotal",
                            "10_Order_Shipping_and_Handling",
                            "11_Order_Tax_Amount",
                            "12_Total_Amount_of_Order",
                            "13_Customer_Number",
                            "14_Tracking_Number",
                            "15_Serial_or_Lot_Number",
                            "16_Shipto_Email_address",
                            "17_Ship_Method",
                            "18_Client_Line_Number",
                            "19_3PL_Reference_Number",
                            "20_Ship_to_ID",
                            "21_Source",
                            "22_Gift_card_code"
                        ]
                    )
                    rows = list(reader)

                # Normalize data
                for row in rows:
                    # Convert blank fields to None or a default value
                    for key, value in row.items():
                        if value == '':
                            row[key] = None

                    # Convert numeric fields to appropriate types
                    int_fields = [
                        "03_Shipped_Qty",
                        "06_Line_From_Order",
                        "10_Order_Shipping_and_Handling",
                        "18_Client_Line_Number",
                        "21_Source"
                    ]
                    for field in int_fields:
                        if row[field] is not None:
                            try:
                                row[field] = int(row[field])
                            except ValueError:
                                row[field] = None

                    numeric_fields = [
                        "05_Item_Unit_Price",
                        "08_Order_Status",
                        "09_Order_Subtotal",
                        "11_Order_Tax_Amount",
                        "12_Total_Amount_of_Order"
                    ]
                    for field in numeric_fields:
                        if row[field] is not None:
                            try:
                                row[field] = float(row[field])
                            except ValueError:
                                row[field] = None

                    # Convert date fields to the correct format
                    date_fields = ["07_Ship_Date"]
                    for field in date_fields:
                        if row[field] is not None:
                            try:
                                parsed_date = datetime.strptime(row[field], '%Y/%m/%d')
                                # Convert the date to a string in the desired format
                                row[field] = parsed_date.strftime('%Y-%m-%d')
                            except ValueError:
                                row[field] = None
                        else:
                            # Ensure the field is explicitly set to a string if it's None
                            row[field] = ""

                    # Add additional fields
                    row["filename"] = filename
                    row["processed_at"] = current_utc_time
                    row["updated_at"] = current_utc_time
                    row["uniq_id"] = uniq_id
                    row["status"] = "loaded"

                # Use the BigQuery client to load data into a temporary table
                temp_table_name = f"{BQ_SHIPMENTS_TABLE.split('.')[-1]}_{uniq_id[:8].replace('-', '')}"
                logging.info(temp_table_name)
                dataset_ref = BQ_CLIENT.dataset(BQ_DATASET)
                temp_table_ref = dataset_ref.table(temp_table_name)

                logging.info(f"Loading data into temporary table {temp_table_name}")
                # Load data into the temporary BigQuery table
                job = BQ_CLIENT.load_table_from_json(
                    rows,
                    temp_table_ref,
                    job_config=bigquery.LoadJobConfig(
                        schema=[
                            bigquery.SchemaField("uniq_id", "STRING", mode="REQUIRED"),
                            bigquery.SchemaField("filename", "STRING", mode="REQUIRED"),
                            bigquery.SchemaField("status", "STRING", mode="REQUIRED"),
                            bigquery.SchemaField("processed_at", "TIMESTAMP"),
                            bigquery.SchemaField("01_Order_Number", "STRING"),
                            bigquery.SchemaField("02_UPC", "STRING"),
                            bigquery.SchemaField("03_Shipped_Qty", "INT64"),
                            bigquery.SchemaField("04_Item_Description", "STRING"),
                            bigquery.SchemaField("05_Item_Unit_Price", "NUMERIC"),
                            bigquery.SchemaField("06_Line_From_Order", "INT64"),
                            bigquery.SchemaField("07_Ship_Date", "DATE"),
                            bigquery.SchemaField("08_Order_Status", "NUMERIC"),
                            bigquery.SchemaField("09_Order_Subtotal", "NUMERIC"),
                            bigquery.SchemaField("10_Order_Shipping_and_Handling", "INT64"),
                            bigquery.SchemaField("11_Order_Tax_Amount", "NUMERIC"),
                            bigquery.SchemaField("12_Total_Amount_of_Order", "NUMERIC"),
                            bigquery.SchemaField("13_Customer_Number", "STRING"),
                            bigquery.SchemaField("14_Tracking_Number", "STRING"),
                            bigquery.SchemaField("15_Serial_or_Lot_Number", "STRING"),
                            bigquery.SchemaField("16_Shipto_Email_address", "STRING"),
                            bigquery.SchemaField("17_Ship_Method", "STRING"),
                            bigquery.SchemaField("18_Client_Line_Number", "INT64"),
                            bigquery.SchemaField("19_3PL_Reference_Number", "STRING"),
                            bigquery.SchemaField("20_Ship_to_ID", "STRING"),
                            bigquery.SchemaField("21_Source", "STRING"),
                            bigquery.SchemaField("22_Gift_card_code", "STRING"),
                            bigquery.SchemaField("updated_at", "TIMESTAMP"),
                        ],
                        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                    )
                )

                job.result()  # Wait for the job to complete

                logging.info(f"Data loaded into temporary table {temp_table_name}")
                # Check the row count in the temporary table
                try:
                    query = f"SELECT COUNT(*) AS row_count FROM `{dataset_ref.project}.{dataset_ref.dataset_id}.{temp_table_name}`"
                    row_count_job = BQ_CLIENT.query(query)
                    row_count = row_count_job.result().to_dataframe()["row_count"][0]

                    logging.info(f"Row count in temporary table {temp_table_name}: {row_count}")

                    # Compare the row count with the original data row count
                    original_row_count = len(rows)
                    if row_count != original_row_count:
                        logging.error(f"Row count mismatch: {row_count} in temporary table vs {original_row_count} in original data")
                        raise ValueError("Row count mismatch between temporary table and original data")
                except Exception as e:
                    logging.error(f"Failed to verify row count in temporary table {temp_table_name}: {e}")
                    logging.info(f"Attempting to delete temporary table {temp_table_name} due to row count verification failure")
                    BQ_CLIENT.delete_table(temp_table_ref)
                    logging.info(f"Deleted temporary table {temp_table_name}")
                    raise

                if row_count > 0:
                    # Insert data from the temporary table into the final table
                    insert_query = f"""
                    INSERT INTO `{BQ_SHIPMENTS_TABLE}`
                    SELECT * FROM `{dataset_ref.project}.{dataset_ref.dataset_id}.{temp_table_name}`
                    """
                    BQ_CLIENT.query(insert_query).result()

                    logging.info(f"Loaded {row_count} rows from {local_file_path} into BigQuery table {BQ_SHIPMENTS_TABLE}")

                    logging.info(f"Attempting to delete temporary table {temp_table_name} after successful insertion")
                    BQ_CLIENT.delete_table(temp_table_ref)
                    logging.info(f"Deleted temporary table {temp_table_name}")
                    logging.info(f"Finished processing file {filename}")
                    # rename the file to start with _DONE_
                    try:
                        logging.info(f"Renaming file {filename} to {done_file_path}...")
                        os.rename(local_file_path, done_file_path)
                    except Exception as e:
                        logging.warning(f"Failed to rename file after successful insertion: {e}")

                else:
                    logging.warning(f"No rows were loaded from {local_file_path} into the temporary table {temp_table_name}")
                    logging.info(f"Attempting to delete temporary table {temp_table_name} due to no rows loaded")
                    BQ_CLIENT.delete_table(temp_table_ref)
                    logging.info(f"Deleted temporary table {temp_table_name}")
                    raise ValueError("No rows were loaded into the temporary table")

            except Exception as e:
                logging.error(f"Failed to process file {filename}: {e}")
                raise

    except Exception as e:
        logging.error(f"Failed to load files into BigQuery: {e}")
        raise

    logging.info("Finished processing all files.")

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def get_orders_from_bigquery():
    logging.info("Fetching shipments from BigQuery.")
    try:
        query = f"""
        SELECT 
            uniq_id,
            order_id,
            carrier_name,
            tracking_number,
            -- tracking_url,
            fulfillment_order_id AS fulfillmentOrderId,
            assigned_location_id AS assignedLocationId,
            status_ff,
            status_sh,
            line_items
        FROM 
            `{BQ_SHIPMENTS_VIEW}`
        WHERE 
            status_sh = 'loaded'
            AND status_ff IS NOT NULL
        LIMIT 4000
        """
        logging.debug(f"Executing query: {query}")
        query_job = BQ_CLIENT.query(query)
        orders = [dict(row) for row in query_job.result()]

        logging.info(f"Fetched {len(orders)} orders with status 'loaded'.")
        for order in orders:
            logging.debug(f"Order details: {order}")

        logging.info("Finished fetching orders from BigQuery.")
        return orders
    except Exception as e:
        logging.error(f"Failed to fetch orders from BigQuery: {e}")
        raise

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def create_and_upload_fulfillments(orders):
    successful_orders = []
    try:
        if not orders:
            logging.info("No new fulfillments to process.")
            return successful_orders

        total_orders = len(orders)

        for chunk_index, batch in enumerate(chunks(orders, SHOPIFY_BATCH_SIZE), start=1):
            current_processed = min(chunk_index * SHOPIFY_BATCH_SIZE, total_orders)
            percent_processed = (current_processed / total_orders) * 100
            logging.info(
                f"Processing chunk {chunk_index}, {current_processed} out of {total_orders} total orders ({percent_processed:.2f}%)."
            )

            # Prepare mutations
            mutations = []
            variables = {}
            for index, order in enumerate(batch):
                fulfillment_order_id = order['fulfillmentOrderId']
                global_fulfillment_order_id = f"gid://shopify/FulfillmentOrder/{fulfillment_order_id}"

                tracking_number = order.get('tracking_number', '')
                line_items = order['line_items']

                # Line items for the current order
                fulfillment_order_line_items = [
                    {
                        "id": f"gid://shopify/FulfillmentOrderLineItem/{line_item['fulfillment_line_id']}",
                        "quantity": line_item['shipped_qty']
                    }
                    for line_item in line_items
                ]

                # Create the fulfillment payload for the entire order
                courier = order['carrier_name'] if 'carrier_name' in order else 'Other'
                fulfillment = {
                    "trackingInfo": {
                        "number": tracking_number,
                        "company": courier
                    },
                    "lineItemsByFulfillmentOrder": [
                        {
                            "fulfillmentOrderId": global_fulfillment_order_id,
                            "fulfillmentOrderLineItems": fulfillment_order_line_items
                        }
                    ],
                    "notifyCustomer": True
                }

                # Add the mutation for the current order
                mutations.append(f"""
                    fulfillmentCreate{index}: fulfillmentCreateV2(fulfillment: $fulfillment_{index}) {{
                      fulfillment {{
                        id
                        status
                        order {{
                          id
                        }}
                      }}
                      userErrors {{
                        field
                        message
                      }}
                    }}
                """)
                variables[f"fulfillment_{index}"] = fulfillment

            # Combine all mutations into one request
            graphql_query = f"mutation ({', '.join([f'$fulfillment_{i}: FulfillmentV2Input!' for i in range(len(batch))])}) {{ {' '.join(mutations)} }}"

            logging.info(f"Executing GraphQL query for batch {chunk_index}.")
            data = execute_graphql_query(graphql_query, variables, SHOPIFY_API_URL)

            # Process the results
            if data:
                for index, order in enumerate(batch):
                    fulfillment_key = f"fulfillmentCreate{index}"
                    if fulfillment_key in data:
                        fulfillment_result = data[fulfillment_key]
                        if 'userErrors' in fulfillment_result and fulfillment_result['userErrors']:
                            order['new_status'] = 'error'
                            logging.error(
                                f"Error fulfilling order {order['fulfillmentOrderId']}: {fulfillment_result['userErrors']}"
                            )
                        else:
                            order['new_status'] = 'fulfilled'
                            successful_orders.append(order)
                            logging.info(f"Successfully fulfilled order {order['order_id']} for fullfillment order {order['fulfillmentOrderId']}.")
                    else:
                        order['new_status'] = 'error'
                        logging.error(f"GraphQL request failed for fulfillment order {order['fulfillmentOrderId']}.")

            # Udate status
            upsert_order_status(batch)

    except Exception as e:
        logging.error(f"Failed to upload fulfillments to Shopify: {e}")
        for order in orders:
            order['new_status'] = 'error'
        raise

    return successful_orders

@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=2, max=10))
def execute_graphql_query(query, variables, url):
    try:
        response = requests.post(url, json={"query": query, "variables": variables}, headers=SHOPIFY_HEADERS)
        response.raise_for_status()  # Retry on HTTP errors
        data = response.json()

        if "errors" in data:
            logging.error(f"GraphQL errors: {data['errors']}")
            return None
        if not data.get("data"):
            logging.error("GraphQL query returned no data.")
            return None

        return data["data"]

    except requests.exceptions.RequestException as e:
        logging.error(f"Network error: {e}")
        raise  # Retry on network issues
    except json.JSONDecodeError as e:
        logging.error(f"Invalid JSON: {e}")
        raise  # Retry on JSON errors
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        raise  # Retry on other exceptions

# ------------------------------------------------------------------------------
# Cloud Function Entry Point
# ------------------------------------------------------------------------------
@functions_framework.http
def process_shipments(request):
    """
    Google Cloud Function entry point for processing shipments.
    
    Args:
        request: Flask request object
        
    Returns:
        Flask response object
    """
    try:
        logging.info("Starting BBX shipments processing...")

        logging.info("Downloading and archiving files...")
        download_and_archive_files()
        
        logging.info("Loading files to BigQuery...")
        load_files_to_bigquery()

        logging.info("Getting orders from BigQuery...")
        orders = get_orders_from_bigquery()
        
        logging.info("Creating and uploading fulfillments...")
        create_and_upload_fulfillments(orders)

        logging.info("Process completed successfully.")
        return {
            'status': 'success',
            'message': 'Process completed successfully.',
            'processed_orders': len(orders) if orders else 0
        }, 200

    except Exception as e:
        logging.error(f"Error in BBX shipments processing: {str(e)}")
        return {
            'status': 'error',
            'message': f'Process failed: {str(e)}'
        }, 500

# ------------------------------------------------------------------------------
# Local Development Entry Point
# ------------------------------------------------------------------------------
def main(request):
    """Local development entry point."""
    return process_shipments(request)

if __name__ == "__main__":
    main(True)
