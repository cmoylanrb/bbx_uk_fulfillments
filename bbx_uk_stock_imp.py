import os
import csv
import uuid
import paramiko
import pandas as pd
from google.cloud import bigquery
from google.oauth2.service_account import Credentials
from datetime import datetime, timezone
import simplejson as json
import requests
import logging
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
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

# Test Mode Configuration - Set to False when ready to make actual updates
TEST_MODE = False

# Big Change Detection Thresholds
BIG_CHANGE_ABSOLUTE = 50      # Flag changes of 50+ units
BIG_CHANGE_PERCENTAGE = 100   # Flag changes of 100%+ (doubling/halving)
BIG_COMMITTED_THRESHOLD = 0.5 # Flag if committed > 50% of warehouse stock

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
BQ_STOCK_TABLE = f"{BQ_DATASET}.bbx_wh_stock_uk"
BQ_STOCK_VIEW = f"{BQ_DATASET}.bbx_wh_stockview_uk"

# Shopify Configuration
SHOPIFY_API_VERSION = os.getenv('SHOPIFY_API_VERSION', '2024-10')
SHOPIFY_ACCESS_TOKEN = get_secret('shopify-access-token', BQ_PROJECT) or os.getenv('SHOPIFY_ACCESS_TOKEN')
SHOPIFY_STORE_URL = os.getenv('SHOPIFY_STORE_URL')
SHOPIFY_API_URL = f"{SHOPIFY_STORE_URL}/admin/api/{SHOPIFY_API_VERSION}/graphql.json"
SHOPIFY_LOCATION_ID = os.getenv('SHOPIFY_LOCATION_ID')
SHOPIFY_FETCH_BATCH_SIZE = int(os.getenv('SHOPIFY_FETCH_BATCH_SIZE', '250'))
SHOPIFY_UPDATE_BATCH_SIZE = int(os.getenv('SHOPIFY_UPDATE_BATCH_SIZE', '40'))
SHOPIFY_HEADERS = {
    "Content-Type": "application/json",
    "X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN,
}

# SFTP Configuration
SFTP_HOST = os.getenv('SFTP_HOST')
SFTP_PORT = int(os.getenv('SFTP_PORT', '22'))
SFTP_USER = get_secret('sftp-user', BQ_PROJECT) or os.getenv('SFTP_USER')
SFTP_PASSWORD = get_secret('sftp-password', BQ_PROJECT) or os.getenv('SFTP_PASSWORD')
SFTP_DOWNLOAD_PATH = os.getenv('SFTP_DOWNLOAD_PATH')
SFTP_DOWNLOAD_ARCHIVE_PATH = os.getenv('SFTP_DOWNLOAD_ARCHIVE_PATH')
STOCK_FNAME_PREFIX = os.getenv('STOCK_FNAME_PREFIX')
TSV_OUTPUT_PATH = os.getenv('TSV_OUTPUT_PATH')

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

def load_files_to_bigquery():
    try:
        # Get the current UTC time
        current_utc_time = datetime.now(timezone.utc).isoformat()

        # List all files in the local directory
        files = os.listdir(TSV_OUTPUT_PATH)
        matching_files = [filename for filename in files if filename.startswith(STOCK_FNAME_PREFIX)]
        if not matching_files:
            logging.info("No matching local files found.")
            return

        logging.info(f"Found matching local files: {matching_files}")
        for filename in matching_files:
            local_file_path = os.path.join(TSV_OUTPUT_PATH, filename)
            done_file_path = os.path.join(TSV_OUTPUT_PATH, f"_DONE_{filename}")
            uniq_id = str(uuid.uuid4())
            temp_table_name = f"{BQ_STOCK_TABLE.split('.')[-1]}_{uniq_id[:8].replace('-', '')}"#-

            # Check if the filename already exists in the BigQuery table
            logging.info(f"Checking local file {filename}...")
            check_query = f"""
            SELECT COUNT(*) AS file_count
            FROM `{BQ_STOCK_TABLE}`
            WHERE filename = "{filename}"
            """
            try:
                check_job = BQ_CLIENT.query(check_query)
                file_count = check_job.result().to_dataframe()["file_count"][0]
            except Exception as e:
                logging.error(f"Failed to check if file {filename} exists in BigQuery: {e}")
                raise

            if file_count > 0:
                logging.info(f"File {filename} already exists in BigQuery table {BQ_STOCK_TABLE}. Skipping.")
                continue

            try:
                # Open the CSV file and handle row skipping
                with open(local_file_path, 'r') as file:
                    next(file)  # Skip the first line
                    next(file)  # Skip the second line

                    # Read the headers from the third line
                    raw_headers = next(file).strip().split(',')
                    indexed_headers = [f"{str(i+1).zfill(2)}_{header.replace('"', '').replace(' ', '_').replace('#', '_')}" for i, header in enumerate(raw_headers)]

                    # Start reading the data from the fourth line
                    reader = csv.DictReader(file, delimiter=',', fieldnames=indexed_headers)

                    # Extract rows starting from the 4th line
                    rows = [row for row in reader]

                rows = [
                {
                    **row,
                    "10_Date_Time": (
                        datetime.strptime(row["10_Date_Time"], '%m/%d/%Y %I:%M:%S%p').strftime('%Y-%m-%d %H:%M:%S') if row["10_Date_Time"] else None
                    ),
                    "uniq_id": uniq_id,
                    "status": "loaded",
                    "filename": filename,
                    "updated_at": current_utc_time
                }
                for row in rows
                ]

                dataset_ref = BQ_CLIENT.dataset(BQ_DATASET)
                temp_table_ref = dataset_ref.table(temp_table_name)

                logging.info(f"Loading data into temporary table {temp_table_name}")
                job = BQ_CLIENT.load_table_from_json(
                    rows,
                    temp_table_ref,
                    job_config=bigquery.LoadJobConfig(
                        schema=[
                            bigquery.SchemaField("uniq_id", "STRING", mode="REQUIRED"),
                            bigquery.SchemaField("status", "STRING", mode="REQUIRED"),
                            bigquery.SchemaField("filename", "STRING", mode="REQUIRED"),
                            bigquery.SchemaField("updated_at", "TIMESTAMP"),
                            bigquery.SchemaField("01_Facility", "STRING"),
                            bigquery.SchemaField("02_Item", "STRING"),
                            bigquery.SchemaField("03_Description", "STRING"),
                            bigquery.SchemaField("04_Prod_Group", "STRING"),
                            bigquery.SchemaField("05_Lot__", "STRING"),
                            bigquery.SchemaField("06_Qty", "INTEGER"),
                            bigquery.SchemaField("07_UOM", "STRING"),
                            bigquery.SchemaField("08_Inventory_Status", "STRING"),
                            bigquery.SchemaField("09_Pick_Status", "STRING"),
                            bigquery.SchemaField("10_Date_Time", "TIMESTAMP"),
                        ],
                        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                    )
                )
                job.result()  # Wait for the job to complete

                logging.info(f"Data loaded into temporary table {temp_table_name}")

                # Insert data from the temporary table into the final table
                insert_query = f"""
                INSERT INTO 
                    `{BQ_STOCK_TABLE}`
                SELECT 
                    * 
                FROM 
                    `{dataset_ref.project}.{dataset_ref.dataset_id}.{temp_table_name}`
                """
                BQ_CLIENT.query(insert_query).result()

                logging.info(f"Loaded data from temporary table {temp_table_name} into final table {BQ_STOCK_TABLE}")

                # Clean up: delete the temporary table
                BQ_CLIENT.delete_table(temp_table_ref)
                logging.info(f"Deleted temporary table {temp_table_name}")

                # Rename the file
                try:
                    logging.info(f"Renaming file {filename} to {done_file_path}...")
                    os.rename(local_file_path, done_file_path)
                except Exception as e:
                    logging.warning(f"Failed to rename file after successful insertion: {e}")

            except Exception as e:
                logging.error(f"Failed to process file {filename}: {e}")
                raise

    except Exception as e:
        logging.error(f"Failed to load files into BigQuery: {e}")
        raise

    logging.info("Finished processing all files.")

def upsert_stock_status(items):
    total_items = len(items)
    logging.info(f"Processing {total_items} unique items...")

    # Deduplicate based on uniq_id and sku
    unique_items = {(item['uniq_id'], item['sku']): item for item in items}
    unique_items_count = len(unique_items)

    values_clause = []

    for index, item in enumerate(unique_items.values()):
        uniq_id = item['uniq_id']
        sku = item['sku']
        status = item['new_status']

        values_clause.append(f"""
        SELECT 
            '{uniq_id}' AS uniq_id, 
            '{sku}' AS sku, 
            '{status}' AS status,
            CURRENT_TIMESTAMP() AS updated_at
        """)

    merge_query = f"""
    MERGE 
      `{BQ_STOCK_TABLE}` T
    USING (
      {" UNION ALL ".join(values_clause)}
    ) S ON 
        T.uniq_id = S.uniq_id AND T.`02_Item` = S.sku
    WHEN MATCHED THEN
      UPDATE SET 
        status = S.status,
        updated_at = S.updated_at
    """

    try:
        query_job = BQ_CLIENT.query(merge_query)
        query_job.result()

        stats = query_job._job_statistics()
        num_dml_affected_rows = stats.get('numDmlAffectedRows', 0)
        logging.info(f"Updated status for {unique_items_count} unique items. Query rows affected: {num_dml_affected_rows}")
    except Exception as e:
        logging.error(f"Failed to upsert stock records: {e}")

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def download_and_archive_files():
    try:
        transport = paramiko.Transport((SFTP_HOST, SFTP_PORT))
        transport.connect(username=SFTP_USER, password=SFTP_PASSWORD)
        sftp = paramiko.SFTPClient.from_transport(transport)

        try:
            # List files matching the prefix
            files = sftp.listdir(SFTP_DOWNLOAD_PATH)
            matching_files = [f for f in files if f.startswith(STOCK_FNAME_PREFIX)]
            if not matching_files:
                logging.info("No matching SFTP files found")
                return

            logging.info(f"Found matching SFTP files: {matching_files}")
        except Exception as e:
            logging.error(f"Failed to list files in SFTP directory: {e}")
            raise

        for filename in matching_files:
            try:
                remote_file_path = os.path.join(SFTP_DOWNLOAD_PATH, filename)
                local_file_path = os.path.join(TSV_OUTPUT_PATH, filename)

                # Download the file
                sftp.get(remote_file_path, local_file_path)
                logging.info(f"Downloaded file {remote_file_path} to {local_file_path}")

                # Compare file sizes
                local_file_size = os.path.getsize(local_file_path)
                remote_file_size = sftp.stat(remote_file_path).st_size

                if local_file_size == remote_file_size:
                    # Move the file to the archive path
                    archive_path = os.path.join(SFTP_DOWNLOAD_ARCHIVE_PATH, filename)
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

# @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
# def upload_stock(stock):

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def get_stock_from_shopify():
    logging.info("Fetching stock from Shopify.")
    all_inventory = []
    
    try:
        # GraphQL query to fetch inventory levels with pagination
        query = """
        query getInventoryLevels($first: Int!, $after: String) {
            location(id: "gid://shopify/Location/66575466553") {
                inventoryLevels(first: $first, after: $after) {
                    edges {
                        node {
                            id
                            quantities(names: ["available", "on_hand", "committed"]) {
                                name
                                quantity
                            }
                            item {
                                id
                                sku
                                variant {
                                    id
                                    title
                                    product {
                                        title
                                    }
                                }
                            }
                        }
                        cursor
                    }
                    pageInfo {
                        hasNextPage
                        endCursor
                    }
                }
            }
        }
        """
        
        # Pagination variables
        variables = {"first": SHOPIFY_FETCH_BATCH_SIZE, "after": None}
        has_next_page = True
        batch_num = 1
        
        while has_next_page:
            logging.info(f"[FETCH] Fetching Shopify inventory batch {batch_num}")
            data = execute_graphql_query(query, variables, SHOPIFY_API_URL)
            
            if not data or not data.get("location") or not data["location"].get("inventoryLevels"):
                logging.error("Failed to fetch inventory data from Shopify")
                break
                
            inventory_levels = data["location"]["inventoryLevels"]
            edges = inventory_levels.get("edges", [])
            
            for edge in edges:
                node = edge["node"]
                item = node.get("item", {})
                variant = item.get("variant", {})
                product = variant.get("product", {}) if variant else {}
                
                # Skip items without SKU
                sku = item.get("sku")
                if not sku:
                    continue
                
                # Parse quantities
                quantities = node.get("quantities", [])
                available = 0
                on_hand = 0
                committed = 0
                
                for qty in quantities:
                    if qty.get("name") == "available":
                        available = qty.get("quantity", 0)
                    elif qty.get("name") == "on_hand":
                        on_hand = qty.get("quantity", 0)
                    elif qty.get("name") == "committed":
                        committed = qty.get("quantity", 0)
                
                inventory_record = {
                    "inventory_level_id": node.get("id"),
                    "available": available,
                    "on_hand": on_hand,
                    "committed": committed,
                    "inventory_item_id": item.get("id", "").replace("gid://shopify/InventoryItem/", ""),
                    "sku": sku,
                    "variant_id": variant.get("id", "").replace("gid://shopify/ProductVariant/", "") if variant else None,
                    "variant_title": variant.get("title", "") if variant else "",
                    "product_title": product.get("title", "") if product else "",
                    "description": f"{product.get('title', '')} - {variant.get('title', '')}" if product and variant else ""
                }
                
                all_inventory.append(inventory_record)
            
            # Check for next page
            page_info = inventory_levels.get("pageInfo", {})
            has_next_page = page_info.get("hasNextPage", False)
            variables["after"] = page_info.get("endCursor") if has_next_page else None
            
            logging.info(f"[BATCH] Batch {batch_num}: {len(edges)} items (Total: {len(all_inventory)})")
            batch_num += 1
        
        logging.info(f"Fetched {len(all_inventory)} total inventory items from Shopify.")
        
        if all_inventory:
            sample_item = all_inventory[0]
            logging.info(f"Sample item - SKU: {sample_item['sku']}, Available: {sample_item['available']}, On Hand: {sample_item['on_hand']}")
            logging.debug(f"Full sample item details: {sample_item}")
        
        return all_inventory
        
    except Exception as e:
        logging.error(f"Failed to fetch stock from Shopify: {e}")
        raise

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def get_stock_from_bigquery():
    logging.info("Fetching stock from BigQuery and current Shopify data.")
    try:
        # First, get warehouse data from BigQuery
        query = f"""
        SELECT 
            uniq_id,
            status,
            filename,
            updated_at,
            item,
            description,
            qty,
            inventory_status,
            pick_status,
            inventory_date_time,
            sku,
            variant_id,
            inventory_item_id,
            available as shopify_available_cached
        FROM 
            `{BQ_STOCK_VIEW}`
        WHERE
            status = 'loaded'
        ORDER BY
            inventory_date_time DESC,
            updated_at DESC,
            uniq_id ASC,
            sku ASC;
        """

        logging.debug(f"Executing BigQuery query: {query}")
        query_job = BQ_CLIENT.query(query)
        warehouse_stock = [dict(row) for row in query_job.result()]

        logging.info(f"Fetched {len(warehouse_stock)} warehouse stock rows from BigQuery.")
        
        if not warehouse_stock:
            logging.info("No warehouse stock data found.")
            return []

        # Display warehouse file information
        unique_files = {}
        for item in warehouse_stock:
            filename = item.get('filename', 'Unknown')
            inv_date = item.get('inventory_date_time', 'Unknown')
            if filename not in unique_files:
                unique_files[filename] = {
                    'inventory_date': inv_date,
                    'count': 0
                }
            unique_files[filename]['count'] += 1
        
        logging.info("WAREHOUSE FILE DETAILS:")
        for filename, details in unique_files.items():
            logging.info(f"  File: {filename}")
            logging.info(f"     Date: {details['inventory_date']}")
            logging.info(f"     Items: {details['count']}")
        
        # Create a mapping of SKU to warehouse data
        warehouse_by_sku = {item['sku']: item for item in warehouse_stock if item.get('sku')}
        
        # Get current Shopify inventory data
        logging.info("Fetching current Shopify inventory data...")
        shopify_stock = get_stock_from_shopify()
        
        # Merge warehouse and current Shopify data
        merged_stock = []
        for shopify_item in shopify_stock:
            sku = shopify_item.get('sku')
            if sku in warehouse_by_sku:
                warehouse_item = warehouse_by_sku[sku]
                
                # Merge the data
                merged_item = {
                    # Warehouse data (source of truth for quantities)
                    'uniq_id': warehouse_item.get('uniq_id'),
                    'status': warehouse_item.get('status'),
                    'filename': warehouse_item.get('filename'),
                    'updated_at': warehouse_item.get('updated_at'),
                    'item': warehouse_item.get('item'),
                    'description': warehouse_item.get('description'),
                    'warehouse_qty': warehouse_item.get('qty', 0),  # Warehouse committed amount
                    'inventory_status': warehouse_item.get('inventory_status'),
                    'pick_status': warehouse_item.get('pick_status'),
                    'inventory_date_time': warehouse_item.get('inventory_date_time'),
                    
                    # Shopify identifiers
                    'sku': sku,
                    'variant_id': shopify_item.get('variant_id'),
                    'inventory_item_id': shopify_item.get('inventory_item_id'),
                    
                    # Current Shopify data
                    'shopify_available': shopify_item.get('available', 0),  # Current committed in Shopify
                    'shopify_on_hand': shopify_item.get('on_hand', 0),     # Current on_hand in Shopify
                    'shopify_committed': shopify_item.get('committed', 0),  # Current committed in Shopify
                    
                    # Cached Shopify data from BigQuery (for reference)
                    'shopify_available_cached': warehouse_item.get('shopify_available_cached', 0),
                    
                    # Product details from Shopify
                    'product_title': shopify_item.get('product_title', ''),
                    'variant_title': shopify_item.get('variant_title', ''),
                }
                
                merged_stock.append(merged_item)
            else:
                logging.debug(f"SKU {sku} found in Shopify but not in warehouse data")

        # Log any warehouse items not found in Shopify
        shopify_skus = {item.get('sku') for item in shopify_stock}
        for sku, warehouse_item in warehouse_by_sku.items():
            if sku not in shopify_skus:
                logging.warning(f"Warehouse SKU {sku} not found in current Shopify inventory")

        logging.info(f"Merged {len(merged_stock)} items with both warehouse and Shopify data.")
        
        if merged_stock:
            sample_item = merged_stock[0]
            logging.info(f"Sample merged item - SKU: {sample_item['sku']}, "
                        f"Warehouse Qty: {sample_item['warehouse_qty']}, "
                        f"Shopify Available: {sample_item['shopify_available']}, "
                        f"Shopify On Hand: {sample_item['shopify_on_hand']}")
            logging.debug(f"Full sample item details: {sample_item}")

        logging.info("Finished fetching and merging warehouse and Shopify stock data.")
        return merged_stock
        
    except Exception as e:
        logging.error(f"Failed to fetch and merge stock data: {e}")
        raise

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

        # Log user errors for each mutation
        for mutation, result in data["data"].items():
            if result and "userErrors" in result:
                for error in result["userErrors"]:
                    logging.error(f"Mutation '{mutation}' failed: {error['message']} (Field: {error.get('field')})")
                    # logging.error(query)

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

def log_compare_quantity_error(error_message, mutation_name, query, stock_item):
    try:
        # Extract relevant parts of the error response
        inventory_item_id = stock_item.get('inventory_item_id', 'Unknown')
        location_id = stock_item.get('location_id', 'Unknown')
        quantity = stock_item.get('qty', 'Unknown')
        compare_quantity = stock_item.get('available', 'Unknown')
        variant_id = stock_item.get('variant_id', 'Unknown')
        sku = stock_item.get('sku', 'Unknown')
        description = stock_item.get('description', 'Unknown')

        # Log the parsed details along with the error message
        logging.error(
            f"Mutation '{mutation_name}' failed due to compareQuantity mismatch.\n"
            f"Error Message: {error_message}\n"
            f"Item Details:\n"
            f"  - Variant ID: {variant_id}\n"
            f"  - SKU: {sku}\n"
            f"  - Description: {description}\n"
            f"Stock Details:\n"
            f"  - Inventory Item ID: {inventory_item_id}\n"
            f"  - Location ID: {location_id}\n"
            f"  - Quantity: {quantity}\n"
            f"  - Compare Quantity: {compare_quantity}\n"
        )
        # logging.error(query)

    except Exception as e:
        logging.error(f"Failed to log compareQuantity error: {e}")
        logging.error(f"Original error message: {error_message}")

def set_shopify_inventory_quantities(stock):
    logging.info("Starting inventory sync: Warehouse vs Shopify comparison")
    if TEST_MODE:
        logging.info("TEST MODE: No actual updates will be made")
    
    total_items = len(stock)
    processed_items = 0
    total_updates_needed = 0
    total_no_changes = 0
    available_mismatches = 0
    on_hand_mismatches = 0
    scenario_counts = {}
    big_changes_count = 0
    errors_count = 0

    for chunk in chunks(stock, SHOPIFY_UPDATE_BATCH_SIZE):
        chunk_size = len(chunk)
        chunk_num = processed_items // SHOPIFY_UPDATE_BATCH_SIZE + 1
        total_chunks = (total_items + SHOPIFY_UPDATE_BATCH_SIZE - 1) // SHOPIFY_UPDATE_BATCH_SIZE
        
        logging.info(f"Processing chunk {chunk_num}/{total_chunks} ({chunk_size} items)")

        # Assemble mutations
        mutations = []
        items_to_update = []
        
        for index, stock_item in enumerate(chunk):
            inventory_item_id = stock_item.get('inventory_item_id')
            sku = stock_item.get('sku')
            
            # Warehouse data (source of truth for physical inventory)
            warehouse_on_hand = stock_item.get('warehouse_qty', 0)  # Physical count from warehouse
            warehouse_committed = 0  # Warehouse doesn't track committed separately
            
            # Current Shopify data
            shopify_available = stock_item.get('shopify_available', 0)
            shopify_on_hand = stock_item.get('shopify_on_hand', 0)
            shopify_committed = stock_item.get('shopify_committed', 0)
            
            stock_item['new_status'] = 'failed'

            if inventory_item_id is None:
                logging.warning(f"Missing inventory ID for SKU: {sku}")
                stock_item['new_status'] = 'error_inventory_item_id'
                continue

            # SCENARIO ANALYSIS: Handle all inventory sync situations
            # Key insight: Available = OnHand - Committed - Reserved - etc.
            # If OnHand=0, Available should be ≤0 (negative means oversold)
            
            available_needs_update = warehouse_on_hand != shopify_available
            on_hand_needs_update = warehouse_on_hand != shopify_on_hand
            
            # SCENARIO DETECTION (corrected logic)
            scenario = "unknown"
            if warehouse_on_hand == shopify_on_hand:
                if shopify_available < 0:
                    scenario = "oversold"  # Negative available is normal when committed > on_hand
                elif shopify_available != warehouse_on_hand and shopify_committed > 0:
                    scenario = "legitimate_committed"  # Available reduced by committed orders
                else:
                    scenario = "no_change"  # Everything matches
            elif warehouse_on_hand > shopify_on_hand:
                scenario = "new_stock_arrival"  # Scenario 1: WH got new inventory
            elif warehouse_on_hand < shopify_on_hand:
                if shopify_committed > (shopify_on_hand - warehouse_on_hand):
                    scenario = "stale_committed"  # Scenario 3: WH shipped but Shopify shows old committed
                else:
                    scenario = "pending_orders"   # Scenario 2: Orders in flight to WH
            
            # Track scenario counts
            scenario_counts[scenario] = scenario_counts.get(scenario, 0) + 1
            
            if available_needs_update:
                available_mismatches += 1
            if on_hand_needs_update:
                on_hand_mismatches += 1
            
            # Log items - simplified for no-change cases
            if not on_hand_needs_update and not available_needs_update:
                logging.info(f"SKU {sku}: No update required")
            else:
                logging.info(f"SKU {sku} [{scenario}]:")
                logging.info(f"  WAREHOUSE:  OnHand={warehouse_on_hand}, Committed={warehouse_committed}")
                logging.info(f"  SHOPIFY:    OnHand={shopify_on_hand}, Committed={shopify_committed}, Available={shopify_available}")
            
            # BIG CHANGE DETECTION - Only for actual inventory updates being posted
            big_change_flags = []
            
            if on_hand_needs_update:
                change_amount = abs(warehouse_on_hand - shopify_on_hand)
                old_value = shopify_on_hand
                new_value = warehouse_on_hand
                
                # Flag large absolute changes
                if change_amount >= BIG_CHANGE_ABSOLUTE:
                    big_change_flags.append(f"LARGE CHANGE: {change_amount} units")
                
                # Flag large percentage changes (avoid division by zero)
                if old_value > 0:
                    percentage_change = abs((new_value - old_value) / old_value * 100)
                    if percentage_change >= BIG_CHANGE_PERCENTAGE:
                        big_change_flags.append(f"BIG % CHANGE: {percentage_change:.0f}%")
                
                # Flag zero-to-stock or stock-to-zero changes
                if old_value == 0 and new_value > 0:
                    big_change_flags.append(f"BACK IN STOCK: 0 → {new_value}")
                elif old_value > 0 and new_value == 0:
                    big_change_flags.append(f"OUT OF STOCK: {old_value} → 0")
                
                # Only count as big change if we're actually updating
                if big_change_flags:
                    big_changes_count += 1
            
            # High committed amounts - informational only, not counted as "big changes"
            high_committed_flags = []
            if shopify_committed > 0 and warehouse_on_hand > 0:
                committed_ratio = shopify_committed / warehouse_on_hand
                if committed_ratio >= BIG_COMMITTED_THRESHOLD:
                    high_committed_flags.append(f"HIGH COMMITTED: {shopify_committed} units ({committed_ratio*100:.0f}% of stock)")
            
            # Display flags prominently - only for items being processed
            if on_hand_needs_update or available_needs_update:
                all_flags = big_change_flags + high_committed_flags
                for flag in all_flags:
                    if flag in big_change_flags:
                        logging.warning(f"  [ALERT] {flag}")
                    else:
                        logging.info(f"  [INFO] {flag}")
                
                if on_hand_needs_update:
                    if scenario == "new_stock_arrival":
                        new_stock = warehouse_on_hand - shopify_on_hand
                        logging.info(f"  → NEW STOCK: WH received {new_stock} units")
                    elif scenario == "pending_orders":
                        logging.info(f"  → PENDING ORDERS: {shopify_committed} units in flight to WH")
                    elif scenario == "stale_committed":
                        stale_committed = shopify_on_hand - warehouse_on_hand
                        logging.warning(f"  → STALE COMMITTED: ~{stale_committed} units likely shipped but still showing committed")
                    
                elif available_needs_update and shopify_on_hand == 0 and shopify_available < 0:
                    # Special case: Zero stock with negative available (oversold) - this is normal, don't log as issue
                    logging.info(f"  → OVERSOLD: Available={shopify_available} (normal when committed > on_hand)")
            else:
                # No changes needed
                logging.info(f"  → NO CHANGE: Inventory levels match warehouse data")

            # Determine what needs updating - ONLY update on_hand discrepancies
            if not on_hand_needs_update:
                stock_item['new_status'] = 'no_change_needed'
                total_no_changes += 1
                continue
                
            # STRATEGY: Only sync on_hand when it differs from warehouse
            # Available will automatically adjust based on committed/reserved states
            total_updates_needed += 1
            global_inventory_item_id = f"gid://shopify/InventoryItem/{inventory_item_id}"
            global_location_id = f"gid://shopify/Location/{SHOPIFY_LOCATION_ID}"

            # Predict post-update available (on_hand - committed = available)
            predicted_available = warehouse_on_hand - shopify_committed
            
            if TEST_MODE:
                logging.info(f"  → Would update Shopify OnHand: {shopify_on_hand} → {warehouse_on_hand}")
                logging.info(f"  → Predicted Available: {predicted_available} (assuming committed stays {shopify_committed})")
                stock_item['new_status'] = 'test_mode_would_update'
                items_to_update.append(stock_item)
            else:
                logging.info(f"  → Updating Shopify OnHand: {shopify_on_hand} → {warehouse_on_hand}")
                logging.info(f"  → Expected Available: {predicted_available}")

                mutation = f"""
                inventorySetQuantities{index}: inventorySetQuantities(input: {{
                    reason: "correction",
                    name: "on_hand",
                    ignoreCompareQuantity: true,
                    quantities: [{{
                        inventoryItemId: "{global_inventory_item_id}",
                        locationId: "{global_location_id}",
                        quantity: {warehouse_on_hand},
                        compareQuantity: {shopify_on_hand}
                    }}]
                }}) {{
                    userErrors {{
                        field
                        message
                    }}
                }}"""
                mutations.append(mutation)

        # Skip processing if no updates are required OR if in test mode
        if not mutations or TEST_MODE:
            if TEST_MODE and items_to_update:
                logging.info(f"Chunk {chunk_num}: {len(items_to_update)} items would be updated")
            
            for stock_item in chunk:
                if 'new_status' not in stock_item:
                    stock_item['new_status'] = 'no_updates_needed'
            processed_items += chunk_size
            continue

        # Execute the mutation (only if not in test mode)
        combined_mutation = f"""
        mutation {{
            {" ".join(mutations)}
        }}
        """
        logging.debug(f"GraphQL mutation: {combined_mutation}")

        try:
            data = execute_graphql_query(combined_mutation, {}, SHOPIFY_API_URL)
        except Exception as e:
            data = None
            logging.error(f"GraphQL execution failed: {e}")

        # Handle the response
        if data:
            for index, stock_item in enumerate(chunk):
                mutation_name = f"inventorySetQuantities{index}"
                result = data.get(mutation_name)
                if result and "userErrors" in result:
                    for error in result["userErrors"]:
                        message = error.get("message", "")
                        if "compareQuantity" in message:
                            log_compare_quantity_error(message, mutation_name, combined_mutation, stock_item)
                            stock_item['new_status'] = 'error_compare_quantity'
                        else:
                            logging.error(f"{mutation_name} failed: {message}")
                            stock_item['new_status'] = 'error_graphql_other'
                            errors_count += 1
                else:
                    if 'new_status' not in stock_item or stock_item['new_status'] == 'failed':
                        stock_item['new_status'] = 'success'
        else:
            logging.error(f"Failed to process chunk {chunk_num}")
            for stock_item in chunk:
                stock_item['new_status'] = 'failed'
                errors_count += 1

        # Update the status in BigQuery
        if not TEST_MODE:
            upsert_stock_status(chunk)
            
        processed_items += chunk_size

    # COMPREHENSIVE FINAL SUMMARY
    progress_pct = processed_items/total_items*100
    success_rate = ((total_items - errors_count) / total_items * 100) if total_items > 0 else 0
    
    logging.info("=" * 80)
    logging.info("INVENTORY SYNC SUMMARY")
    logging.info("=" * 80)
    
    # Overall stats
    logging.info(f"PROCESSING COMPLETE: {processed_items:,} items ({progress_pct:.1f}%)")
    logging.info(f"SUCCESS RATE: {success_rate:.1f}% ({total_items - errors_count:,} successful, {errors_count} errors)")
    
    # Action breakdown
    logging.info(f"")
    logging.info(f"ACTIONS TAKEN:")
    logging.info(f"  [UPDATE] Shopify updates: {total_updates_needed}")
    logging.info(f"  [UNCHANGED] No changes needed: {total_no_changes}")
    logging.info(f"  [ERROR] Errors encountered: {errors_count}")
    
    # Scenario breakdown
    logging.info(f"")
    logging.info(f"SCENARIO BREAKDOWN:")
    for scenario, count in sorted(scenario_counts.items()):
        percentage = (count / total_items * 100) if total_items > 0 else 0
        logging.info(f"  {scenario}: {count:,} items ({percentage:.1f}%)")
    
    # Attention items
    logging.info(f"")
    logging.info(f"ATTENTION REQUIRED:")
    logging.info(f"  [ALERT] Big changes flagged: {big_changes_count}")
    logging.info(f"  [INFO] Available mismatches: {available_mismatches} (normal if committed orders exist)")
    logging.info(f"  [INFO] On-hand mismatches: {on_hand_mismatches} (physical inventory differences)")
    
    if TEST_MODE:
        logging.info(f"")
        logging.info("[TEST] TEST MODE COMPLETE - No actual changes made")
    else:
        logging.info(f"")
        logging.info("[SUCCESS] LIVE SYNC COMPLETE - Shopify inventory updated")
    
    logging.info("=" * 80)

# ------------------------------------------------------------------------------
# Cloud Function Entry Point
# ------------------------------------------------------------------------------
@functions_framework.http
def process_stock(request):
    """
    Google Cloud Function entry point for processing stock.
    
    Args:
        request: Flask request object
        
    Returns:
        Flask response object
    """
    try:
        logging.info("Starting BBX stock processing...")

        logging.info("Downloading and archiving files...")
        download_and_archive_files()
        
        logging.info("Loading files to BigQuery...")
        load_files_to_bigquery()

        logging.info("Getting stock from BigQuery...")
        stock = get_stock_from_bigquery()
        
        logging.info("Setting Shopify inventory quantities...")
        set_shopify_inventory_quantities(stock)

        logging.info("Process completed successfully.")
        return {
            'status': 'success',
            'message': 'Process completed successfully.',
            'processed_items': len(stock) if stock else 0
        }, 200

    except Exception as e:
        logging.error(f"Error in BBX stock processing: {str(e)}")
        return {
            'status': 'error',
            'message': f'Process failed: {str(e)}'
        }, 500

# ------------------------------------------------------------------------------
# Local Development Entry Point
# ------------------------------------------------------------------------------
def main(request):
    """Local development entry point."""
    return process_stock(request)

if __name__ == "__main__":
    main(True)