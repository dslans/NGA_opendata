import os
import pandas as pd
from google.cloud import bigquery

# --- Configuration ---
PROJECT_ID = "nga-open"  # Replace with your Google Cloud project ID
DATASET_ID = "nga_open_data"
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data")

# --- BigQuery Client ---
client = bigquery.Client(project=PROJECT_ID)

def load_csv_to_bigquery(file_name, table_id, rename_columns=None):
    """
    Reads a CSV file, optionally renames columns, and loads it into a BigQuery table.

    Args:
        file_name (str): The name of the CSV file in the data directory.
        table_id (str): The ID of the table to create in BigQuery.
        rename_columns (dict, optional): A dictionary to rename columns. 
                                        Defaults to None.
    """
    print(f"--- Processing {file_name} ---")
    
    # Construct full file path
    csv_path = os.path.join(DATA_DIR, file_name)
    
    # Read CSV into a pandas DataFrame
    try:
        df = pd.read_csv(csv_path, low_memory=False)
        print(f"Successfully read {file_name} into DataFrame.")
    except Exception as e:
        print(f"Error reading {csv_path}: {e}")
        return

    # Rename columns if a mapping is provided
    if rename_columns:
        df = df.rename(columns=rename_columns)
        print(f"Renamed columns: {rename_columns}")

    # Configure the load job
    table_ref = client.dataset(DATASET_ID).table(table_id)
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",  # Overwrite the table if it exists
        autodetect=True,                     # Automatically infer schema
    )

    # Start the load job
    try:
        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()  # Wait for the job to complete
        print(f"Successfully loaded {len(df)} rows into {DATASET_ID}.{table_id}.")
    except Exception as e:
        print(f"Error loading data into BigQuery for table {table_id}: {e}")
    
    print("\n")


if __name__ == "__main__":
    print("Starting NGA Open Data load process to BigQuery...")

    # Create the dataset if it doesn't exist
    try:
        client.get_dataset(DATASET_ID)
        print(f"Dataset {DATASET_ID} already exists.")
    except Exception:
        client.create_dataset(DATASET_ID)
        print(f"Successfully created dataset {DATASET_ID}.")

    # --- Load tables without dependencies first ---
    load_csv_to_bigquery("objects.csv", "objects")
    load_csv_to_bigquery("constituents.csv", "constituents")
    
    # --- Load tables with dependencies on 'objects' or 'constituents' ---
    load_csv_to_bigquery("objects_constituents.csv", "objects_constituents")
    load_csv_to_bigquery("objects_terms.csv", "objects_terms")

    # --- Get valid object IDs from the 'objects' table for filtering ---
    print("\n--- Fetching valid object IDs from objects table ---")
    valid_object_ids = set()
    try:
        sql = f"SELECT objectid FROM `{PROJECT_ID}.{DATASET_ID}.objects`"
        valid_object_ids_df = client.query(sql).to_dataframe()
        valid_object_ids = set(valid_object_ids_df['objectid'])
        print(f"Found {len(valid_object_ids)} unique object IDs.")
        if not valid_object_ids:
            print("Error: No valid object IDs found. Aborting.")
            exit()
    except Exception as e:
        print(f"Error fetching object IDs: {e}")
        exit()

    # --- Special handling for published_images.csv to ensure referential integrity ---
    print("\n--- Processing published_images.csv with filtering ---")
    csv_path = os.path.join(DATA_DIR, "published_images.csv")
    try:
        df_images = pd.read_csv(csv_path, low_memory=False)
        print(f"Successfully read {csv_path} into DataFrame.")

        # Rename column to match the 'objects' table
        df_images = df_images.rename(columns={"depictstmsobjectid": "objectid"})
        print(f"Renamed columns: {{'depictstmsobjectid': 'objectid'}}")

        # Filter the DataFrame
        original_row_count = len(df_images)
        df_images_cleaned = df_images[df_images['objectid'].isin(valid_object_ids)]
        cleaned_row_count = len(df_images_cleaned)
        removed_count = original_row_count - cleaned_row_count
        print(f"Filtered published_images: {original_row_count} rows -> {cleaned_row_count} rows ({removed_count} rows removed).")

        # Configure and run the BigQuery load job
        table_ref = client.dataset(DATASET_ID).table("published_images")
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            autodetect=True,
        )
        job = client.load_table_from_dataframe(df_images_cleaned, table_ref, job_config=job_config)
        job.result()
        print(f"Successfully loaded {cleaned_row_count} rows into {DATASET_ID}.published_images.")

    except Exception as e:
        print(f"Error processing published_images.csv: {e}")
    
    print("\n--- Data loading process complete. ---")
