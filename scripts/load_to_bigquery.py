import os
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from dotenv import load_dotenv
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# --- Configuration ---
PROJECT_ID = "nga-open"
DATASET_ID = "nga_open_data"
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data")

# Initialize BigQuery client with credentials
credentials = service_account.Credentials.from_service_account_file(
    os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
)
client = bigquery.Client(project=PROJECT_ID, credentials=credentials)

# --- Table Configurations with Descriptions and Column Mappings ---
TABLE_CONFIGS = {
    # Core tables (already loaded but will be enhanced with descriptions)
    "objects": {
        "file": "objects.csv",
        "description": "Core table containing information about each art object in the NGA collection, including titles, dates, materials, dimensions, and classification details.",
        "column_renames": {},
        "dependencies": []
    },
    "constituents": {
        "file": "constituents.csv", 
        "description": "Information about people and organizations associated with art objects, including artists, donors, and owners, with biographical data and identifiers.",
        "column_renames": {},
        "dependencies": []
    },
    "objects_constituents": {
        "file": "objects_constituents.csv",
        "description": "Junction table linking objects to constituents, defining relationships such as artist, donor, or owner roles with associated dates and locations.",
        "column_renames": {},
        "dependencies": ["objects", "constituents"]
    },
    "objects_terms": {
        "file": "objects_terms.csv",
        "description": "Keywords, themes, styles, techniques, and other descriptive terms associated with art objects for categorization and search.",
        "column_renames": {},
        "dependencies": ["objects"]
    },
    "published_images": {
        "file": "published_images.csv",
        "description": "IIIF-compatible images of art objects published on NGA websites, including primary and alternate views with technical metadata.",
        "column_renames": {"depictstmsobjectid": "objectid"},
        "dependencies": ["objects"]
    },
    
    # New tables to be loaded
    "alternative_identifiers": {
        "file": "alternative_identifiers.csv",
        "description": "Alternative identifiers for NGA objects and constituents, including Wikidata IDs and other external system identifiers.",
        "column_renames": {},
        "dependencies": []
    },
    "constituents_altnames": {
        "file": "constituents_altnames.csv",
        "description": "Alternative names used by or assigned to constituents, including birth names, pseudonyms, and variant spellings.",
        "column_renames": {},
        "dependencies": ["constituents"]
    },
    "constituents_text_entries": {
        "file": "constituents_text_entries.csv",
        "description": "Long text entries associated with constituents, primarily bibliographic information and biographical details.",
        "column_renames": {},
        "dependencies": ["constituents"]
    },
    "locations": {
        "file": "locations.csv",
        "description": "Physical locations within the National Gallery of Art's Washington, D.C. campus where art objects are displayed or stored.",
        "column_renames": {},
        "dependencies": []
    },
    "media_items": {
        "file": "media_items.csv",
        "description": "Audio and video content published on www.nga.gov, including lectures, tours, and educational materials with metadata.",
        "column_renames": {},
        "dependencies": []
    },
    "media_relationships": {
        "file": "media_relationships.csv",
        "description": "Relationships between media items and NGA art objects or constituents, linking multimedia content to collection records.",
        "column_renames": {},
        "dependencies": ["media_items"]
    },
    "object_associations": {
        "file": "object_associations.csv",
        "description": "Parent-child relationships between art objects, defining separable and inseparable connections such as triptych panels or portfolio contents.",
        "column_renames": {},
        "dependencies": ["objects"]
    },
    "objects_dimensions": {
        "file": "objects_dimensions.csv",
        "description": "Detailed dimensional measurements for art objects, including height, width, depth, and weight with specific measurement contexts.",
        "column_renames": {},
        "dependencies": ["objects"]
    },
    "objects_historical_data": {
        "file": "objects_historical_data.csv",
        "description": "Historical audit trail for important object properties, including previous attributions and title changes with dates.",
        "column_renames": {},
        "dependencies": ["objects"]
    },
    "objects_text_entries": {
        "file": "objects_text_entries.csv",
        "description": "Long text entries associated with objects, including provenance, exhibition history, conservation notes, and bibliographic information.",
        "column_renames": {},
        "dependencies": ["objects"]
    },
    "preferred_locations": {
        "file": "preferred_locations.csv",
        "description": "Public-facing location descriptions optimized for website display, including gallery names and hierarchical location relationships.",
        "column_renames": {},
        "dependencies": []
    },
    "preferred_locations_tms_locations": {
        "file": "preferred_locations_tms_locations.csv",
        "description": "Mapping between internal collection management system location IDs and public-facing preferred location descriptions.",
        "column_renames": {},
        "dependencies": ["preferred_locations", "locations"]
    }
}

# --- Column Descriptions Based on Data Dictionary ---
COLUMN_DESCRIPTIONS = {
    # Objects table columns
    "objectid": "Primary identifier for an art object created by the collection management system",
    "uuid": "Persistent unique identifier for linking with external data sources",
    "accessioned": "Flag indicating whether this is an NGA accessioned work (always 1 in public dataset)",
    "accessionnum": "Official accession number assigned to the art object (format: YYYY.lot.sequence)",
    "objectleonardoid": "Legacy collection management system identifier",
    "locationid": "Current physical location identifier for the primary object component",
    "title": "Title of the art object",
    "displaydate": "Human-readable date corresponding to object creation timeframe",
    "beginyear": "Computer-readable start year for object creation",
    "endyear": "Computer-readable end year for object creation",
    "visualbrowsertimespan": "Computer-generated timeframe slot based on begin and end years",
    "medium": "Materials and techniques used in creating the art object",
    "dimensions": "Human-readable dimensions of the art object",
    "inscription": "Description of text or marks by the artist on the object",
    "markings": "Description of other visual marks on the object and their locations",
    "attributioninverted": "Artist attribution in inverted format (Last, First)",
    "attribution": "Artist attribution in standard format (First Last)",
    "creditline": "Acknowledgment of donors or funders who enabled the acquisition",
    "classification": "Primary type/category of the art object",
    "subclassification": "Secondary classification providing more specific categorization",
    "visualbrowserclassification": "Normalized classification for web display",
    "provenancetext": "Detailed ownership history of the art object",
    "parentid": "Parent object identifier for composite works",
    "isvirtual": "Flag indicating whether this is a conceptual rather than physical object",
    "departmentabbr": "Abbreviation for the NGA curatorial department responsible for the object",
    "portfolio": "Portfolio name for objects that are part of a portfolio",
    "series": "Series name for objects that are part of a series",
    "volume": "Volume name for objects that are part of a volume",
    "watermarks": "Description of any watermarks present on the object",
    "lastdetectedmodification": "Timestamp of last modification in the source system",
    "wikidataid": "Wikidata identifier for cross-referencing with external databases",
    "customprinturl": "URL for ordering custom prints if available",
    
    # Constituents table columns
    "constituentid": "Primary identifier for a person or organization",
    "ulanid": "Getty Union List of Artist Names (ULAN) identifier",
    "preferreddisplayname": "NGA's preferred name for the constituent (inverted format)",
    "forwarddisplayname": "Preferred name in forward format (First Last)",
    "lastname": "Preferred last name of the constituent",
    "displaydate": "Birth and death dates in human-readable format",
    "artistofngaobject": "Flag indicating whether constituent is an artist of NGA objects",
    "beginyear": "Birth year of the constituent",
    "endyear": "Death year of the constituent",
    "visualbrowsertimespan": "Computed time-span slot for the constituent's life",
    "nationality": "Nationality of the constituent",
    "visualbrowsernationality": "Normalized nationality (top 25 or 'other')",
    "constituenttype": "Type of constituent (individual, corporate, anonymous, etc.)",
    
    # Common columns across multiple tables
    "term": "Descriptive term, keyword, or classification",
    "termtype": "Category of term (Keyword, Style, Technique, Theme, etc.)",
    "termid": "Identifier for the term in the collection management system",
    "roletype": "General category of relationship (artist, donor, owner)",
    "role": "Specific role within the role type (painter, sculptor, etc.)",
    "displayorder": "Order for displaying multiple related items",
    "objectid": "Foreign key reference to the objects table",
    "constituentid": "Foreign key reference to the constituents table",
    
    # Published images columns
    "iiifurl": "Base IIIF URL for accessing the image via IIIF Image API",
    "iiifthumburl": "IIIF URL for generating thumbnail images",
    "viewtype": "Image type: 'primary' for main view, 'alternate' for additional views",
    "sequence": "Sort order for multiple images of the same object",
    "width": "Full width of the source image at highest zoom level",
    "height": "Full height of the source image at highest zoom level",
    "maxpixels": "Maximum pixel limit enforced for fair use compliance",
    "created": "Creation date of the source image file",
    "modified": "Last modification date of the image metadata",
    "assistivetext": "Descriptive text for screen readers and accessibility",
    
    # Location columns
    "locationid": "Primary identifier for a physical location",
    "site": "Building location (East Building, West Building, Sculpture Garden)",
    "room": "Room identifier within the building",
    "publicaccess": "Flag indicating whether location has public access",
    "description": "Full text description of the location",
    "unitposition": "Specific position within the room (e.g., South Wall)",
    
    # Media columns
    "mediaid": "Unique identifier for media item from external provider",
    "mediatype": "Type of media content (audio or video)",
    "duration": "Duration of media item in seconds",
    "language": "Two-letter language code for media content",
    "thumbnailurl": "URL for media thumbnail image",
    "playurl": "URL for direct media playback",
    "downloadurl": "URL for media download (if available)",
    "keywords": "Non-controlled keywords assigned to media item",
    "tags": "Content management system tags for media item",
    "imageurl": "URL for full-size associated image",
    "presentationdate": "Date of original presentation or event",
    "releasedate": "Publication date of media item",
    "lastmodified": "Last modification date of media metadata"
}

def create_dataset_if_not_exists():
    """Create the BigQuery dataset if it doesn't exist."""
    try:
        client.get_dataset(DATASET_ID)
        logger.info(f"Dataset {DATASET_ID} already exists.")
    except Exception:
        dataset = bigquery.Dataset(f"{PROJECT_ID}.{DATASET_ID}")
        dataset.description = "National Gallery of Art Open Data Collection - Complete dataset including all CSV files from the NGA's public collection data program"
        dataset.location = "US"
        client.create_dataset(dataset, exists_ok=True)
        logger.info(f"Successfully created dataset {DATASET_ID}.")

def sanitize_column_names(df):
    """Sanitize column names for BigQuery compatibility."""
    new_columns = {}
    for col in df.columns:
        # Convert to lowercase and replace special characters with underscores
        sanitized = col.lower().replace(' ', '_').replace('-', '_').replace('(', '').replace(')', '')
        # Remove any other special characters except underscores
        sanitized = ''.join(c if c.isalnum() or c == '_' else '' for c in sanitized)
        # Ensure it doesn't start with a number
        if sanitized and sanitized[0].isdigit():
            sanitized = 'col_' + sanitized
        new_columns[col] = sanitized
    
    return df.rename(columns=new_columns), new_columns

def add_table_description(table_ref, description):
    """Add description to a BigQuery table."""
    table = client.get_table(table_ref)
    table.description = description
    client.update_table(table, ["description"])
    logger.info(f"Updated table description for {table_ref.table_id}")

def add_column_descriptions(table_ref, column_mapping):
    """Add descriptions to table columns."""
    table = client.get_table(table_ref)
    
    # Create new schema with descriptions
    new_schema = []
    for field in table.schema:
        # Look for description in our mapping (case-insensitive)
        description = None
        for key, desc in COLUMN_DESCRIPTIONS.items():
            if key.lower() == field.name.lower():
                description = desc
                break
        
        # Create new field with description
        new_field = bigquery.SchemaField(
            field.name,
            field.field_type,
            mode=field.mode,
            description=description,
            fields=field.fields
        )
        new_schema.append(new_field)
    
    # Update table schema
    table.schema = new_schema
    client.update_table(table, ["schema"])
    logger.info(f"Updated column descriptions for {table_ref.table_id}")

def load_csv_to_bigquery(table_id, config):
    """Load a CSV file into BigQuery with error handling and metadata."""
    logger.info(f"--- Processing {config['file']} -> {table_id} ---")
    
    # Construct full file path
    csv_path = os.path.join(DATA_DIR, config['file'])
    
    if not os.path.exists(csv_path):
        logger.error(f"File not found: {csv_path}")
        return False
    
    # Read CSV into pandas DataFrame
    try:
        df = pd.read_csv(csv_path, low_memory=False)
        logger.info(f"Successfully read {config['file']} - {len(df)} rows, {len(df.columns)} columns")
    except Exception as e:
        logger.error(f"Error reading {csv_path}: {e}")
        return False
    
    # Sanitize column names for BigQuery
    df, column_mapping = sanitize_column_names(df)
    
    # Apply any custom column renames
    if config.get('column_renames'):
        df = df.rename(columns=config['column_renames'])
        logger.info(f"Applied custom column renames: {config['column_renames']}")
    
    # Configure the load job
    table_ref = client.dataset(DATASET_ID).table(table_id)
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",  # Overwrite the table if it exists
        autodetect=True,                     # Automatically infer schema
    )
    
    # Load data to BigQuery
    try:
        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()  # Wait for the job to complete
        logger.info(f"Successfully loaded {len(df)} rows into {DATASET_ID}.{table_id}")
        
        # Add table description
        add_table_description(table_ref, config['description'])
        
        # Add column descriptions
        add_column_descriptions(table_ref, column_mapping)
        
        return True
        
    except Exception as e:
        logger.error(f"Error loading data into BigQuery for table {table_id}: {e}")
        return False

def get_dependency_order():
    """Return tables in dependency order for loading."""
    # Tables with no dependencies first
    no_deps = [name for name, config in TABLE_CONFIGS.items() if not config['dependencies']]
    
    # Then tables with dependencies
    with_deps = [name for name, config in TABLE_CONFIGS.items() if config['dependencies']]
    
    # Simple dependency resolution - load in order of dependency count
    with_deps.sort(key=lambda x: len(TABLE_CONFIGS[x]['dependencies']))
    
    return no_deps + with_deps

def filter_published_images(df):
    """Special filtering for published_images to ensure referential integrity."""
    logger.info("--- Applying special filtering for published_images ---")
    
    # Get valid object IDs
    try:
        sql = f"SELECT objectid FROM `{PROJECT_ID}.{DATASET_ID}.objects`"
        valid_object_ids_df = client.query(sql).to_dataframe()
        valid_object_ids = set(valid_object_ids_df['objectid'])
        logger.info(f"Found {len(valid_object_ids)} valid object IDs for filtering")
        
        if not valid_object_ids:
            logger.error("No valid object IDs found. Cannot filter published_images.")
            return df
            
    except Exception as e:
        logger.error(f"Error fetching object IDs for filtering: {e}")
        return df
    
    # Filter the DataFrame
    original_count = len(df)
    filtered_df = df[df['objectid'].isin(valid_object_ids)]
    filtered_count = len(filtered_df)
    removed_count = original_count - filtered_count
    
    logger.info(f"Filtered published_images: {original_count} -> {filtered_count} rows ({removed_count} removed)")
    return filtered_df

def main():
    """Main execution function."""
    logger.info("Starting NGA Open Data load process to BigQuery...")
    
    # Create dataset
    create_dataset_if_not_exists()
    
    # Get loading order
    loading_order = get_dependency_order()
    logger.info(f"Loading {len(loading_order)} tables in dependency order: {loading_order}")
    
    # Track success/failure
    successful_loads = []
    failed_loads = []
    
    # Load each table
    for table_id in loading_order:
        config = TABLE_CONFIGS[table_id]
        
        # Special handling for published_images
        if table_id == "published_images" and "objects" in successful_loads:
            # Load the CSV first
            csv_path = os.path.join(DATA_DIR, config['file'])
            try:
                df = pd.read_csv(csv_path, low_memory=False)
                df, column_mapping = sanitize_column_names(df)
                
                # Apply column renames
                if config.get('column_renames'):
                    df = df.rename(columns=config['column_renames'])
                
                # Filter for referential integrity
                df = filter_published_images(df)
                
                # Load to BigQuery
                table_ref = client.dataset(DATASET_ID).table(table_id)
                job_config = bigquery.LoadJobConfig(
                    write_disposition="WRITE_TRUNCATE",
                    autodetect=True,
                )
                
                job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
                job.result()
                
                logger.info(f"Successfully loaded {len(df)} rows into {DATASET_ID}.{table_id}")
                
                # Add descriptions
                add_table_description(table_ref, config['description'])
                add_column_descriptions(table_ref, column_mapping)
                
                successful_loads.append(table_id)
                
            except Exception as e:
                logger.error(f"Error processing {table_id}: {e}")
                failed_loads.append(table_id)
        else:
            # Standard loading process
            if load_csv_to_bigquery(table_id, config):
                successful_loads.append(table_id)
            else:
                failed_loads.append(table_id)
    
    # Summary
    logger.info("\n" + "="*60)
    logger.info("LOADING SUMMARY")
    logger.info("="*60)
    logger.info(f"Successfully loaded: {len(successful_loads)} tables")
    for table in successful_loads:
        logger.info(f"  ✓ {table}")
    
    if failed_loads:
        logger.info(f"\nFailed to load: {len(failed_loads)} tables")
        for table in failed_loads:
            logger.info(f"  ✗ {table}")
    
    logger.info("\n--- Data loading process complete ---")
    
    # Provide next steps
    logger.info("\nNext steps:")
    logger.info("1. Verify table descriptions in BigQuery console")
    logger.info("2. Test queries against new tables")
    logger.info("3. Update application code to use additional tables")
    logger.info("4. Consider creating views for commonly used table joins")

if __name__ == "__main__":
    main()