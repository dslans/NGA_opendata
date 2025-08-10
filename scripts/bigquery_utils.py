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

# Initialize BigQuery client with credentials
credentials = service_account.Credentials.from_service_account_file(
    os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
)
client = bigquery.Client(project=PROJECT_ID, credentials=credentials)

def list_tables_with_info():
    """List all tables in the dataset with metadata information."""
    logger.info("="*60)
    logger.info("BIGQUERY DATASET OVERVIEW")
    logger.info("="*60)
    
    try:
        # Get dataset
        dataset = client.get_dataset(DATASET_ID)
        logger.info(f"Dataset: {DATASET_ID}")
        logger.info(f"Description: {dataset.description}")
        logger.info(f"Location: {dataset.location}")
        logger.info(f"Created: {dataset.created}")
        logger.info("")
        
        # List tables
        tables = list(client.list_tables(dataset))
        logger.info(f"Total tables: {len(tables)}")
        logger.info("")
        
        # Get detailed info for each table
        for table in tables:
            table_ref = client.get_table(table.reference)
            logger.info(f"Table: {table_ref.table_id}")
            logger.info(f"  Rows: {table_ref.num_rows:,}")
            logger.info(f"  Size: {table_ref.num_bytes / (1024*1024):.2f} MB")
            logger.info(f"  Columns: {len(table_ref.schema)}")
            if table_ref.description:
                logger.info(f"  Description: {table_ref.description}")
            logger.info(f"  Created: {table_ref.created}")
            logger.info(f"  Modified: {table_ref.modified}")
            logger.info("")
            
    except Exception as e:
        logger.error(f"Error retrieving dataset information: {e}")

def validate_table_relationships():
    """Validate referential integrity between tables."""
    logger.info("="*60)
    logger.info("REFERENTIAL INTEGRITY VALIDATION")
    logger.info("="*60)
    
    validations = [
        {
            "name": "Objects -> Constituents Relationships",
            "query": """
            SELECT COUNT(*) as orphaned_relationships
            FROM `nga_open_data.objects_constituents` oc
            LEFT JOIN `nga_open_data.objects` o ON oc.objectid = o.objectid
            LEFT JOIN `nga_open_data.constituents` c ON oc.constituentid = c.constituentid
            WHERE o.objectid IS NULL OR c.constituentid IS NULL
            """
        },
        {
            "name": "Objects Terms Relationships",
            "query": """
            SELECT COUNT(*) as orphaned_terms
            FROM `nga_open_data.objects_terms` ot
            LEFT JOIN `nga_open_data.objects` o ON ot.objectid = o.objectid
            WHERE o.objectid IS NULL
            """
        },
        {
            "name": "Published Images Relationships",
            "query": """
            SELECT COUNT(*) as orphaned_images
            FROM `nga_open_data.published_images` pi
            LEFT JOIN `nga_open_data.objects` o ON pi.objectid = o.objectid
            WHERE o.objectid IS NULL
            """
        },
        {
            "name": "Object Associations Self-Relationships",
            "query": """
            SELECT COUNT(*) as orphaned_associations
            FROM `nga_open_data.object_associations` oa
            LEFT JOIN `nga_open_data.objects` parent ON oa.parentobjectid = parent.objectid
            LEFT JOIN `nga_open_data.objects` child ON oa.childobjectid = child.objectid
            WHERE parent.objectid IS NULL OR child.objectid IS NULL
            """
        }
    ]
    
    for validation in validations:
        try:
            result = client.query(validation["query"]).to_dataframe()
            count = result.iloc[0, 0]
            status = "✓ PASS" if count == 0 else f"✗ FAIL ({count} orphaned records)"
            logger.info(f"{validation['name']}: {status}")
        except Exception as e:
            logger.error(f"{validation['name']}: Error - {e}")
    
    logger.info("")

def get_table_sample_data(table_name, limit=5):
    """Get sample data from a table."""
    logger.info(f"Sample data from {table_name} (first {limit} rows):")
    logger.info("-" * 40)
    
    try:
        query = f"SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.{table_name}` LIMIT {limit}"
        df = client.query(query).to_dataframe()
        
        if df.empty:
            logger.info("No data found in table")
        else:
            # Display basic info
            logger.info(f"Columns: {list(df.columns)}")
            logger.info(f"Sample rows:")
            print(df.to_string())
        
        logger.info("")
        
    except Exception as e:
        logger.error(f"Error retrieving sample data from {table_name}: {e}")

def analyze_data_quality():
    """Analyze data quality across tables."""
    logger.info("="*60)
    logger.info("DATA QUALITY ANALYSIS")
    logger.info("="*60)
    
    analyses = [
        {
            "name": "Objects with Missing Images",
            "query": """
            SELECT COUNT(*) as objects_without_images
            FROM `nga_open_data.objects` o
            LEFT JOIN `nga_open_data.published_images` pi ON o.objectid = pi.objectid
            WHERE pi.objectid IS NULL
            """
        },
        {
            "name": "Objects with Missing Artist Attribution",
            "query": """
            SELECT COUNT(*) as objects_without_artists
            FROM `nga_open_data.objects` o
            LEFT JOIN `nga_open_data.objects_constituents` oc 
                ON o.objectid = oc.objectid AND oc.roletype = 'artist'
            WHERE oc.objectid IS NULL
            """
        },
        {
            "name": "Constituents Distribution by Type",
            "query": """
            SELECT constituenttype, COUNT(*) as count
            FROM `nga_open_data.constituents`
            GROUP BY constituenttype
            ORDER BY count DESC
            """
        },
        {
            "name": "Objects by Classification",
            "query": """
            SELECT classification, COUNT(*) as count
            FROM `nga_open_data.objects`
            WHERE classification IS NOT NULL
            GROUP BY classification
            ORDER BY count DESC
            LIMIT 10
            """
        }
    ]
    
    for analysis in analyses:
        try:
            logger.info(f"\n{analysis['name']}:")
            logger.info("-" * len(analysis['name']))
            result = client.query(analysis["query"]).to_dataframe()
            print(result.to_string(index=False))
            logger.info("")
        except Exception as e:
            logger.error(f"Error in {analysis['name']}: {e}")

def create_useful_views():
    """Create useful views for common queries."""
    logger.info("="*60)
    logger.info("CREATING USEFUL VIEWS")
    logger.info("="*60)
    
    views = [
        {
            "name": "artwork_details",
            "description": "Complete artwork information with artist, images, and location",
            "query": """
            CREATE OR REPLACE VIEW `nga_open_data.artwork_details` AS
            SELECT 
                o.objectid,
                o.title,
                o.displaydate,
                o.medium,
                o.dimensions,
                o.classification,
                o.attribution,
                o.creditline,
                c.preferreddisplayname as artist_name,
                c.displaydate as artist_dates,
                c.nationality as artist_nationality,
                pi.iiifurl as primary_image_url,
                l.description as location_description,
                l.site as building
            FROM `nga_open_data.objects` o
            LEFT JOIN `nga_open_data.objects_constituents` oc 
                ON o.objectid = oc.objectid AND oc.roletype = 'artist' AND oc.displayorder = 1
            LEFT JOIN `nga_open_data.constituents` c 
                ON oc.constituentid = c.constituentid
            LEFT JOIN `nga_open_data.published_images` pi 
                ON o.objectid = pi.objectid AND pi.viewtype = 'primary'
            LEFT JOIN `nga_open_data.locations` l 
                ON o.locationid = l.locationid
            WHERE o.accessioned = 1
            """
        },
        {
            "name": "searchable_artworks",
            "description": "Artworks with concatenated searchable text including terms and themes",
            "query": """
            CREATE OR REPLACE VIEW `nga_open_data.searchable_artworks` AS
            SELECT 
                o.objectid,
                o.title,
                o.attribution,
                o.medium,
                o.classification,
                STRING_AGG(DISTINCT ot.term, ', ') as all_terms,
                STRING_AGG(DISTINCT ot.term, ' ') as searchable_text,
                COUNT(DISTINCT pi.uuid) as image_count
            FROM `nga_open_data.objects` o
            LEFT JOIN `nga_open_data.objects_terms` ot ON o.objectid = ot.objectid
            LEFT JOIN `nga_open_data.published_images` pi ON o.objectid = pi.objectid
            WHERE o.accessioned = 1
            GROUP BY o.objectid, o.title, o.attribution, o.medium, o.classification
            """
        },
        {
            "name": "artist_statistics",
            "description": "Artist statistics including artwork count and date ranges",
            "query": """
            CREATE OR REPLACE VIEW `nga_open_data.artist_statistics` AS
            SELECT 
                c.constituentid,
                c.preferreddisplayname as artist_name,
                c.displaydate as artist_dates,
                c.nationality,
                c.beginyear,
                c.endyear,
                COUNT(DISTINCT oc.objectid) as artwork_count,
                MIN(o.beginyear) as earliest_work_year,
                MAX(o.endyear) as latest_work_year,
                COUNT(DISTINCT o.classification) as medium_diversity
            FROM `nga_open_data.constituents` c
            JOIN `nga_open_data.objects_constituents` oc 
                ON c.constituentid = oc.constituentid AND oc.roletype = 'artist'
            JOIN `nga_open_data.objects` o 
                ON oc.objectid = o.objectid
            WHERE c.artistofngaobject = 1 AND o.accessioned = 1
            GROUP BY c.constituentid, c.preferreddisplayname, c.displaydate, 
                     c.nationality, c.beginyear, c.endyear
            HAVING artwork_count > 0
            ORDER BY artwork_count DESC
            """
        }
    ]
    
    for view in views:
        try:
            client.query(view["query"]).result()
            logger.info(f"✓ Created view: {view['name']} - {view['description']}")
        except Exception as e:
            logger.error(f"✗ Failed to create view {view['name']}: {e}")
    
    logger.info("")

def main():
    """Main function to run all utilities."""
    print("NGA BigQuery Dataset Management Utility")
    print("=" * 60)
    
    while True:
        print("\nAvailable operations:")
        print("1. List tables with information")
        print("2. Validate table relationships")
        print("3. Analyze data quality")
        print("4. Create useful views")
        print("5. Get sample data from table")
        print("6. Run all checks")
        print("0. Exit")
        
        choice = input("\nSelect operation (0-6): ").strip()
        
        if choice == "1":
            list_tables_with_info()
        elif choice == "2":
            validate_table_relationships()
        elif choice == "3":
            analyze_data_quality()
        elif choice == "4":
            create_useful_views()
        elif choice == "5":
            table_name = input("Enter table name: ").strip()
            limit = input("Enter number of rows (default 5): ").strip()
            limit = int(limit) if limit.isdigit() else 5
            get_table_sample_data(table_name, limit)
        elif choice == "6":
            list_tables_with_info()
            validate_table_relationships()
            analyze_data_quality()
            create_useful_views()
        elif choice == "0":
            break
        else:
            print("Invalid choice. Please try again.")

if __name__ == "__main__":
    main()
