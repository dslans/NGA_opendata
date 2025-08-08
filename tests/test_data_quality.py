
import pytest
from google.cloud import bigquery
import pandas as pd

# --- Configuration ---
PROJECT_ID = "nga-open"
DATASET_ID = "nga_open_data"

# --- Pytest Fixture for BigQuery Client ---
@pytest.fixture(scope="module")
def bq_client():
    """
    Provides a BigQuery client for the test session.
    """
    return bigquery.Client(project=PROJECT_ID)

# --- Test Cases ---

def test_table_creation(bq_client):
    """
    Tests that all expected tables were created in the dataset.
    """
    expected_tables = {
        "objects",
        "objects_terms",
        "published_images",
        "constituents",
        "objects_constituents",
    }
    tables = {table.table_id for table in bq_client.list_tables(DATASET_ID)}
    assert expected_tables.issubset(tables)

def test_object_id_uniqueness(bq_client):
    """
    Tests that the 'objectid' in the 'objects' table is unique.
    """
    sql = f"""
        SELECT objectid, COUNT(*)
        FROM `{PROJECT_ID}.{DATASET_ID}.objects`
        GROUP BY 1
        HAVING COUNT(*) > 1;
    """
    query_job = bq_client.query(sql)
    results = query_job.result()
    assert results.total_rows == 0, "Found duplicate objectids in the objects table."

def test_critical_columns_not_null(bq_client):
    """
    Tests that critical columns in the 'objects' table are not null.
    """
    sql = f"""
        SELECT COUNT(*)
        FROM `{PROJECT_ID}.{DATASET_ID}.objects`
        WHERE objectid IS NULL OR title IS NULL;
    """
    query_job = bq_client.query(sql)
    results = query_job.result()
    for row in results:
        assert row[0] == 0, "Found NULL values in critical columns of the objects table."

def test_referential_integrity_objects_terms(bq_client):
    """
    Tests that all 'objectid's in 'objects_terms' exist in the 'objects' table.
    """
    sql = f"""
        SELECT t.objectid
        FROM `{PROJECT_ID}.{DATASET_ID}.objects_terms` AS t
        LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.objects` AS o ON t.objectid = o.objectid
        WHERE o.objectid IS NULL
        LIMIT 1;
    """
    query_job = bq_client.query(sql)
    results = query_job.result()
    assert results.total_rows == 0, "Found orphaned objectids in objects_terms."

def test_referential_integrity_published_images(bq_client):
    """
    Tests that all 'objectid's in 'published_images' exist in the 'objects' table.
    """
    sql = f"""
        SELECT t.objectid
        FROM `{PROJECT_ID}.{DATASET_ID}.published_images` AS t
        LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.objects` AS o ON t.objectid = o.objectid
        WHERE o.objectid IS NULL
        LIMIT 1;
    """
    query_job = bq_client.query(sql)
    results = query_job.result()
    assert results.total_rows == 0, "Found orphaned objectids in published_images."

def test_column_rename_in_published_images(bq_client):
    """
    Tests that the 'depictstmsobjectid' column was correctly renamed to 'objectid'.
    """
    sql = f"""
        SELECT column_name
        FROM `{PROJECT_ID}.{DATASET_ID}.INFORMATION_SCHEMA.COLUMNS`
        WHERE table_name = 'published_images' AND column_name = 'objectid';
    """
    query_job = bq_client.query(sql)
    results = query_job.result()
    assert results.total_rows == 1, "The 'objectid' column is missing from the published_images table."
