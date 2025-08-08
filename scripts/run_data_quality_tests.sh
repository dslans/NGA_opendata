#!/bin/bash

# This script runs the data quality tests, generates an HTML report,
# and archives the report with a timestamp.

# 1. Define file and directory names
REPORT_NAME="test_data_load_report"
LATEST_REPORT_FILE="${REPORT_NAME}.html"
ARCHIVE_DIR="tests/reports"
TIMESTAMP=$(date +"%Y-%m-%d_%H-%M-%S")
ARCHIVED_REPORT_FILE="${ARCHIVE_DIR}/${REPORT_NAME}_${TIMESTAMP}.html"

# 2. Ensure the archive directory exists
mkdir -p $ARCHIVE_DIR

# 3. Run pytest, targeting the specific data quality test file
#    and generating the initial HTML report.
pytest "tests/test_data_quality.py" --html=$LATEST_REPORT_FILE --self-contained-html

# 4. Copy the generated report to the archive with a timestamp
cp $LATEST_REPORT_FILE $ARCHIVED_REPORT_FILE

echo "Data quality test complete."
echo "Latest report is at: ${LATEST_REPORT_FILE}"
echo "Archived report is at: ${ARCHIVED_REPORT_FILE}"
