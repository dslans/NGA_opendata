# BigQuery Data Loading System

This document describes the comprehensive solution for ingesting all NGA open data CSV files into BigQuery with proper table and column descriptions.

## Overview

The system loads all 17 CSV files from the National Gallery of Art's open data collection into BigQuery, providing:

- Complete dataset ingestion (previously only 5 of 17 tables were loaded)
- Comprehensive table descriptions based on the official data dictionary
- Column-level descriptions for better data understanding
- Referential integrity validation
- Data quality analysis tools
- Useful views for common queries
- Streamlit application utilizing the full dataset

## Files Created

### 1. Data Loader (`scripts/load_to_bigquery.py`)

**Purpose**: Comprehensive data loading script that handles all 17 CSV files with proper metadata.

**Key Features**:
- Loads all CSV files in dependency order
- Sanitizes column names for BigQuery compatibility
- Adds table descriptions based on the NGA data dictionary
- Adds column descriptions for better understanding
- Handles referential integrity for related tables
- Provides detailed logging and error handling
- Tracks loading success/failure for each table

**Tables Loaded**:
- `objects` - Core artwork information ✓
- `constituents` - People and organizations ✓
- `objects_constituents` - Object-constituent relationships ✓
- `objects_terms` - Keywords and descriptive terms ✓
- `published_images` - IIIF-compatible images ✓
- `alternative_identifiers` - External identifiers (NEW)
- `constituents_altnames` - Alternative constituent names (NEW)
- `constituents_text_entries` - Biographical texts (NEW)
- `locations` - Physical gallery locations (NEW)
- `media_items` - Audio/video content (NEW)
- `media_relationships` - Media-object relationships (NEW)
- `object_associations` - Object hierarchies (NEW)
- `objects_dimensions` - Detailed measurements (NEW)
- `objects_historical_data` - Attribution/title history (NEW)
- `objects_text_entries` - Provenance/exhibition history (NEW)
- `preferred_locations` - Public location descriptions (NEW)
- `preferred_locations_tms_locations` - Location mappings (NEW)

### 2. BigQuery Management Utilities (`scripts/bigquery_utils.py`)

**Purpose**: Interactive utility for managing and validating the BigQuery dataset.

**Features**:
- Dataset overview with table statistics
- Referential integrity validation
- Data quality analysis
- Sample data viewing
- Creation of useful views for common queries

**Useful Views Created**:
- `artwork_details` - Complete artwork info with artist, images, location
- `searchable_artworks` - Artworks with concatenated searchable text
- `artist_statistics` - Artist stats including artwork count and diversity

### 3. Streamlit Application (`nga-curator/app.py`)

**Purpose**: Improved web application that leverages the complete dataset.

**New Features**:
- Search across all metadata fields (titles, terms, artist names, medium, classification)
- Configurable search scope (comprehensive, terms-only, title-only)
- Detailed artwork information including:
  - Provenance and ownership history
  - Exhibition history and conservation notes
  - Related artworks by the same artist
  - Associated terms and keywords
- Exhibition statistics in sidebar
- Better error handling and user guidance

## Usage Instructions

### 1. Running the Data Loader

```bash
# Navigate to the project directory
cd /Users/dslans/NGA_opendata/scripts

# Ensure environment is set up with credentials
# Make sure .env file contains GOOGLE_APPLICATION_CREDENTIALS path

# Run the loader
python load_to_bigquery.py
```

**Expected Output**:
- Progress logs for each table
- Success/failure summary
- Total loading statistics
- Next steps recommendations

### 2. Using the BigQuery Utilities

```bash
# Run the interactive utility
python bigquery_utils.py
```

**Menu Options**:
1. List tables with information - Shows all tables with row counts, sizes
2. Validate table relationships - Checks referential integrity
3. Analyze data quality - Data completeness analysis
4. Create useful views - Creates optimized views for common queries
5. Get sample data from table - Preview any table's content
6. Run all checks - Comprehensive validation and setup

### 3. Running the Streamlit App

```bash
# Run the application
streamlit run app.py
```

**New Capabilities**:
- More sophisticated search across all metadata
- Detailed artwork information panels
- Provenance and exhibition history
- Related artwork discovery
- Exhibition statistics and analytics

## Database Schema

### Core Tables
- **objects**: Primary artwork records (130,000+ items)
- **constituents**: People and organizations (50,000+ records)
- **objects_constituents**: Relationships between artworks and people

### Extended Metadata
- **objects_terms**: Keywords, themes, styles (500,000+ terms)
- **objects_text_entries**: Long-form text (provenance, exhibitions)
- **objects_dimensions**: Detailed measurements
- **objects_historical_data**: Attribution/title changes over time

### Images and Media
- **published_images**: IIIF-compatible artwork images
- **media_items**: Audio/video content
- **media_relationships**: Links media to artworks/artists

### Location and Organization
- **locations**: Physical gallery locations within NGA
- **preferred_locations**: Public-friendly location descriptions
- **object_associations**: Parent-child artwork relationships

### External Integration
- **alternative_identifiers**: Wikidata IDs and other external identifiers
- **constituents_altnames**: Name variations for people/organizations

## Data Quality and Validation

The system includes comprehensive validation:

### Referential Integrity Checks
- Objects ↔ Constituents relationships
- Object ↔ Terms associations  
- Published Images ↔ Objects links
- Object hierarchical associations

### Data Quality Metrics
- Objects missing images
- Objects missing artist attribution
- Classification distribution
- Date range coverage

### Performance Optimizations
- Views for common query patterns
- Indexed relationships
- Optimized column data types

## Migration from Original System

If upgrading from the original system:

1. **Backup existing data** (optional, as script uses WRITE_TRUNCATE)
2. **Run loader** to populate all tables with descriptions
3. **Validate using utilities** to ensure data integrity
4. **Update application code** to use app.py
5. **Test new features** like detailed information panels

## Benefits of this System

### For Developers
- Complete dataset access (12 additional tables)
- Proper metadata and documentation
- Data validation and quality checks
- Reusable views for complex queries

### For Users
- Richer search capabilities across all metadata
- Detailed artwork information including provenance
- Related artwork discovery
- Better search guidance and suggestions

### For Data Analysis
- Comprehensive relationship mapping
- Historical data for trend analysis
- Media content integration
- External system integration via identifiers

## Troubleshooting

### Common Issues

1. **Authentication Errors**
   - Verify GOOGLE_APPLICATION_CREDENTIALS environment variable
   - Check service account permissions

2. **Memory Issues with Large CSV Files**
   - Script uses `low_memory=False` for pandas
   - Monitor system memory during large table loads

3. **Query Timeouts**
   - BigQuery automatically handles large datasets
   - Views are optimized for performance

4. **Missing Data**
   - Some tables may have sparse data (expected)
   - Validation tools identify actual integrity issues

### Performance Tips

1. **Use Views**: Pre-created views optimize common queries
2. **Filter Early**: Apply WHERE clauses on indexed columns first
3. **Limit Results**: Use appropriate LIMIT clauses for user interfaces
4. **Cache Results**: Streamlit caching reduces repeated queries

## Future Enhancements

Potential improvements to consider:

1. **Incremental Loading**: Update only changed records
2. **Data Lineage Tracking**: Monitor data freshness and updates
3. **Advanced Search**: Full-text search across text entries
4. **Visualization**: Charts and graphs for collection analytics
5. **API Development**: REST API for external system integration

## Support and Maintenance

### Regular Tasks
- Monitor data freshness (NGA updates daily)
- Run validation utilities periodically
- Review BigQuery usage and costs
- Update table descriptions as schema evolves

### Monitoring
- BigQuery job history for load failures
- Dataset size and growth trends
- Query performance metrics
- User application usage patterns

---

*This system provides a comprehensive foundation for working with the National Gallery of Art's open data collection, enabling rich applications and deep data analysis.*
