# National Gallery of Art Open Data

This project contains the National Gallery of Art (NGA) open data, which provides information on over 130,000 works of art and artists in its collection. The dataset is provided as a set of CSV files and is updated daily.

## Data Overview

The dataset is comprised of several CSV files, each representing a different aspect of the NGA's collection. Here's a summary of the key data tables:

*   **objects.csv**: The core table, containing information about each art object, including its title, artist, creation date, medium, dimensions, and more.
*   **constituents.csv**: Information about the creators of the art objects, including their names, birth and death dates, and nationality.
*   **objects_constituents.csv**:  A mapping table that links objects to their constituents, defining their roles (e.g., artist, donor, owner).
*   **published_images.csv**:  Information about the images of the art objects that have been published on the NGA's website, including IIIF URLs for accessing the images.
*   **locations.csv**:  The physical locations of the art objects within the NGA's Washington, D.C. campus.
*   **alternative_identifiers.csv**: Alternative identifiers for objects and constituents, such as Wikidata IDs.
*   **constituents_altnames.csv**: Alternative names for the constituents.
*   **constituents_text_entries.csv**:  Bibliographical and other textual information about the constituents.
*   **media_items.csv**:  Audio and video items related to the collection.
*   **media_relationships.csv**:  Links between media items and the art objects or constituents they relate to.
*   **object_associations.csv**:  Relationships between art objects, such as when one object is part of another (e.g., a panel in a triptych).
*   **objects_dimensions.csv**:  Detailed dimensions for the art objects.
*   **objects_historical_data.csv**:  Historical data about the objects, such as previous attributions or titles.
*   **objects_terms.csv**:  Keywords, themes, and other terms associated with the objects.
*   **objects_text_entries.csv**:  Exhibition histories, provenance, and other textual information about the objects.
*   **preferred_locations.csv**:  Public-facing location descriptions.
*   **preferred_locations_tms_locations.csv**:  A mapping between the internal location IDs and the public-facing location descriptions.

## About the Data

The NGA has released this dataset under a Creative Commons Zero (CC0) license, which means it can be used freely without permission from the Gallery. The data includes factual information about the artworks and artists, but does not contain images or other media. However, it does include links and references to media files.

The primary dataset is available for download from the NGA's GitHub repository.

## Project Structure

This project is organized as follows:

*   `data/`: Contains the raw CSV data files from the NGA.
*   `sql_tables/`: Contains SQL table definitions corresponding to the CSV files.
*   `scripts/`: Contains scripts for working with the data, such as refreshing the data from the NGA's GitHub repository.
*   `documentation/`: Contains the data dictionary and other documentation.
