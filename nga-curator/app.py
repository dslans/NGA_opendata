import streamlit as st
import os
import pandas as pd
import vertexai
from vertexai.generative_models import GenerativeModel
from google.cloud import bigquery
from google.oauth2 import service_account
from dotenv import load_dotenv

load_dotenv()

# Initialize Vertex AI
credentials = service_account.Credentials.from_service_account_file(os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"))
vertexai.init(project="nga-open", credentials=credentials)

st.set_page_config(
    page_title="AI Museum Curator",
    page_icon="🎨",
    layout="wide"
)

st.title("🎨 AI Museum Curator")
st.markdown("*Discover art from the National Gallery of Art's complete collection*")

# Function to get BigQuery client
@st.cache_resource
def get_bigquery_client():
    return bigquery.Client(project='nga-open', credentials=credentials)

# query function with comprehensive search
def query_artworks_enhanced(keywords, search_type="comprehensive", limit=10):
    client = get_bigquery_client()
    
    # Construct WHERE clauses dynamically
    where_clauses = []
    params = []
    
    for keyword in keywords:
        keyword_lower = f"%{keyword.lower()}%"
        
        if search_type == "comprehensive":
            # Search across titles, terms, artist names, and other text fields
            where_clause = """
            (LOWER(o.title) LIKE ? 
             OR LOWER(ot.term) LIKE ? 
             OR LOWER(c.preferreddisplayname) LIKE ?
             OR LOWER(o.medium) LIKE ?
             OR LOWER(o.classification) LIKE ?)
            """
            params.extend([keyword_lower] * 5)
        elif search_type == "terms_only":
            where_clause = "LOWER(ot.term) LIKE ?"
            params.append(keyword_lower)
        elif search_type == "title_only":
            where_clause = "LOWER(o.title) LIKE ?"
            params.append(keyword_lower)
        
        where_clauses.append(where_clause)
    
    # query with detailed information
    query = f"""
        WITH artwork_base AS (
            SELECT DISTINCT
                o.objectid,
                o.title,
                o.displaydate,
                o.medium,
                o.dimensions,
                o.classification,
                o.attribution,
                o.creditline,
                o.accessionnum,
                c.preferreddisplayname as artist_name,
                c.displaydate as artist_dates,
                c.nationality as artist_nationality,
                c.beginyear as artist_birth_year,
                c.endyear as artist_death_year,
                pi.iiifurl,
                pi.width as image_width,
                pi.height as image_height,
                l.description as location_description,
                l.site as building,
                o.beginyear as creation_start_year,
                o.endyear as creation_end_year
            FROM `nga_open_data.objects` o
            LEFT JOIN `nga_open_data.objects_terms` ot ON o.objectid = ot.objectid
            LEFT JOIN `nga_open_data.objects_constituents` oc 
                ON o.objectid = oc.objectid AND oc.roletype = 'artist'
            LEFT JOIN `nga_open_data.constituents` c 
                ON oc.constituentid = c.constituentid
            LEFT JOIN `nga_open_data.published_images` pi 
                ON o.objectid = pi.objectid AND pi.viewtype = 'primary'
            LEFT JOIN `nga_open_data.locations` l 
                ON o.locationid = l.locationid
            WHERE o.accessioned = 1
                AND ({' OR '.join(where_clauses)})
                AND pi.iiifurl IS NOT NULL
        ),
        artwork_with_terms AS (
            SELECT 
                ab.*,
                STRING_AGG(DISTINCT ot2.term, ', ') as all_terms
            FROM artwork_base ab
            LEFT JOIN `nga_open_data.objects_terms` ot2 ON ab.objectid = ot2.objectid
            GROUP BY ab.objectid, ab.title, ab.displaydate, ab.medium, ab.dimensions, 
                     ab.classification, ab.attribution, ab.creditline, ab.accessionnum,
                     ab.artist_name, ab.artist_dates, ab.artist_nationality, 
                     ab.artist_birth_year, ab.artist_death_year, ab.iiifurl,
                     ab.image_width, ab.image_height, ab.location_description,
                     ab.building, ab.creation_start_year, ab.creation_end_year
        )
        SELECT * FROM artwork_with_terms
        ORDER BY CASE 
            WHEN artist_name IS NOT NULL THEN 0 
            ELSE 1 
        END, creation_start_year DESC
        LIMIT {limit}
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(None, "STRING", value) for value in params
        ]
    )
    
    query_job = client.query(query, job_config=job_config)
    return query_job.to_dataframe()

def get_artwork_provenance(object_id):
    """Get provenance information for an artwork."""
    client = get_bigquery_client()
    
    query = """
    SELECT 
        oc.roletype,
        oc.role,
        oc.displaydate,
        c.preferreddisplayname as name,
        oc.displayorder
    FROM `nga_open_data.objects_constituents` oc
    JOIN `nga_open_data.constituents` c ON oc.constituentid = c.constituentid
    WHERE oc.objectid = ?
        AND oc.roletype IN ('owner', 'donor')
    ORDER BY oc.displayorder
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(None, "INTEGER", object_id)
        ]
    )
    
    return client.query(query, job_config=job_config).to_dataframe()

def get_artwork_text_entries(object_id):
    """Get text entries (provenance, exhibition history, etc.) for an artwork."""
    client = get_bigquery_client()
    
    query = """
    SELECT 
        texttype,
        text,
        year
    FROM `nga_open_data.objects_text_entries`
    WHERE objectid = ?
    ORDER BY texttype, year
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(None, "INTEGER", object_id)
        ]
    )
    
    return client.query(query, job_config=job_config).to_dataframe()

def get_related_artworks(object_id):
    """Get related artworks (same artist, similar terms, etc.)."""
    client = get_bigquery_client()
    
    query = """
    WITH current_artwork AS (
        SELECT o.objectid, c.constituentid as artist_id
        FROM `nga_open_data.objects` o
        JOIN `nga_open_data.objects_constituents` oc ON o.objectid = oc.objectid
        JOIN `nga_open_data.constituents` c ON oc.constituentid = c.constituentid
        WHERE o.objectid = ? AND oc.roletype = 'artist'
        LIMIT 1
    )
    SELECT DISTINCT
        o.objectid,
        o.title,
        o.displaydate,
        pi.iiifurl,
        'Same Artist' as relation_type
    FROM current_artwork ca
    JOIN `nga_open_data.objects_constituents` oc ON ca.artist_id = oc.constituentid
    JOIN `nga_open_data.objects` o ON oc.objectid = o.objectid
    LEFT JOIN `nga_open_data.published_images` pi ON o.objectid = pi.objectid AND pi.viewtype = 'primary'
    WHERE o.objectid != ca.objectid
        AND oc.roletype = 'artist'
        AND pi.iiifurl IS NOT NULL
    ORDER BY o.displaydate
    LIMIT 5
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(None, "INTEGER", object_id)
        ]
    )
    
    return client.query(query, job_config=job_config).to_dataframe()

# Sidebar for advanced options
st.sidebar.header("🎯 Search Options")

search_type = st.sidebar.selectbox(
    "Search Scope",
    ["comprehensive", "terms_only", "title_only"],
    index=0,
    help="Choose how broadly to search across the collection"
)

result_limit = st.sidebar.slider(
    "Number of Results",
    min_value=5,
    max_value=20,
    value=10,
    help="Maximum number of artworks to display"
)

show_detailed_info = st.sidebar.checkbox(
    "Show Detailed Information",
    value=False,
    help="Include provenance, exhibition history, and related works"
)

# Main interface
user_theme = st.text_input(
    "Enter a theme for your exhibition:",
    placeholder="e.g., French Impressionism, Abstract Expressionism, Renaissance portraits..."
)

if user_theme:
    with st.spinner("🤖 AI Curator is analyzing your theme..."):
        # 1. Get keywords from Vertex AI
        model = GenerativeModel("gemini-2.5-flash")
        prompt = f"""
        You are an expert museum curator with deep knowledge of art history. 
        Your task is to analyze a user's theme and extract a comprehensive list of search keywords.
        
        Based on the theme '{user_theme}', provide a comma-separated list of 7-10 keywords 
        that would help find relevant artworks in a museum database.
        
        Include:
        - Art movements and styles
        - Techniques and media
        - Subject matter and themes
        - Time periods
        - Geographic regions when relevant
        - Artist names if specific
        
        Focus on terms that would appear in artwork metadata, artist information, or descriptive tags.
        Do not include explanations, just the comma-separated list.
        
        Theme: "{user_theme}"
        Keywords:
        """
        
        try:
            response = model.generate_content(prompt)
            keywords_text = response.text.strip()
            keywords = [keyword.strip() for keyword in keywords_text.split(',')]
            
            st.success(f"🔍 **Curator's Analysis:** {', '.join(keywords)}")
            
            # 2. Search for artworks
            with st.spinner("🔍 Searching the collection..."):
                selected_artworks = query_artworks_enhanced(keywords, search_type, result_limit)
            
            # 3. Display Results
            if not selected_artworks.empty:
                st.header(f"🖼️ Curated Exhibition: *{user_theme}*")
                st.markdown(f"Found **{len(selected_artworks)}** artworks matching your theme")
                
                for index, row in selected_artworks.iterrows():
                    col1, col2 = st.columns([1, 2])
                    
                    with col1:
                        # Display artwork image
                        if pd.notna(row['iiifurl']):
                            image_url = f"{row['iiifurl']}/full/400,/0/default.jpg"
                            st.image(image_url, use_container_width=True)
                        else:
                            st.info("Image not available")
                    
                    with col2:
                        # Basic artwork information
                        st.subheader(row['title'] if pd.notna(row['title']) else "Untitled")
                        
                        if pd.notna(row['artist_name']):
                            artist_info = row['artist_name']
                            if pd.notna(row['artist_dates']):
                                artist_info += f" ({row['artist_dates']})"
                            if pd.notna(row['artist_nationality']):
                                artist_info += f", {row['artist_nationality']}"
                            st.markdown(f"**Artist:** {artist_info}")
                        
                        if pd.notna(row['displaydate']):
                            st.markdown(f"**Date:** {row['displaydate']}")
                        
                        if pd.notna(row['medium']):
                            st.markdown(f"**Medium:** {row['medium']}")
                        
                        if pd.notna(row['dimensions']):
                            st.markdown(f"**Dimensions:** {row['dimensions']}")
                        
                        if pd.notna(row['classification']):
                            st.markdown(f"**Type:** {row['classification']}")
                        
                        if pd.notna(row['accessionnum']):
                            st.markdown(f"**Accession Number:** {row['accessionnum']}")
                        
                        if pd.notna(row['location_description']):
                            st.markdown(f"**Location:** {row['location_description']}")
                        
                        # Terms/Keywords associated with this artwork
                        if pd.notna(row['all_terms']) and row['all_terms'].strip():
                            with st.expander("🏷️ Associated Terms"):
                                st.markdown(row['all_terms'])
                        
                        # Show detailed information if requested
                        if show_detailed_info:
                            object_id = int(row['objectid'])
                            
                            # Provenance information
                            with st.expander("📜 Provenance & Ownership"):
                                provenance_df = get_artwork_provenance(object_id)
                                if not provenance_df.empty:
                                    for _, prov_row in provenance_df.iterrows():
                                        role_display = f"{prov_row['roletype'].title()}"
                                        if pd.notna(prov_row['role']) and prov_row['role'] != prov_row['roletype']:
                                            role_display += f" ({prov_row['role']})"
                                        
                                        date_display = ""
                                        if pd.notna(prov_row['displaydate']):
                                            date_display = f" - {prov_row['displaydate']}"
                                        
                                        st.markdown(f"• **{role_display}:** {prov_row['name']}{date_display}")
                                else:
                                    st.info("No provenance information available")
                            
                            # Text entries (exhibition history, etc.)
                            with st.expander("📚 Additional Information"):
                                text_entries = get_artwork_text_entries(object_id)
                                if not text_entries.empty:
                                    for text_type in text_entries['texttype'].unique():
                                        entries = text_entries[text_entries['texttype'] == text_type]
                                        st.markdown(f"**{text_type.replace('_', ' ').title()}:**")
                                        for _, entry in entries.iterrows():
                                            year_info = f" ({entry['year']})" if pd.notna(entry['year']) else ""
                                            st.markdown(f"• {entry['text'][:200]}{'...' if len(entry['text']) > 200 else ''}{year_info}")
                                else:
                                    st.info("No additional text information available")
                            
                            # Related artworks
                            with st.expander("🎨 Related Artworks"):
                                related_df = get_related_artworks(object_id)
                                if not related_df.empty:
                                    cols = st.columns(min(len(related_df), 3))
                                    for idx, (_, related_row) in enumerate(related_df.iterrows()):
                                        if idx < len(cols):
                                            with cols[idx]:
                                                if pd.notna(related_row['iiifurl']):
                                                    related_image_url = f"{related_row['iiifurl']}/full/200,/0/default.jpg"
                                                    st.image(related_image_url, use_container_width=True)
                                                st.caption(f"{related_row['title']}")
                                                if pd.notna(related_row['displaydate']):
                                                    st.caption(f"({related_row['displaydate']})")
                                else:
                                    st.info("No related artworks found")
                        
                        # Credit line
                        if pd.notna(row['creditline']):
                            st.markdown(f"*{row['creditline']}*")
                    
                    st.divider()
                
                # Summary statistics
                st.sidebar.header("📊 Exhibition Statistics")
                
                # Count by classification
                if 'classification' in selected_artworks.columns:
                    classification_counts = selected_artworks['classification'].value_counts()
                    st.sidebar.markdown("**By Type:**")
                    for class_type, count in classification_counts.items():
                        if pd.notna(class_type):
                            st.sidebar.markdown(f"• {class_type}: {count}")
                
                # Date range
                valid_start_years = selected_artworks['creation_start_year'].dropna()
                valid_end_years = selected_artworks['creation_end_year'].dropna()
                if not valid_start_years.empty and not valid_end_years.empty:
                    earliest = int(valid_start_years.min())
                    latest = int(valid_end_years.max())
                    st.sidebar.markdown(f"**Date Range:** {earliest} - {latest}")
                
                # Artists represented
                unique_artists = selected_artworks['artist_name'].dropna().nunique()
                if unique_artists > 0:
                    st.sidebar.markdown(f"**Artists Represented:** {unique_artists}")
                
            else:
                st.warning("🔍 No artworks found matching your theme. Try different keywords or broaden your search.")
                st.info("💡 **Suggestions:**\n- Use broader terms (e.g., 'landscape' instead of 'sunset landscapes')\n- Try different art movements or periods\n- Include medium types (painting, sculpture, print)\n- Consider geographic regions (French, Italian, American)")
        
        except Exception as e:
            st.error(f"Error generating keywords: {str(e)}")
            st.info("You can try entering keywords directly, separated by commas.")

# Footer with information
st.markdown("---")
st.markdown("""
<div style='text-align: center; color: #666;'>
<p><strong>AI Museum Curator</strong> - Powered by the National Gallery of Art's Complete Open Data Collection</p>
<p>This application searches across all available metadata including artworks, artists, terms, provenance, and exhibition history.</p>
<p>Data courtesy of the <a href='https://www.nga.gov/open-access-images.html' target='_blank'>National Gallery of Art</a> • 
Built with Streamlit, Google Cloud AI, and BigQuery</p>
</div>
""", unsafe_allow_html=True)