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

st.title("AI Museum Curator")

# Function to get BigQuery client
@st.cache_resource
def get_bigquery_client():
    return bigquery.Client(project='nga-open', credentials=credentials)

# Function to query BigQuery
def query_bigquery(keywords):
    client = get_bigquery_client()
    
    # Construct the WHERE clause dynamically
    where_clauses = []
    params = []
    for keyword in keywords:
        where_clauses.append("(LOWER(t.term) LIKE ? OR LOWER(o.title) LIKE ?)")
        params.extend([f"%{keyword.lower()}%", f"%{keyword.lower()}%"])

    # Construct the full query
    query = f"""
        SELECT DISTINCT
            o.title,
            t.term,
            FIRST_VALUE(o.medium) OVER(title_key) AS medium,
            FIRST_VALUE(c.preferreddisplayname) OVER(title_key) AS preferreddisplayname,
            FIRST_VALUE(c.beginyear) OVER(title_key) AS beginyear,
            FIRST_VALUE(c.endyear) OVER(title_key) AS endyear,
            FIRST_VALUE(pi.iiifurl) OVER(title_key) AS iiifurl,
        FROM
            `nga_open_data.objects` AS o
        JOIN
            `nga_open_data.objects_terms` AS t ON o.objectid = t.objectid
        JOIN
            `nga_open_data.published_images` AS pi ON o.objectid = pi.objectid
        JOIN
            `nga_open_data.objects_constituents` AS oc ON o.objectid = oc.objectid
        JOIN
            `nga_open_data.constituents` AS c ON oc.constituentid = c.constituentid
        WHERE
            {' OR '.join(where_clauses)}
            AND pi.viewtype = 'primary'
        WINDOW title_key AS (PARTITION BY o.title ORDER BY c.endyear DESC)
        LIMIT 5
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(None, "STRING", value) for value in params
        ]
    )
    
    query_job = client.query(query, job_config=job_config)
    return query_job.to_dataframe()

user_theme = st.text_input("Enter a theme for your exhibition:")

if user_theme:
    st.write(f"Finding artworks related to: {user_theme}")

    # 1. Reasoning Step: Get keywords from Vertex AI
    model = GenerativeModel("gemini-2.5-flash")
    prompt = f"""
    You are an expert museum curator. Your task is to analyze a user's theme and extract a concise list of search keywords.
    Based on the theme '{user_theme}', provide a comma-separated list of 5-7 keywords to search for in an art database.
    Focus on nouns, artistic styles, and key concepts. Do not include any explanation, just the list.

    Example Theme: "Paintings of the Dutch Golden Age"
    Example Keywords: "dutch golden age, painting, portrait, still life, landscape"

    Theme: "{user_theme}"
    Keywords:
    """
    response = model.generate_content(prompt)
    keywords_text = response.text.strip()
    keywords = [keyword.strip() for keyword in keywords_text.split(',')]

    st.write(f"Curator's keywords: {', '.join(keywords)}")

    # 2. Acting Step: Search for artworks using the keywords in BigQuery
    selected_artworks = query_bigquery(keywords)

    # 3. Display Results
    if not selected_artworks.empty:
        st.subheader("Curated Exhibition:")
        for index, row in selected_artworks.iterrows():
            st.image(f"{row['iiifurl']}/full/full/0/default.jpg", width=300)
            st.write(f"**{row['title']}**")
            st.write(f"*{row['preferreddisplayname']}* ({row['beginyear']} - {row['endyear']})")
            st.write(f"_{row['medium']}_")
            st.divider()
    else:
        st.write("No artworks found matching your theme. Please try a different theme.")