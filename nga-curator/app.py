import streamlit as st
import os
import pandas as pd
import vertexai
from vertexai.generative_models import GenerativeModel

# Initialize Vertex AI
vertexai.init()

st.title("AI Museum Curator")

# Load the data
@st.cache_data
def load_data():
    # Get the absolute path to the script's directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(script_dir, "..", "data")

    objects = pd.read_csv(os.path.join(data_dir, "objects.csv"))
    terms = pd.read_csv(os.path.join(data_dir, "objects_terms.csv"))
    images = pd.read_csv(os.path.join(data_dir, "published_images.csv"))
    constituents = pd.read_csv(os.path.join(data_dir, "constituents.csv"))
    obj_const = pd.read_csv(os.path.join(data_dir, "objects_constituents.csv"))

    # Merge the datasets
    artworks = pd.merge(objects, terms, on="objectid")
    artworks = pd.merge(artworks, images, left_on="objectid", right_on="depictstmsobjectid")
    artworks = pd.merge(artworks, obj_const, on="objectid")
    artworks = pd.merge(artworks, constituents, on="constituentid")

    # Filter for primary images
    artworks = artworks[artworks["view_type"] == "primary"]

    return artworks

artworks_df = load_data()

user_theme = st.text_input("Enter a theme for your exhibition:")

if user_theme:
    st.write(f"Finding artworks related to: {user_theme}")

    # 1. Reasoning Step: Get keywords from Vertex AI
    model = GenerativeModel("gemini-1.0-pro")
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

    # 2. Acting Step: Search for artworks using the keywords
    def find_artworks(df, search_keywords):
        """
        Searches the artwork DataFrame for matches in the 'term' or 'title' columns.
        """
        # Create a regex pattern to search for any of the keywords (case-insensitive)
        search_pattern = '|'.join(search_keywords)
        
        # Search in both 'term' and 'title' columns
        results = df[
            df['term'].str.contains(search_pattern, case=False, na=False) |
            df['title'].str.contains(search_pattern, case=False, na=False)
        ]
        return results.drop_duplicates(subset=['objectid']).head(5) # Return top 5 unique results

    selected_artworks = find_artworks(artworks_df, keywords)

    # 3. Display Results
    if not selected_artworks.empty:
        st.subheader("Curated Exhibition:")
        for index, row in selected_artworks.iterrows():
            st.image(row['iiif_url'], width=300)
            st.write(f"**{row['title']}**")
            st.write(f"*{row['displayname']}* ({row['beginendyear']})")
            st.write(f"_{row['medium']}_")
            st.divider()
    else:
        st.write("No artworks found matching your theme. Please try a different theme.")
