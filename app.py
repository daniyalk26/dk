import streamlit as st
import boto3
import json
import pandas as pd
import time
import matplotlib.pyplot as plt
import numpy as np
import os


from spotify_etl import authenticate_and_extract

AWS_ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY')
REGION = 'us-east-2'
PROCESSED_BUCKET = 'spotify-processed-data-dk'

def fetch_processed_data(processed_key):
    try:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=REGION
        )
        response = s3_client.get_object(Bucket=PROCESSED_BUCKET, Key=processed_key)
        data = json.loads(response['Body'].read().decode('utf-8'))
        return data
    except Exception as e:
        st.error(f"Error fetching processed data: {e}")
        return None

def display_grid(items, item_type="artist", columns_per_row=3):
    """
    Lay out items (artists or tracks) in a grid (rows of columns).
    
    :param items: List of dicts, each dict with keys like:
                  - rank
                  - artist_name (or track_name)
                  - artist_image (or album_image)
    :param item_type: "artist" or "track"
    :param columns_per_row: How many columns to use per row
    """
    if not items:
        st.info(f"No {item_type} data found.")
        return

    rows = [items[i:i+columns_per_row] for i in range(0, len(items), columns_per_row)]
    for row in rows:
        cols = st.columns(len(row))
        for col, item in zip(cols, row):
            rank = item.get('rank', '?')
            if item_type == "artist":
                name = item.get('artist_name', 'Unknown Artist')
                image_url = item.get('artist_image')
                col.markdown(f"**{rank}. {name}**")
                if image_url:
                    col.image(image_url, use_container_width=True)
            else:  # track
                name = item.get('track_name', 'Unknown Track')
                artist = item.get('artist_name', 'Unknown Artist')
                image_url = item.get('album_image')
                col.markdown(f"**{rank}. {name}**")
                col.caption(f"by {artist}")
                if image_url:
                    col.image(image_url, use_container_width=True)

def main():
    st.title("Spotify Dashboard – Day vs Night, Mainstream Score, and More!")
    st.markdown("""
        This dashboard lets you connect to your Spotify account and see:
        - **Genre Distribution** in a colorful pie chart
        - **Mainstream Score**
        - **Day vs. Night** listening habits
        - **Top 10 Artists** & **Top 10 Tracks**
        - **Daily Listening** stats for the past 7 days
    """)

    connect_spotify = st.button("Connect Spotify & Load Data")
    if connect_spotify:
        with st.spinner("Authenticating and extracting data from Spotify..."):
            try:
                raw_data, upload_message, raw_key = authenticate_and_extract()
                st.success(upload_message)

                with st.expander("View Raw Spotify Data", expanded=False):
                    st.json(raw_data)

            except Exception as e:
                st.error(f"Error during Spotify authentication/extraction: {e}")
                return

        # Build processed key
        processed_key = raw_key.replace("raw/", "processed/").replace(".json", ".processed.json")

        st.info("Waiting for data processing (Lambda)...")
        time.sleep(5)  # allow Lambda time to process

        with st.spinner("Loading processed data..."):
            processed_data = fetch_processed_data(processed_key)

            if processed_data is None:
                st.error("Failed to load processed data from S3.")
                return

            with st.expander("View Processed Data (Full JSON)", expanded=False):
                st.json(processed_data)

            # Section 1) Genre Distribution
            with st.expander("View Genre Distribution", expanded=False):
                genres = processed_data.get("genres", {})
                genre_labels = genres.get("labels", [])
                genre_sizes = genres.get("sizes", [])
                if genre_labels and genre_sizes:
                    fig, ax = plt.subplots()
                    colors = plt.cm.tab20.colors
                    colors = list(colors[:len(genre_labels)])
                    ax.pie(
                        genre_sizes,
                        labels=genre_labels,
                        autopct='%1.1f%%',
                        colors=colors,
                        startangle=140
                    )
                    ax.axis('equal')  # make the pie chart a circle
                    st.pyplot(fig)
                else:
                    st.info("No genre data found.")

            # Section 2) Mainstream Score
            with st.expander("View Mainstream Score", expanded=False):
                mainstream_score = processed_data.get("mainstream_score", 0)
                mainstream_score_rounded = round(mainstream_score, 1)
                if mainstream_score_rounded > 0:
                    st.write(f"Your average track popularity is **{mainstream_score_rounded}** out of 100.")
                    if mainstream_score_rounded >= 70:
                        st.write("Wow, you’re very mainstream — your playlist could dominate the radio!")
                    elif mainstream_score_rounded >= 40:
                        st.write("You’re moderately mainstream — a balanced blend of hits and hidden gems.")
                    else:
                        st.write("You’re quite indie — you dig deep cuts and obscure tracks!")
                else:
                    st.info("No mainstream data found.")

            # Section 3) Day vs. Night
            with st.expander("View Day vs. Night Listening", expanded=False):
                day_vs_night = processed_data.get("day_vs_night", {})
                day_percent = day_vs_night.get("day_percent", 0)
                night_percent = day_vs_night.get("night_percent", 0)
                st.write(f"**{day_percent}%** of your listening is during the day, **{night_percent}%** at night.")
                if night_percent > day_percent:
                    st.write("You’re a midnight music muncher! 🌙")
                else:
                    st.write("You’re more of a daytime music star! ☀️")

            # Section 4) Top 10 Artists
            with st.expander("View Top 10 Artists", expanded=False):
                top_artists = processed_data.get("top_artists", [])
                display_grid(top_artists, item_type="artist", columns_per_row=3)

            # Section 5) Top 10 Tracks
            with st.expander("View Top 10 Tracks", expanded=False):
                top_tracks = processed_data.get("top_tracks", [])
                display_grid(top_tracks, item_type="track", columns_per_row=3)

            # Section 6) Daily Listening
            with st.expander("View Daily Listening (Past 7 Days)", expanded=False):
                listening_time = processed_data.get("listening_time", {})
                labels = listening_time.get("daily_listening_labels", [])
                values = listening_time.get("daily_listening_values", [])
                if labels and values:
                    df_listen = pd.DataFrame({"Date": labels, "Minutes": values}).set_index("Date")
                    st.bar_chart(df_listen)
                else:
                    st.info("No daily listening data found.")

if __name__ == "__main__":
    main()
