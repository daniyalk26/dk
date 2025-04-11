import os
import json
import time
from datetime import datetime, timedelta

import streamlit as st
import boto3
import pandas as pd
import matplotlib.pyplot as plt

import spotipy
from spotipy.oauth2 import SpotifyOAuth

# --------------------- Environment Variables --------------------- #
AWS_ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY')
REGION = os.environ.get('REGION', 'us-east-2')

# Spotify
CLIENT_ID = os.environ.get('SPOTIFY_CLIENT_ID')
CLIENT_SECRET = os.environ.get('SPOTIFY_CLIENT_SECRET')

# Your EC2's public IP or DNS + port
BASE_URL = "http://18.119.104.127:8501"
REDIRECT_URI = f"{BASE_URL}/callback"

# Buckets
S3_RAW_BUCKET = "spotify-raw-data-dk"
PROCESSED_BUCKET = "spotify-processed-data-dk"

# Create S3 client
s3_client = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=REGION
)

# ------------------------ Helper Functions ------------------------ #
def upload_to_s3(file_name, bucket, object_key):
    """Upload a local file to S3."""
    s3_client.upload_file(file_name, bucket, object_key)
    return f"Uploaded {file_name} to bucket '{bucket}' as '{object_key}'."

def display_grid(items, item_type="artist", columns_per_row=3):
    """
    Lay out items (artists or tracks) in a grid (rows of columns).
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

def fetch_processed_data(processed_key):
    """Fetch the processed JSON from S3."""
    try:
        response = s3_client.get_object(Bucket=PROCESSED_BUCKET, Key=processed_key)
        data = json.loads(response['Body'].read().decode('utf-8'))
        return data
    except Exception as e:
        st.error(f"Error fetching processed data: {e}")
        return None

def extract_spotify_data(sp):
    """
    1) Fetch user profile
    2) Fetch top artists/tracks
    3) Fetch recently played (7 days)
    4) Upload combined JSON to S3 (raw data)
    5) Return (raw_data, upload_message, raw_key)
    """
    # Get user info
    current_user = sp.current_user()
    user_id = current_user.get("id", "unknown_user")
    display_name = current_user.get("display_name", "Unknown")

    # Get top artists/tracks
    top_artists_long = sp.current_user_top_artists(limit=50, time_range='long_term')
    top_tracks_long = sp.current_user_top_tracks(limit=50, time_range='long_term')

    # Recently played (7 days)
    seven_days_ago = datetime.now() - timedelta(days=7)
    after_timestamp_ms = int(seven_days_ago.timestamp() * 1000)
    recently_played = sp.current_user_recently_played(limit=50, after=after_timestamp_ms)

    raw_data = {
        "user_id": user_id,
        "display_name": display_name,
        "top_artists_long": top_artists_long,
        "top_tracks_long": top_tracks_long,
        "recently_played": recently_played
    }

    # Save locally
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    local_file_name = f"user_spotify_data_{timestamp}.json"
    with open(local_file_name, 'w') as f:
        json.dump(raw_data, f, indent=2)

    # Upload to S3 (raw data bucket)
    raw_key = f"raw/{local_file_name}"
    upload_message = upload_to_s3(local_file_name, S3_RAW_BUCKET, raw_key)

    return raw_data, upload_message, raw_key


# --------------------------- Main App --------------------------- #
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

    # Create Spotipy OAuth object (manual approach)
    sp_oauth = SpotifyOAuth(
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        redirect_uri=REDIRECT_URI,
        scope="user-read-private user-top-read user-read-recently-played",
        show_dialog=True,
        open_browser=False,        # so it won't try to open a local browser
        cache_path=".spotipyoauth" # optional custom cache
    )

    # Check if we have a "code" parameter in the URL
    query_params = st.experimental_get_query_params()
    code_param = query_params.get("code", [None])[0]

    if code_param is None:
        # User hasn't authorized yet -> show a link to the Spotify login
        st.write("#### Step 1: Authorize with Spotify")
        auth_url = sp_oauth.get_authorize_url()
        st.markdown(f"[**Connect Spotify & Load Data**]({auth_url})")
        st.info("Click the link above to log into Spotify and grant permissions.")
    else:
        # We have a code in the URL, try to exchange for token
        st.info("Exchanging authorization code for token...")
        token_info = sp_oauth.get_access_token(code_param)

        if not token_info:
            st.error("Could not retrieve access token from Spotify. Please try again.")
            return

        access_token = token_info["access_token"]
        sp = spotipy.Spotify(auth=access_token)

        st.success("Spotify authorization successful!")
        st.write("Now extracting your Spotify data...")

        # 1) Extract & upload raw data
        try:
            raw_data, upload_message, raw_key = extract_spotify_data(sp)
            st.success(upload_message)

            with st.expander("View Raw Spotify Data", expanded=False):
                st.json(raw_data)
        except Exception as e:
            st.error(f"Error during Spotify extraction: {e}")
            return

        # 2) Wait for Lambda to process, then fetch processed data
        # Build processed key => raw/user_spotify_data_XXX.json => processed/user_spotify_data_XXX.processed.json
        processed_key = raw_key.replace("raw/", "processed/").replace(".json", ".processed.json")

        st.info("Waiting for data processing (Lambda)...")
        time.sleep(5)

        with st.spinner("Loading processed data..."):
            processed_data = fetch_processed_data(processed_key)

            if processed_data is None:
                st.error("Failed to load processed data from S3.")
                return

            with st.expander("View Processed Data (Full JSON)", expanded=False):
                st.json(processed_data)

            # ----------------- Show the UI sections ----------------- #
            # Section 1) Genre Distribution
            with st.expander("View Genre Distribution", expanded=False):
                genres = processed_data.get("genres", {})
                genre_labels = genres.get("labels", [])
                genre_sizes = genres.get("sizes", [])
                if genre_labels and genre_sizes:
                    fig, ax = plt.subplots()
                    colors = plt.cm.tab20.colors[:len(genre_labels)]
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
