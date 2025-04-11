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
BASE_URL = "http://18.119.104.127:8501"  # Change if needed
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

# ------------- Helper Functions (Same as Before) ------------- #
def display_grid(items, item_type="artist", columns_per_row=3):
    """Lay out items (artists or tracks) in a grid, forcing uniform width=160."""
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
                    col.image(image_url, width=160)
            else:  # track
                name = item.get('track_name', 'Unknown Track')
                artist = item.get('artist_name', 'Unknown Artist')
                image_url = item.get('album_image')
                col.markdown(f"**{rank}. {name}**")
                col.caption(f"by {artist}")
                if image_url:
                    col.image(image_url, width=160)

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
    5) Return (raw_data, raw_key)
    """
    current_user = sp.current_user()
    user_id = current_user.get("id", "unknown_user")
    display_name = current_user.get("display_name", "Unknown")

    top_artists_long = sp.current_user_top_artists(limit=50, time_range='long_term')
    top_tracks_long = sp.current_user_top_tracks(limit=50, time_range='long_term')

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

    # Save locally & upload to raw S3 bucket (for your Lambda to process)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    local_file_name = f"user_spotify_data_{timestamp}.json"
    with open(local_file_name, 'w') as f:
        json.dump(raw_data, f, indent=2)

    raw_key = f"raw/{local_file_name}"
    try:
        s3_client.upload_file(local_file_name, S3_RAW_BUCKET, raw_key)
    except Exception as e:
        st.error(f"Error uploading raw data to S3: {e}")

    return raw_data, raw_key

# --------------------------- Main App --------------------------- #
def main():
    st.title("Spotify Snapshot")
    st.markdown("""
    **Connect to see your**:
    - Genre Distribution
    - Mainstream Score
    - Day vs. Night habits
    - Top 10 Artists & Tracks
    - Daily Listening (7 days)
    """)

    # 1) Spotipy OAuth with NO shared cache file
    sp_oauth = SpotifyOAuth(
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        redirect_uri=REDIRECT_URI,
        scope="user-read-private user-top-read user-read-recently-played",
        show_dialog=True,
        open_browser=False,
        cache_path=None  # <--- DISABLE the default file-based cache
    )

    # 2) Check st.session_state for existing token
    if "spotify_token" not in st.session_state:
        st.session_state["spotify_token"] = None

    # 3) Check if we have a "code" parameter in the URL
    code_param = st.query_params.get("code", None)

    if not st.session_state["spotify_token"]:
        # We don't have a token in session_state, so let's see if user just got redirected back
        if code_param is None:
            # No code -> show link to Spotify login
            auth_url = sp_oauth.get_authorize_url()
            st.markdown(f"[**Connect Spotify**]({auth_url})")
            st.info("Click above to grant permissions and load your data.")
            return
        else:
            # 4) We have code -> exchange for a token (no file cache)
            token_info = sp_oauth.get_access_token(code_param, check_cache=False)
            if not token_info:
                st.error("Could not retrieve an access token from Spotify. Please try again.")
                return
            # Store token in session_state for this user's session
            st.session_state["spotify_token"] = token_info["access_token"]

    # If we reach here, st.session_state["spotify_token"] is set
    access_token = st.session_state["spotify_token"]
    sp = spotipy.Spotify(auth=access_token)

    # 5) Extract & upload raw data
    try:
        raw_data, raw_key = extract_spotify_data(sp)
    except Exception as e:
        st.error(f"Error extracting Spotify data: {e}")
        return

    # 6) Wait for Lambda to process, then fetch processed data
    processed_key = raw_key.replace("raw/", "processed/").replace(".json", ".processed.json")

    time.sleep(5)  # Wait for your Lambda to produce the processed JSON
    with st.spinner("Loading your processed Spotify insights..."):
        processed_data = fetch_processed_data(processed_key)

    if processed_data is None:
        st.error("Failed to load processed data from S3.")
        return

    # ----------------------- UI Sections ----------------------- #
    # 1) Genre Distribution
    with st.expander("Genre Distribution", expanded=False):
        genres = processed_data.get("genres", {})
        labels = genres.get("labels", [])
        sizes = genres.get("sizes", [])
        if labels and sizes:
            fig, ax = plt.subplots()
            colors = plt.cm.tab20.colors[:len(labels)]
            ax.pie(sizes, labels=labels, autopct='%1.1f%%', colors=colors, startangle=140)
            ax.axis('equal')
            st.pyplot(fig)
        else:
            st.info("No genre data available.")

    # 2) Mainstream Score
    with st.expander("Mainstream Score", expanded=False):
        score = processed_data.get("mainstream_score", 0)
        score_rounded = round(score, 1)
        if score_rounded > 0:
            st.write(f"Your average track popularity is **{score_rounded}** (out of 100).")
            if score_rounded >= 70:
                st.write("Wow, you’re very mainstream — radio hits all day!")
            elif score_rounded >= 40:
                st.write("Moderately mainstream — a nice blend of hits and hidden gems.")
            else:
                st.write("You’re quite indie — you love deep cuts and obscure tracks!")
        else:
            st.info("No mainstream data found.")

    # 3) Day vs. Night
    with st.expander("Day vs. Night Listening", expanded=False):
        d = processed_data.get("day_vs_night", {})
        day_percent = d.get("day_percent", 0)
        night_percent = d.get("night_percent", 0)
        st.write(f"**{day_percent}%** day, **{night_percent}%** night.")
        if night_percent > day_percent:
            st.write("You're a midnight music muncher! 🌙")
        else:
            st.write("You're more of a daytime star! ☀️")

    # 4) Top 10 Artists
    with st.expander("Top 10 Artists", expanded=False):
        top_artists = processed_data.get("top_artists", [])
        display_grid(top_artists, item_type="artist", columns_per_row=3)

    # 5) Top 10 Tracks
    with st.expander("Top 10 Tracks", expanded=False):
        top_tracks = processed_data.get("top_tracks", [])
        display_grid(top_tracks, item_type="track", columns_per_row=3)

    # 6) Daily Listening
    with st.expander("Daily Listening (Past 7 Days)", expanded=False):
        lt = processed_data.get("listening_time", {})
        labels = lt.get("daily_listening_labels", [])
        values = lt.get("daily_listening_values", [])
        if labels and values:
            df_listen = pd.DataFrame({"Date": labels, "Minutes": values}).set_index("Date")
            st.bar_chart(df_listen)
        else:
            st.info("No daily listening data found.")

    # RAW & PROCESSED at the end
    st.subheader("Advanced: Raw and Processed JSON")
    with st.expander("View Raw Spotify Data"):
        st.json(raw_data)
    with st.expander("View Processed Data (Full JSON)"):
        st.json(processed_data)


if __name__ == "__main__":
    main()
