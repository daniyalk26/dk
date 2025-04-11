import os
import json
from datetime import datetime, timedelta

import streamlit as st
import boto3
import spotipy
from spotipy.oauth2 import SpotifyOAuth
####update
# ================== ENVIRONMENT VARIABLES ================== #
CLIENT_ID = os.environ.get('SPOTIFY_CLIENT_ID')
CLIENT_SECRET = os.environ.get('SPOTIFY_CLIENT_SECRET')
# IMPORTANT: The base URL of your EC2 + "/callback".
# Make sure it matches your Spotify Developer Dashboard settings EXACTLY.
BASE_URL = "http://18.119.104.127:8501"
REDIRECT_URI = f"{BASE_URL}/callback"
SCOPE = "user-read-private user-top-read user-read-recently-played"

AWS_ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY')
REGION = os.environ.get('REGION', 'us-east-2')
S3_BUCKET = "spotify-raw-data-dk"

# =============== BOTO3 CLIENT FOR S3 UPLOAD =============== #
s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=REGION
)

# ================ EXTRACTION FUNCTION ====================== #
def extract_and_upload_to_s3(sp):
    """
    Use an *already authorized* Spotipy client (sp) to:
    1) Fetch user info
    2) Fetch top artists, top tracks
    3) Fetch recently played (past 7 days)
    4) Upload combined JSON to S3
    5) Return data + upload message
    """
    # Get user info
    current_user = sp.current_user()
    user_id = current_user.get("id", "unknown_user")
    display_name = current_user.get("display_name", "Unknown")

    # Fetch top artists & tracks
    top_artists_long = sp.current_user_top_artists(limit=50, time_range="long_term")
    top_tracks_long = sp.current_user_top_tracks(limit=50, time_range="long_term")

    # Fetch recently played (7 days)
    seven_days_ago = datetime.now() - timedelta(days=7)
    after_timestamp_ms = int(seven_days_ago.timestamp() * 1000)
    recently_played = sp.current_user_recently_played(limit=50, after=after_timestamp_ms)

    # Combine into one JSON
    combined_data = {
        "user_id": user_id,
        "display_name": display_name,
        "top_artists_long": top_artists_long,
        "top_tracks_long": top_tracks_long,
        "recently_played": recently_played
    }

    # Save locally, then upload to S3
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    local_file_name = f"user_spotify_data_{timestamp}.json"
    with open(local_file_name, "w") as f:
        json.dump(combined_data, f, indent=2)

    raw_key = f"raw/{local_file_name}"
    s3_client.upload_file(local_file_name, S3_BUCKET, raw_key)
    upload_message = (
        f"Uploaded {local_file_name} to S3 bucket '{S3_BUCKET}' as '{raw_key}'."
    )

    return combined_data, upload_message


# ================ STREAMLIT APP ============================ #
def main():
    st.title("Spotify Dashboard – EC2 Edition")

    # 1) Create a SpotifyOAuth object WITHOUT forcing it to start a local server
    sp_oauth = SpotifyOAuth(
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        redirect_uri=REDIRECT_URI,  
        scope=SCOPE,
        show_dialog=True,
        cache_path=".spotipyoauthcache",  # optional custom cache path
        open_browser=False               # so it won't try to open local browser
    )

    # 2) Check if we have a "?code=..." param in the current URL
    query_params = st.experimental_get_query_params()
    code_param = query_params.get("code", [None])[0]

    if code_param is None:
        # 3) If no code yet, present a link button to start Spotify authorization
        auth_url = sp_oauth.get_authorize_url()
        st.markdown("### Step 1: Authorize with Spotify")
        st.write("Click below to log into Spotify and grant permissions:")
        st.markdown(f"[**Authorize on Spotify**]({auth_url})")
    else:
        # 4) We have ?code=..., let's exchange it for a token
        st.info("Exchanging authorization code for token...")
        token_info = sp_oauth.get_access_token(code_param)

        if not token_info:
            st.error("Could not retrieve access token from Spotify. Please try again.")
            return

        access_token = token_info["access_token"]
        sp = spotipy.Spotify(auth=access_token)
        st.success("Spotify authorization successful!")

        # 5) Now use the authorized 'sp' client to do your extraction + S3 upload
        with st.spinner("Extracting your Spotify data..."):
            combined_data, upload_message = extract_and_upload_to_s3(sp)
        st.success(upload_message)

        # 6) Optionally show the raw data
        with st.expander("View Raw Data", expanded=False):
            st.json(combined_data)


if __name__ == "__main__":
    main()
