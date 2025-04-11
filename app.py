import os
import json
from datetime import datetime, timedelta
import boto3
import spotipy
from spotipy.oauth2 import SpotifyOAuth

# Retrieve environment variables (already set on your EC2 instance)
CLIENT_ID = os.environ.get('SPOTIFY_CLIENT_ID')
CLIENT_SECRET = os.environ.get('SPOTIFY_CLIENT_SECRET')
# IMPORTANT: Use your EC2 public DNS/IP + port in place of localhost
REDIRECT_URI = 'http://18.119.104.127:8501/callback'
SCOPE = 'user-read-private user-top-read user-read-recently-played'

# If you want to rely on AWS instance roles for S3, you can omit credentials here.
AWS_ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY')
REGION = os.environ.get('REGION', 'us-east-2')

S3_BUCKET = 'spotify-raw-data-dk'
s3_client = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=REGION
)

def authenticate_and_extract():
    """
    1) Get user's Spotify ID and display name.
    2) Get top artists (long_term) for both genre distribution & top 10 artists.
    3) Get top tracks (long_term) for top 10 + popularity analysis.
    4) Get recently played (past 7 days) for day/night stats & daily listening.
    5) Upload combined raw JSON to S3.
    """
    # Remove cache file to force new login each time
    if os.path.exists(".cache"):
        os.remove(".cache")

    sp = spotipy.Spotify(auth_manager=SpotifyOAuth(
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        redirect_uri=REDIRECT_URI,
        scope=SCOPE,
        show_dialog=True
    ))

    # Get user info
    current_user = sp.current_user()
    user_id = current_user.get("id", "unknown_user")
    display_name = current_user.get("display_name", "Unknown")

    # Fetch top artists & tracks
    top_artists_long = sp.current_user_top_artists(limit=50, time_range='long_term')
    top_tracks_long = sp.current_user_top_tracks(limit=50, time_range='long_term')

    # Recently played (7 days)
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
    with open(local_file_name, 'w') as f:
        json.dump(combined_data, f, indent=2)
    
    raw_key = f"raw/{local_file_name}"
    s3_client.upload_file(local_file_name, S3_BUCKET, raw_key)
    upload_message = f"Uploaded {local_file_name} to S3 bucket '{S3_BUCKET}' as '{raw_key}'"

    return combined_data, upload_message, raw_key
