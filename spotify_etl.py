import os
import json
from datetime import datetime, timedelta
import boto3
import spotipy
from spotipy.oauth2 import SpotifyOAuth


CLIENT_ID = os.environ.get('SPOTIFY_CLIENT_ID')
CLIENT_SECRET = os.environ.get('SPOTIFY_CLIENT_SECRET')
REDIRECT_URI = 'http://localhost:8888/callback'
SCOPE = 'user-read-private user-top-read user-read-recently-played'

S3_BUCKET = 'spotify-raw-data-dk'
s3_client = boto3.client(
    's3',
    aws_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key= os.environ.get('AWS_SECRET_ACCESS_KEY'),
    region_name= os.environ.get('REGION', 'us-east-2')  # default if not set

)

def upload_to_s3(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket."""
    if object_name is None:
        object_name = os.path.basename(file_name)
    try:
        s3_client.upload_file(file_name, bucket, object_name)
        return f"Uploaded {file_name} to bucket '{bucket}' as '{object_name}'."
    except Exception as e:
        raise Exception(f"Error uploading file: {e}")

def authenticate_and_extract():
    """
    1) Get user's Spotify ID and display name.
    2) Get top artists (long_term) for both genre distribution & top 10 artists.
    3) Get top tracks (long_term) for top 10 + popularity analysis.
    4) Get recently played (past 7 days) for day/night stats & daily listening.
    5) Upload combined raw JSON to S3.
    """
    # Remove cache file to force a new login each time.
    if os.path.exists(".cache"):
        os.remove(".cache")

    sp = spotipy.Spotify(auth_manager=SpotifyOAuth(
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        redirect_uri=REDIRECT_URI,
        scope=SCOPE,
        show_dialog=True
    ))

    # ---- NEW: Fetch current Spotify user profile (for user_id) ----
    current_user = sp.current_user()
    user_id = current_user.get("id", "unknown_user")
    display_name = current_user.get("display_name", "Unknown")

    # (1) Long-term top artists
    top_artists_long = sp.current_user_top_artists(limit=50, time_range='long_term')
    
    # (2) Long-term top tracks
    top_tracks_long = sp.current_user_top_tracks(limit=50, time_range='long_term')

    # (3) Recently played for the past 7 days
    seven_days_ago = datetime.now() - timedelta(days=7)
    after_timestamp_ms = int(seven_days_ago.timestamp() * 1000)
    recently_played = sp.current_user_recently_played(
        limit=50,
        after=after_timestamp_ms
    )

    # Combine into one JSON
    combined_data = {
        "user_id": user_id,               # <--- store user info
        "display_name": display_name,
        "top_artists_long": top_artists_long,
        "top_tracks_long": top_tracks_long,
        "recently_played": recently_played
    }

    # Save & upload raw JSON to S3
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    local_file_name = f"user_spotify_data_{timestamp}.json"
    with open(local_file_name, 'w') as f:
        json.dump(combined_data, f, indent=2)
    
    raw_key = f"raw/{local_file_name}"
    upload_message = upload_to_s3(local_file_name, S3_BUCKET, raw_key)

    return combined_data, upload_message, raw_key
