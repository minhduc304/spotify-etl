import spotipy
from spotipy.oauth2 import SpotifyOAuth
from dotenv import load_dotenv
import os
import csv 
from datetime import datetime
import pandas as pd
import requests
from postgres_connection import PostgresConnect


# Load environment variables
load_dotenv()
client_id = os.getenv("SPOTIPY_CLIENT_ID")
client_secret = os.getenv("SPOTIPY_CLIENT_SECRET")
redirect_uri = os.getenv("SPOTIPY_REDIRECT_URI")
scope = os.getenv("SPOTIFY_SCOPE")

sp = spotipy.Spotify(auth_manager=SpotifyOAuth(client_id=client_id,
                                               client_secret=client_secret,
                                               redirect_uri=redirect_uri,
                                               scope=scope))

class SpotifyConnect:
    def __init__(self):
        self.client_id = os.getenv("SPOTIPY_CLIENT_ID")
        self.client_secret = os.getenv("SPOTIPY_CLIENT_SECRET")
        self.redirect_uri = os.getenv("SPOTIPY_REDIRECT_URI")
        self.scope = os.getenv("SPOTIFY_SCOPE")

    def connect(self):
        sp = spotipy.Spotify(auth_manager=SpotifyOAuth(client_id=self.client_id,
                                                       client_secret=self.client_secret,
                                                       redirect_uri=self.redirect_uri,
                                                       scope=self.scope))
        return sp
    
    def get_recent_listen_timestamp(self):
        conn = PostgresConnect().connector()
        cur = conn.cursor()

        query = """" \
        "SELECT MAX(played_at) FROM recently_played
        """

        cur.execute(query)
        result = cur.fetchone()

        if result is None:
            today = datetime.now()
            previous_date = today - pd.DateOffset(days=30)
            previous_date_unix_timestamp = int(previous_date.timestamp()) * 1000
            latest_timestamp = previous_date_unix_timestamp
        else:
            latest_timestamp = int(max(result.timestamp)) * 1000
        
        return latest_timestamp
    
    def get_recently_played(self, limit=50):
        results = sp.current_user_recently_played(limit=limit)
        tracks = []
        for item in results['items']:
            track = item['track']
            track_info = {
                'name': track['name'],
                'artist': ', '.join([artist['name'] for artist in track['artists']]),
                'album': track['album']['name'],
                'played_at': item['played_at']
            }
            tracks.append(track_info)
        return tracks
    
    def save_to_csv(self, tracks):
        datetime_str = datetime.now().strftime("%Y-%m-%d")
        filename = f"recently_played_{datetime_str}.csv"
        os.makedirs('data', exist_ok=True)



# def get_recently_played():
#     results = sp.current_user_recently_played(limit=50)
#     tracks = []
#     for item in results['items']:
#         track = item['track']
#         track_info = {
#             'name': track['name'],
#             'artist': ', '.join([artist['name'] for artist in track['artists']]),
#             'album': track['album']['name'],
#             'played_at': item['played_at']
#         }
#         tracks.append(track_info)

#     return tracks

# def save_to_csv(tracks):
#     datetime_str = datetime.now().strftime("%Y-%m-%d")
#     filename = f"recently_played_{datetime_str}.csv"
#     os.makedirs('data', exist_ok=True)

#     with open(filename, 'a', newline='') as f:
#         writer = csv.DictWriter(f, fieldnames=tracks[0].keys())
#         if os.path.getsize(filename) == 0:
#             writer.writeheader()
#             writer.writerows(tracks)
    
# if __name__ == "__main__":
#     tracks = get_recently_played()
#     save_to_csv(tracks)
#     print(f"Saved {len(tracks)} recently played tracks to CSV.")
