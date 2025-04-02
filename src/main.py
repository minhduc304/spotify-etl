import spotipy
from spotipy.oauth2 import SpotifyOAuth
from dotenv import load_dotenv
import os
import csv 
from datetime import datetime
import pandas as pd
from postgres_connection import PostgresConnect

load_dotenv()


class SpotifyConnect:
    def __init__(self):
        # Load environment variables from .env file
        # Get secrets
        self.client_id = os.getenv("SPOTIFY_CLIENT_ID")
        self.client_secret = os.getenv("SPOTIFY_CLIENT_SECRET")
        self.redirect_uri = os.getenv("SPOTIFY_REDIRECT_URI")
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

        query = "SELECT MAX(played_at) FROM recently_played"

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
        # Get the latest timestamp from the database
        latest_timestamp = self.get_recent_listen_timestamp()

        # Get recently played tracks from Spotify
        results = sp.current_user_recently_played(limit=limit, after=latest_timestamp)
        tracks = []
        for item in results['items']:
            track = item['track']
            track_info = {
                'name': track['name'],
                'artist_id': ', '.join([artist['id'] for artist in track['artists']]),
                'artist': ', '.join([artist['name'] for artist in track['artists']]),
                'album': track['album']['name'],
                'genre': ', '.join([genre for genre in track['album']['genres']]),
                'played_at': item['played_at']
            }
            tracks.append(track_info)

        return tracks
        
    
    def save_to_csv(self, tracks):
        # Check if the file already exists, if so, append to it
        # If not, create a new file
        file_path = 'recently_played.csv'
        file_exists = os.path.isfile(file_path)
        with open(file_path, mode='a', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['name', 'artist', 'album', 'played_at']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            # Write header only if the file does not exist
            if not file_exists:
                writer.writeheader()

            for track in tracks:
                writer.writerow(track)



    
if __name__ == "__main__":
    # Connect to Spotify
    spotify = SpotifyConnect()
    sp = spotify.connect()

    # Get recently played tracks
    tracks = spotify.get_recently_played(limit=50)

    # Save to CSV
    spotify.save_to_csv(tracks)
    
    # Get the latest timestamp from the database
    latest_timestamp = spotify.get_recent_listen_timestamp()
    
    print(f"Latest timestamp: {latest_timestamp}")
