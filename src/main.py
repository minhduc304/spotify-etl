import spotipy
from spotipy.oauth2 import SpotifyOAuth
from dotenv import load_dotenv
import os
import csv 
from datetime import datetime
import pandas as pd
from postgres_connection import PostgresConnect

# load_dotenv('/Users/ducvu/Projects/spotify-etl-/.env')


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
    
    def _get_default_timestamp(self):
        """
        Returns a default timestamp from 30 days ago in Unix milliseconds.
        """
        today = datetime.now()
        previous_date = today - pd.DateOffset(days=30)
        return int(previous_date.timestamp()) * 1000

    def get_recent_listen_timestamp(self):
        """
        Retrieves the most recent timestamp from the recently_played table.
        Creates the table if it doesn't exist.
        """
        conn = PostgresConnect().connector()
        cur = conn.cursor()

        #First, check if the table exists
        check_table_query = """
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = 'recently_played'
        );
        """
        cur.execute(check_table_query)
        table_exists = cur.fetchone()[0]

        if not table_exists:
            create_table_query = """    
            CREATE TABLE recently_played (
                track_id VARCHAR(255),
                name VARCHAR(255),
                artist_id VARCHAR(255),
                artist VARCHAR(255),
                album VARCHAR(255),
                genre VARCHAR(255),
                played_at TIMESTAMP
                primary key (played_at)
            );
            """
            cur.execute(create_table_query)
            conn.commit()

            #Return a default timestamp (30 days ago) since there's no data
            return self._get_default_timestamp()

        #If the table exists, get the latest timestamp
        query = "SELECT MAX(played_at) FROM recently_played"
        cur.execute(query)
        result = cur.fetchone()

        if result[0] is None:
        # No records in the table yet
            latest_timestamp = self._get_default_timestamp()
        else:
            # Convert the timestamp to Unix timestamp in milliseconds
            latest_timestamp = int(result[0].timestamp()) * 1000
        
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
                'track id': track['id'],
                'artist_id': ', '.join([artist['id'] for artist in track['artists']]),
                'artist': ', '.join([artist['name'] for artist in track['artists']]),
                'album': track['album']['name'],
                'genres': sp.artist(track['artists'][0]['id'])['genres'],
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
            fieldnames = ['name', 'track_id', 'artist', 'artist_id', 'album', 'genres', 'played_at']
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
