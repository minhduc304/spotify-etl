import spotipy
from spotipy.oauth2 import SpotifyOAuth
from dotenv import load_dotenv
import os
from datetime import datetime
import pandas as pd
import json
from postgres_connection import PostgresConnect


class SpotifyETL:
    def __init__(self):
        # Load environment variables from .env file
        load_dotenv()

        # Get secrets
        self.client_id = os.getenv("SPOTIFY_CLIENT_ID")
        self.client_secret = os.getenv("SPOTIFY_CLIENT_SECRET")
        self.redirect_uri = os.getenv("SPOTIFY_REDIRECT_URI")
        self.scope = os.getenv("SPOTIFY_SCOPE")

        # Connect to PostgreSQL
        self.db_conn = PostgresConnect().connector()
        self.cursor = self.db_conn.cursor()

        # Connect to Spotify API
        self.sp = self.connect()

        # Initialize database tables
        self.initialize_tables()

    def connect(self):
        """Establish connection to Spotify API"""
        sp = spotipy.Spotify(auth_manager=SpotifyOAuth(client_id=self.client_id,
                                                       client_secret=self.client_secret,
                                                       redirect_uri=self.redirect_uri,
                                                       scope=self.scope))
        return sp
    
    def initialize_tables(self):
        """Create necessary database tables if they don't exist"""
        tables = {
            # Table for user listening history
            'user_listening_history': """
                CREATE TABLE IF NOT EXISTS user_listening_history (
                    id SERIAL PRIMARY KEY,
                    track_id VARCHAR(50) NOT NULL,
                    played_at TIMESTAMP NOT NULL,
                    ms_played INTEGER,
                    context_type VARCHAR(50),
                    context_id VARCHAR(100),
                    UNIQUE(track_id, played_at)
                );
            """,

            # Table for tracks with audio features
            'tracks': """
                CREATE TABLE IF NOT EXISTS tracks (
                    track_id VARCHAR(50) PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    album_id VARCHAR(50) NOT NULL,
                    duration_ms INTEGER,
                    popularity INTEGER,
                    explicit BOOLEAN,
                    track_number INTEGER,
                    is_local BOOLEAN,
                    uri VARCHAR(100),
                    time_signature INTEGER,
                    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP    
                );
            """,

            # Table for artists
            'artists': """
                CREATE TABLE IF NOT EXISTS artists (
                    artist_id VARCHAR(50) PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    popularity INTEGER,
                    followers INTEGER,
                    genres JSONB,
                    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """,

            # Table for albums
            'albums': """
                CREATE TABLE IF NOT EXISTS albums (
                    album_id VARCHAR(50) PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    album_type VARCHAR(50),
                    release_date VARCHAR(50),
                    release_date_precision VARCHAR(20),
                    total_tracks INTEGER,
                    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """,

            # Table for album artists (many-to-many relationship)
            'album_artists': """
                CREATE TABLE IF NOT EXISTS album_artists (
                    album_id VARCHAR(50) NOT NULL,
                    artist_id VARCHAR(50) NOT NULL,
                    PRIMARY KEY (album_id, artist_id)
                );
            """,
            
            # Table for track artists (many-to-many relationship)
            'track_artists': """
                CREATE TABLE IF NOT EXISTS track_artists (
                    track_id VARCHAR(50) NOT NULL,
                    artist_id VARCHAR(50) NOT NULL,
                    position INTEGER,
                    PRIMARY KEY (track_id, artist_id)
                );
            """,
            
            # Table for playlists
            'playlists': """
                CREATE TABLE IF NOT EXISTS playlists (
                    playlist_id VARCHAR(50) PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    description TEXT,
                    owner_id VARCHAR(100),
                    is_public BOOLEAN,
                    is_collaborative BOOLEAN,
                    snapshot_id VARCHAR(100),
                    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """,

            # Table for playlist tracks (many-to-many relationship)
            'playlist_tracks': """
                CREATE TABLE IF NOT EXISTS playlist_tracks (
                    playlist_id VARCHAR(50) NOT NULL,
                    track_id VARCHAR(50) NOT NULL,
                    position INTEGER,
                    added_at TIMESTAMP,
                    added_by VARCHAR(100),
                    PRIMARY KEY (playlist_id, track_id, position)
                );
            """
        }
        
        for table_name, create_query in tables.items():
            self.cursor.execute(create_query)
        
        self.db_conn.commit()


    
    def _get_default_timestamp(self, days=30):
        """
        Returns a default timestamp from specified days ago in Unix milliseconds.
        """
        today = datetime.now()
        previous_date = today - pd.DateOffset(days = days)
        return int(previous_date.timestamp()) * 1000

    def get_recent_listen_timestamp(self, table, timestamp_column):
        """
        Retrieves the most recent timestamp from specified table and column.
        """
        #If the table exists, get the latest timestamp
        query = f"SELECT MAX(played_at) FROM {table};"
        self.cursor.execute(query)
        result = self.cursor.fetchone()

        if result[0] is None:
        # No records in the table yet
            return self._get_default_timestamp()
        else:
            # Convert the timestamp to Unix timestamp in milliseconds
            return int(result[0].timestamp()) * 1000
        
    def get_recently_played(self, limit=50):
        """Get recently played tracks from Spotify"""

        # Get the latest timestamp from the database
        latest_timestamp = self.get_recent_listen_timestamp('user_listening_history', 'played_at')

        # Get recently played tracks from Spotify
        results = self.sp.current_user_recently_played(limit=limit, after=latest_timestamp)

        if not results['items']:
            print("No new tracks played since last fetch.")
            return []
        
        # Process and store the data 
        self.process_recently_played(results['items'])

    def process_recently_played(self, items):
        """Process and store recently played tracks with related entities"""   
        for item in items:
            track = item['track']

        # 1. Store track data
        self.store_track(track)

        # 2. Store album data
        self.store_album(track['album'])

        # 3. Store artists data 
        for artist in track['artists']:
            self.store_artist(artist['id'])

            # Store track-artist relationship
            position = track['artists'].index(artist)
            self.store_track_artist_relation(track['id'], artist['id'], position)
        
         # 4. Store user listening record
            context_type = item.get('context', {}).get('type') if item.get('context') else None
            context_id = item.get('context', {}).get('uri') if item.get('context') else None
            
            self.cursor.execute("""
                INSERT INTO user_listening_history 
                (track_id, played_at, context_type, context_id)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (track_id, played_at) DO NOTHING
            """, (
                track['id'],
                item['played_at'],
                context_type,
                context_id
            ))
        
        self.db_conn.commit()

    def store_track(self, track):
        """Store track data with audio features."""
        try:
            # Check if track already exists
            self.cursor.execute("SELECT 1 FROM tracks WHERE track_id = %s", (track['id'],))
            if self.cursor.fetchone():
                return

            # Insert track data
            self.cursor.execute("""
                INSERT INTO tracks (
                    track_id, name, album_id, duration_ms, popularity, 
                    explicit, track_number, is_local, uri
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (track_id) DO NOTHING
            """, (
                track['id'],
                track['name'],
                track['album']['id'],
                track['duration_ms'],
                track['popularity'],
                track['explicit'],
                track['track_number'],
                track.get('is_local', False),
                track['uri']
            ))
            
            self.db_conn.commit()
        except Exception as e:
            print(f"Error storing track {track.get('id')}: {e}")
            self.db_conn.rollback()
        
    def store_album(self, album):
        """Store album data."""
        try:
            # Check if album already exists
            self.cursor.execute("SELECT 1 FROM albums WHERE album_id = %s", (album['id'],))
            if self.cursor.fetchone():
                return
            
            # Insert album data
            self.cursor.execute("""
                INSERT INTO albums (
                    album_id, name, album_type, release_date, 
                    release_date_precision, total_tracks
                ) VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (album_id) DO NOTHING
            """, (
                album['id'],
                album['name'],
                album['album_type'],
                album['release_date'],
                album['release_date_precision'],
                album.get('total_tracks', 0)
            ))
            
            # Store album-artist relationships
            for artist in album['artists']:
                self.cursor.execute("""
                    INSERT INTO album_artists (album_id, artist_id)
                    VALUES (%s, %s)
                    ON CONFLICT (album_id, artist_id) DO NOTHING
                """, (album['id'], artist['id']))
            
            self.db_conn.commit()
        except Exception as e:
            print(f"Error storing album {album.get('id')}: {e}")
            self.db_conn.rollback()
    
    def store_artist(self, artist_id):
        """Store artist data."""
        try:
            # Check if artist already exists
            self.cursor.execute("SELECT 1 FROM artists WHERE artist_id = %s", (artist_id,))
            if self.cursor.fetchone():
                return
            
            # Fetch full artist data
            artist = self.sp.artist(artist_id)
            
            # Prepare JSON fields
            genres = json.dumps(artist.get('genres', []))
    
            # Insert artist data
            self.cursor.execute("""
                INSERT INTO artists (
                    artist_id, name, popularity, followers, genres
                ) VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (artist_id) DO NOTHING
            """, (
                artist['id'],
                artist['name'],
                artist['popularity'],
                artist['followers']['total'],
                genres
            ))
            
            self.db_conn.commit()
        except Exception as e:
            print(f"Error storing artist {artist_id}: {e}")
            self.db_conn.rollback()
    
    def store_track_artist_relation(self, track_id, artist_id, position):
        """Store track-artist relationships."""
        try:
            self.cursor.execute("""
                INSERT INTO track_artists (track_id, artist_id, position)
                VALUES (%s, %s, %s)
                ON CONFLICT (track_id, artist_id) DO NOTHING
            """, (track_id, artist_id, position))
            
            self.db_conn.commit()
        except Exception as e:
            print(f"Error storing track-artist relation {track_id}-{artist_id}: {e}")
            self.db_conn.rollback()

    def fetch_user_playlists(self):
        """Fetch user's playlists and their tracks."""
        # Get user's playlists
        playlists = []
        results = self.sp.current_user_playlists(limit=50)
        playlists.extend(results['items'])
        
        while results['next']:
            results = self.sp.next(results)
            playlists.extend(results['items'])
        
        for playlist in playlists:
            self.store_playlist(playlist)
            self.fetch_playlist_tracks(playlist['id'])
        
        return playlists
    
    def store_playlist(self, playlist):
        """Store playlist data."""
        try:
            # Get full playlist data if needed
            if 'followers' not in playlist:
                playlist = self.sp.playlist(playlist['id'])
            
            # Insert playlist data
            self.cursor.execute("""
                INSERT INTO playlists (
                    playlist_id, name, description, owner_id,
                    is_public, is_collaborative, followers, snapshot_id
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (playlist_id) 
                DO UPDATE SET 
                    name = EXCLUDED.name,
                    description = EXCLUDED.description,
                    is_public = EXCLUDED.is_public,
                    is_collaborative = EXCLUDED.is_collaborative,
                    followers = EXCLUDED.followers,
                    snapshot_id = EXCLUDED.snapshot_id
            """, (
                playlist['id'],
                playlist['name'],
                playlist.get('description', ''),
                playlist['owner']['id'],
                playlist.get('public', False),
                playlist.get('collaborative', False),
                playlist.get('followers', {}).get('total', 0),
                playlist.get('snapshot_id', '')
            ))
            
            self.db_conn.commit()
        except Exception as e:
            print(f"Error storing playlist {playlist.get('id')}: {e}")
            self.db_conn.rollback()
    
    def fetch_playlist_tracks(self, playlist_id):
        """Fetch and store tracks from a playlist."""
        results = self.sp.playlist_items(playlist_id, limit=100)
        self.process_playlist_tracks(playlist_id, results['items'])
        
        # Handle pagination for large playlists
        while results['next']:
            results = self.sp.next(results)
            self.process_playlist_tracks(playlist_id, results['items'])

    def process_playlist_tracks(self, playlist_id, items):
        """Process and store playlist tracks."""
        for position, item in enumerate(items):
            # Some items might be None if tracks were removed
            if not item or not item.get('track'):
                continue
                
            track = item['track']
            
            # Skip local tracks as they don't have complete info
            if track.get('is_local', False):
                continue
                
            # Store track data
            self.store_track(track)
            
            # Store album
            self.store_album(track['album'])
            
            # Store artists
            for artist in track['artists']:
                self.store_artist(artist['id'])
                self.store_track_artist_relation(track['id'], artist['id'], track['artists'].index(artist))
            
            # Store playlist-track relationship
            try:
                added_at = item.get('added_at')
                added_by = item.get('added_by', {}).get('id') if item.get('added_by') else None
                
                self.cursor.execute("""
                    INSERT INTO playlist_tracks (playlist_id, track_id, position, added_at, added_by)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (playlist_id, track_id, position) 
                    DO UPDATE SET 
                        added_at = EXCLUDED.added_at,
                        added_by = EXCLUDED.added_by
                """, (
                    playlist_id,
                    track['id'],
                    position,
                    added_at,
                    added_by
                ))
                
                self.db_conn.commit()
            except Exception as e:
                print(f"Error storing playlist track relation {playlist_id}-{track.get('id')}: {e}")
                self.db_conn.rollback()
        
    def fetch_top_items(self, item_type='tracks', time_range='short_term', limit=50):
        """Fetch user's top tracks or artists."""
        valid_types = ['tracks', 'artists']
        valid_ranges = ['short_term', 'medium_term', 'long_term']
        
        if item_type not in valid_types:
            raise ValueError(f"Item type must be one of {valid_types}")
        
        if time_range not in valid_ranges:
            raise ValueError(f"Time range must be one of {valid_ranges}")
        
        # Fetch top items
        top_items = self.sp.current_user_top_tracks(limit=limit, time_range=time_range) if item_type == 'tracks' else self.sp.current_user_top_artists(limit=limit, time_range=time_range)
        
        # Process items based on type
        if item_type == 'tracks':
            for track in top_items['items']:
                self.store_track(track)
                self.store_album(track['album'])
                
                for artist in track['artists']:
                    self.store_artist(artist['id'])
                    self.store_track_artist_relation(track['id'], artist['id'], track['artists'].index(artist))
        else:  # artists
            for artist in top_items['items']:
                self.store_artist(artist['id'])
        
        return top_items
    
    def close(self):
        """Close database connection."""
        if self.cursor:
            self.cursor.close()
        if self.db_conn:
            self.db_conn.close()
    

    
def main():
    try:
        # Initialize ETL process
        etl = SpotifyETL()
        
        # 1. Fetch recently played tracks
        print("Fetching recently played tracks...")
        recently_played = etl.get_recently_played(limit=50)
        print(f"Processed {len(recently_played)} recently played tracks")
        
        # 2. Fetch user playlists
        print("Fetching user playlists...")
        playlists = etl.fetch_user_playlists()
        print(f"Processed {len(playlists)} playlists")
        
        # 3. Fetch top tracks and artists for different time ranges
        time_ranges = ['short_term', 'medium_term', 'long_term']  # last 4 weeks, last 6 months, all time
        
        for time_range in time_ranges:
            print(f"Fetching top tracks ({time_range})...")
            top_tracks = etl.fetch_top_items(item_type='tracks', time_range=time_range)
            print(f"Processed {len(top_tracks['items'])} top tracks for {time_range}")
            
            print(f"Fetching top artists ({time_range})...")
            top_artists = etl.fetch_top_items(item_type='artists', time_range=time_range)
            print(f"Processed {len(top_artists['items'])} top artists for {time_range}")
        
        print("ETL process completed successfully!")
    except Exception as e:
        print(f"Error in ETL process: {e}")
        raise
    finally:
        # Ensure database connection is closed
        etl.close()

if __name__ == "__main__":
    main()