import spotipy
from spotipy.oauth2 import SpotifyOAuth
from dotenv import load_dotenv
import os
import csv 
from datetime import datetime

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

def get_recently_played():
    results = sp.current_user_recently_played(limit=50)
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

def save_to_csv(tracks):
    datetime_str = datetime.now().strftime("%Y-%m-%d")
    filename = f"recently_played_{datetime_str}.csv"
    os.makedirs('data', exist_ok=True)

    with open(filename, 'a', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=tracks[0].keys())
        if os.path.getsize(filename) == 0:
            writer.writeheader()
            writer.writerows(tracks)
    
if __name__ == "__main__":
    tracks = get_recently_played()
    save_to_csv(tracks)
    print(f"Saved {len(tracks)} recently played tracks to CSV.")
