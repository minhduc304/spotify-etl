import subprocess
import os
import tempfile
import pandas as pd
from pydub import AudioSegment
import essentia
import essentia.standard as es
from tqdm import tqdm
from youtubesearchpython import VideosSearch
import spotipy
from spotipy.oauth2 import SpotifyOAuth
from dotenv import load_dotenv
import os 

# Setup paths
OUTPUT_FOLDER = "extracted_features"
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

load_dotenv()
sp = spotipy.Spotify(auth_manager=SpotifyOAuth(client_id=os.getenv("SPOTIFY_CLIENT_ID"),
                                                       client_secret=os.getenv("SPOTIFY_CLIENT_SECRET"),
                                                       redirect_uri=os.getenv("SPOTIFY_REDIRECT_URI"),
                                                       scope=os.getenv("SPOTIFY_SCOPE")))


def search_youtube(song_title, artist_name):
    query = f"{song_title} {artist_name} audio"
    search = VideosSearch(query, limit=1)
    result = search.result()
    try:
        video_id = result['result'][0]['id']
        url = f"https://www.youtube.com/watch?v={video_id}"
        return url
    except IndexError:
        print(f"No YouTube result for: {query}")
        return None

def download_youtube_audio(youtube_url, output_dir):
    output_template = os.path.join(output_dir, "%(id)s.%(ext)s")
    cmd = [
        'yt-dlp',
        '-x', '--audio-format', 'mp3',
        '--output', output_template,
        youtube_url,
        '--quiet'
    ]
    try:
        subprocess.run(cmd, check=True)
        video_id = youtube_url.split('v=')[1]
        downloaded_path = os.path.join(output_dir, f"{video_id}.mp3")
        return downloaded_path
    except subprocess.CalledProcessError:
        print(f"Failed to download {youtube_url}")
        return None

def clip_audio(input_path, output_path, duration_ms=30000):
    try:
        audio = AudioSegment.from_file(input_path)
        clip = audio[:duration_ms]
        clip.export(output_path, format="mp3")
        return True
    except Exception as e:
        print(f"Error clipping {input_path}: {e}")
        return False

def extract_essentia_features(audio_path):
    loader = es.MonoLoader(filename=audio_path)
    audio = loader()

    mfcc = es.MFCC()
    bands, mfcc_coeffs = mfcc(audio)

    return mfcc_coeffs.mean(axis=0)

def process_track(song_title, artist_name, tmpdir):
    youtube_url = search_youtube(song_title, artist_name)
    if not youtube_url:
        return None

    full_audio_path = download_youtube_audio(youtube_url, tmpdir)
    if not full_audio_path:
        return None

    clipped_audio_path = os.path.join(tmpdir, "clipped.mp3")
    success = clip_audio(full_audio_path, clipped_audio_path)
    if not success:
        return None

    features = extract_essentia_features(clipped_audio_path)
    return features.tolist()

def remove_duplication(songs_df):
    unique_songs = []
    seen = set()

    for song in songs_df:
        song_tuple = (song["song_title"], song["artist_name"])
        if song_tuple not in seen:
            unique_songs.append(song)
            seen.add(song_tuple)
    
    return unique_songs

def process_batch(songs_df):
    results = []

    with tempfile.TemporaryDirectory() as tmpdir:
        print(f"Temporary audio folder: {tmpdir}")

        for i, row in tqdm(songs_df.iterrows(), total=len(songs_df), desc="Processing batch"):
            song_title = row['song_title']
            artist_name = row['artist_name']

            features = process_track(song_title, artist_name, tmpdir)
            if features:
                results.append({
                    "song_title": song_title,
                    "artist_name": artist_name,
                    "features": features
                })
            else:
                results.append({
                    "song_title": song_title,
                    "artist_name": artist_name,
                    "features": None
                })

        # After batch: copy features to output folder
        output_csv = os.path.join(OUTPUT_FOLDER, "features.csv")
        df = pd.DataFrame(results)
        df.to_csv(output_csv, index=False)
        print(f"Saved extracted features to {output_csv}")


if __name__ == "__main__":
    tracks = sp.current_user_recently_played()['items']
    songs_data = []
    for track in tracks:
        songs_data.append({"song_title" : track['track']['name'],
                           "artist_name" : track['track']['artists'][0]['name']})
    
    
    songs_df = pd.DataFrame(remove_duplication(songs_data))

    process_batch(songs_df)
