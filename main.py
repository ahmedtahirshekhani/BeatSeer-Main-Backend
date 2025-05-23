import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from tabulate import tabulate
import requests
import pandas as pd
from bs4 import BeautifulSoup
from io import StringIO
from datetime import datetime
import numpy as np
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()
import anthropic
import json
import musicbrainzngs
# Set up MusicBrainz API
from pymongo import MongoClient
import  datetime 
from datetime import datetime, timedelta
musicbrainzngs.set_useragent("BeetSeer_AI_Backend", "1.0", "ahmedtahir.developer@gmail.com")
import re
import time
import platform
from requests.adapters import HTTPAdapter
import urllib3
from urllib3.util import connection
import socket
from urllib.parse import quote
def allowed_gai_family():
    return socket.AF_INET  # forces use of IPv4

# Patch the connection module globally
connection.allowed_gai_family = allowed_gai_family
MONGODB_URI=os.getenv("MONGODB_URI")

if platform.system() == 'Linux':
    socket_options = [
        (socket.SOL_SOCKET, socket.SO_REUSEADDR, 1),
        (socket.SOL_SOCKET, socket.SO_BINDTODEVICE, "eth0".encode()),
        (socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    ]
else:  # Windows/Mac
    socket_options = [
        (socket.SOL_SOCKET, socket.SO_REUSEADDR, 1),
        (socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    ]

def get_artist_countries(artist_names):
    # print(artist_names)
    client = MongoClient(MONGODB_URI)
    db = client["musictrend"] 
    collection = db["artist_countries"]
    artist_countries = {}

    existing_data = {doc["_id"]: doc["country"] for doc in collection.find({"_id": {"$in": list(artist_names)}})}
    missing_artists = [artist for artist in list(artist_names) if artist not in list(existing_data.keys())]


    for artist in missing_artists:
        try:
            result = musicbrainzngs.search_artists(artist=artist, limit=1)
            if result.get('artist-list'):
                fetched_name = result['artist-list'][0]['name']
                fetched_country = result['artist-list'][0].get('country', 'Unknown')

                # Compare first word of both names (case-insensitive)
                if artist.split()[0].lower() == fetched_name.split()[0].lower():
                    country = fetched_country
                else:
                    country = 'Unknown'
            else:
                country = 'Unknown'

            artist_countries[artist] = country
        except Exception as e:
            print(f"Error fetching country for {artist}: {e}")
            artist_countries[artist] = 'Unknown'

    if artist_countries:
        collection.insert_many([{"_id": artist, "country": country} for artist, country in artist_countries.items()])
    client.close()
    sorted_countries = {artist: (existing_data.get(artist) or artist_countries.get(artist, 'Unknown')) for artist in artist_names}
    return sorted_countries




app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"], 
    allow_headers=["*"],
)

# Example endpoint
@app.get("/")
def read_root():
    return {"message": "Hello, FastAPI!"}


def get_date_range():


    def last_friday():
        today = datetime.today()
        days_since_friday = (today.weekday() - 4) % 7  # 4 represents Friday
        last_friday_date = today - timedelta(days=days_since_friday if days_since_friday else 7)
        return last_friday_date

    start7days = pd.to_datetime(last_friday()) - timedelta(days=7)

    date_range = start7days.strftime('%d%b%Y').lower() + '-' + last_friday().strftime('%d%b%Y').lower()
    formatted_date_range = re.sub(r'\b0(\d)', r'\1', date_range)
    return formatted_date_range

BASE_URL = "https://www.last.fm"
def get_encoded_artist_url(artist_name):
    """Properly encode artist names for URLs"""
    return f"{BASE_URL}/music/{quote(artist_name)}"

def create_lastfm_session():
    """Create a session with IPv4 enforcement and proper headers"""
    session = requests.Session()
    
    # Force IPv4 (platform-independent approach)
    class ForceIPv4HTTPAdapter(HTTPAdapter):
        def init_poolmanager(self, *args, **kwargs):
            kwargs['socket_options'] = [
                (socket.SOL_SOCKET, socket.SO_REUSEADDR, 1),
                (socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            ]
            # Force IPv4 by specifying family
            kwargs['source_address'] = ('0.0.0.0', 0)
            super().init_poolmanager(*args, **kwargs)
    
    # Mount the adapter for both http and https
    session.mount('http://', ForceIPv4HTTPAdapter())
    session.mount('https://', ForceIPv4HTTPAdapter())
    
    # Set headers to mimic browser behavior
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"
    })
    
    return session

def safe_request(session, url, max_retries=3):
    """Handle requests with retries and delays"""
    for attempt in range(max_retries):
        try:
            response = session.get(url)
            if response.status_code == 200:
                return response
            elif response.status_code == 403:
                print(f"Blocked on attempt {attempt + 1} for {url}")
                time.sleep(5 * (attempt + 1))  # Exponential backoff
            else:
                print(f"Unexpected status {response.status_code} for {url}")
                return None
        except Exception as e:
            print(f"Request failed (attempt {attempt + 1}): {str(e)}")
            time.sleep(2)
    return None

def collect_lastfm():
    print("Collecting LastFM data")
    session = create_lastfm_session()
    
    try:
        # Get date range from first page
        url = f"{BASE_URL}/charts/weekly?page=1"
        response = safe_request(session, url)
        if not response:
            print("Failed to fetch initial page")
            return

        soup = BeautifulSoup(response.text, 'html.parser')
        date_range = get_date_range()  # Your existing fallback function
        
        if soup.find("h3"):
            try:
                date_text = soup.find("h3").get_text(strip=True)
                date_range = date_text.replace(" ", "").replace("—", "-").lower()
                date_range = re.sub(r'\b0(\d)', r'\1', date_range)
            except Exception as e:
                print(f"Error parsing date: {str(e)}")

        client = MongoClient(MONGODB_URI)
        db = client.musictrend
        collection = db.lastfm_top200
        
        if not collection.find_one({'date_range': date_range}):
            dfs_list = []
            for i in range(1, 5):  # Pages 1-4
                url = f"{BASE_URL}/charts/weekly?page={i}"
                response = safe_request(session, url)
                if not response:
                    continue
                
                try:
                    tables = pd.read_html(response.text)
                    chart_table = tables[0]
                    chart_table["artist_name"] = chart_table["Artist.1"].str.extract(r'^(.*?)(?=\s\d)')
                    main_df = chart_table[['artist_name', 'Listeners', 'Scrobbles']]
                    main_df.reset_index(drop=True, inplace=True)
                    main_df = main_df.copy()
                    main_df.loc[:, 'rank'] = main_df.index + 1 + (i-1) * 50
                    main_df.rename(columns={
                        'Listeners': 'listeners', 
                        'Scrobbles': 'scrobbles'
                    }, inplace=True)
                    dfs_list.append(main_df)
                    time.sleep(2)  # Respectful delay
                except Exception as e:
                    print(f"Error processing page {i}: {str(e)}")
            
            if dfs_list:
                main_df = pd.concat(dfs_list).reset_index(drop=True)
                collection.insert_one({
                    'count': collection.count_documents({}) + 1,
                    'data': main_df.to_dict(orient='records'),
                    'date_range': date_range
                })
        
    finally:
        client.close()
        print("LastFM data collection completed")



def get_lastfm_data():
    print("Getting LastFM data")
    collect_lastfm()
    client = MongoClient(MONGODB_URI)
    db = client.musictrend
    collection = db.lastfm_top200
    data = collection.find().sort('_id', -1).limit(2)
    df1 = pd.DataFrame(data[0]['data'])
    df2 = pd.DataFrame(data[1]['data'])
    
    client.close()
    combined_df = pd.merge(df1, df2, on='artist_name', suffixes=('_1', '_2'), how='inner')
    combined_df['change_listeners'] = ((combined_df['listeners_1'] - combined_df['listeners_2'])/combined_df['listeners_2']) * 100
    combined_df['change_scrobbles'] = ((combined_df['scrobbles_1'] - combined_df['scrobbles_2'])/combined_df['scrobbles_2']) * 100
    combined_df['change_perc'] = (combined_df['change_listeners'] + combined_df['change_scrobbles']) / 2
    combined_df['country'] = get_artist_countries(combined_df['artist_name']).values()
    combined_df = combined_df[combined_df['country'].isin(['US', 'CA', 'MX', 'GB', 'FR', 'DE', 'IT', 'ES', 'NL', 'BE', 'CH', 'AT', 'SE', 'NO', 'DK', 'FI', 'IE', 'PT', 'LU', 'IS'])]


    print("LastFM data fetched")
    return combined_df


def fetch_info(html):


    soup = BeautifulSoup(html, 'html.parser')

    artist_name = soup.find('h1', class_='header-new-title').text.strip()

    album_section = soup.find('li', {'itemtype': 'http://schema.org/MusicAlbum'})
    album_name = album_section.find('h3').text.strip() if album_section else None
    release_date = album_section.find('p', class_='artist-header-featured-items-item-date').text.strip() if album_section else None

    listeners_tag = soup.find('li', class_='header-metadata-tnew-item')
    scrobbles_tag = soup.find_all('li', class_='header-metadata-tnew-item')[1]
    artist_listeners = listeners_tag.find('p').text.strip() if listeners_tag else None
    artist_scrobbles = scrobbles_tag.find('p').text.strip() if scrobbles_tag else None

    # Convert listeners (handle K and M)
    def convert_to_number(value):
        if value:
            value = value.replace(',', '')  # Remove commas
            if 'K' in value:
                return float(value.replace('K', '')) * 1000
            elif 'M' in value:
                return float(value.replace('M', '')) * 1_000_000
            else:
                return int(value)  # Convert plain numbers
        return None

    artist_listeners = convert_to_number(artist_listeners)
    artist_scrobbles = convert_to_number(artist_scrobbles)


    div = soup.find('div', class_='header-new-background-image')
    if div:
        style = div.get('style', '')
        artist_image_url = style.split('url(')[-1].split(')')[0] if 'url(' in style else None
    else:
        artist_image_url = soup.find('img', {'alt': album_name})['src'] if album_name else None


    track_section = soup.find('li', {'itemtype': 'http://schema.org/MusicRecording'})
    track_image_url = track_section.find('img')['src'] if track_section else None

    # Extract genre from "Related Tags"
    tags_section = soup.find('ul', class_='tags-list')
    genre = ", ".join([tag.text for tag in tags_section.find_all('a')]) if tags_section else "Unknown"

    # Store the extracted data in a DataFrame
    df = {
        'artist': artist_name,
        'album': album_name,
        'release_date': release_date,
        'artist_id': artist_name.lower().replace(' ', '_'),  # Creating an ID
        'album_type': 'Album',
        'artist_followers': artist_listeners,
        'artist_image_url': artist_image_url,
        'track_image_url': track_image_url,
        'genre': genre,
        'followers': artist_listeners
    }

    return df



def get_lastfm_new_art():
    session = create_lastfm_session()
    dfs = []

    for i in range(1, 4):  # Pages 1-3
        url = f"{BASE_URL}/tag/new/artists?page={i}"
        response = safe_request(session, url)
        if not response:
            continue

        soup = BeautifulSoup(response.text, "html.parser")
        artists = []
        
        for artist_section in soup.find_all("li", class_="big-artist-list-wrap", itemscope=True):
            artist_name_elem = artist_section.find("h3", class_="big-artist-list-title")
            if not artist_name_elem:
                continue
                
            artist_name = artist_name_elem.get_text(strip=True)
            artist_url = get_encoded_artist_url(artist_name)
            
            artist_response = safe_request(session, artist_url)
            if artist_response:
                artist_det = fetch_info(artist_response.text)
                if artist_det:
                    artists.append(artist_det)
            time.sleep(1)  # Delay between artist requests
        
        if artists:
            dfs.append(pd.DataFrame(artists))
        time.sleep(3)  # Delay between page requests

    if not dfs:
        return pd.DataFrame()

    df = pd.concat(dfs).reset_index(drop=True)
    df['artist'] = df['artist'].str.lower().str.strip()
    df = df.dropna(subset=['artist']).drop_duplicates(subset=['artist']).reset_index(drop=True)
    
    if not df.empty:
        df['popularity'] = (df['artist_followers'] / df['artist_followers'].sum() * 100).round(2)
        df['country'] = list(get_artist_countries(df['artist']).values())
        df = df[df['country'].isin([
            'US', 'CA', 'MX', 'GB', 'FR', 'DE', 'IT', 'ES', 'NL',
            'BE', 'CH', 'AT', 'SE', 'NO', 'DK', 'FI', 'IE', 'PT', 'LU', 'IS'
        ])]

    return df

def fetch_or_update_kworb_data(date_range, category='global_daily'):
    client = MongoClient(MONGODB_URI)
    db = client["musictrend"]
    collection = db["kworb_spotify"]
    key_name_db = f"{date_range}_{category or 'daily'}"
    print(f'Fetching data from MongoDB, key_name_db: {key_name_db}')
    existing_data = collection.find_one({"_id": key_name_db})

    if existing_data:
        return existing_data["data"]

    df = get_kworb_spotify_data(category)
    collection.insert_one({"_id": key_name_db, "data": df.to_dict("records")})
    client.close()
    return df

def get_kworb_spotify_data(country='global_daily'):
        

        url = f'https://kworb.net/spotify/country/{country}.html'
        print("Fetching data from", url)
        response = requests.get(url)
        response.encoding = 'utf-8'
        html_content = StringIO(response.text)
        

        tables = pd.read_html(html_content)

        df = tables[0]

        artist_ids = []
        track_ids = []

        soup = BeautifulSoup(response.text, 'html.parser')

        for td in soup.find_all('td', class_='text mp'):
            links = td.find_all('a')
            if len(links) > 0:
                artist_href = links[0]['href']
                track_href = links[1]['href']

                # Extract the artist_id and track_id (they are part of the href)
                artist_id = artist_href.split('/')[-1].replace('.html', '')  # Extracts the artist ID from the URL
                track_id = track_href.split('/')[-1].replace('.html', '')  # Extracts the track ID from the URL

                artist_ids.append(artist_id)
                track_ids.append(track_id)
            else:
                artist_ids.append(None)
                track_ids.append(None)
        

        # Add the extracted IDs as new columns in the DataFrame
        df_f = pd.DataFrame()
        df_f['artist_id'] = artist_ids
        df_f['track_id'] = track_ids
        df_f['artist'] = df['Artist and Title'].apply(lambda x: x.split(' - ')[0])
        df_f['title'] = df['Artist and Title'].apply(lambda x: x.split(' - ')[1])
        if country == 'global_daily':
            df_f['trending_percent'] = df['7Day+'] / df['7Day']
            df_f['Streams'] = df['7Day']
            df_f['Streams+'] = df['7Day+']
        else:
            df_f['trending_percent'] = df['Streams+'] / df['Streams']
            df_f['Streams'] = df['Streams']
            df_f['Streams+'] = df['Streams+']

        df_f['P+'] = df['P+']
        df_f.fillna({'artist_id': ''}, inplace=True)
        df_f.fillna({'track_id': ''}, inplace=True)
        df_f.fillna({'trending_percent': 1}, inplace=True)
        # drop duplicates artist
        df_f.drop_duplicates(subset=['artist'], inplace=True)
        df_f['artist'] = df_f['artist'].str.lower().str.strip()
        df_f['country'] = get_artist_countries(df_f['artist']).values()
        df_filtered = df_f[df_f['country'].isin(['US', 'CA', 'MX', 'GB', 'FR', 'DE', 'IT', 'ES', 'NL', 'BE', 'CH', 'AT', 'SE', 'NO', 'DK', 'FI', 'IE', 'PT', 'LU', 'IS'])]

        return df_filtered


def get_artists(art_ids, sp):
    try:
        return sp.artists(art_ids)["artists"]
    except Exception as e:
        print(f"Spotify API error: {e}")
        return []


def get_artist_genres(artist_ids, sp):
    client = MongoClient(MONGODB_URI)
    db = client["musictrend"]
    collection = db["artist_genres"]
    artist_genres = {}
    existing_data = {doc["_id"]: doc["genre"] for doc in collection.find({"_id": {"$in": artist_ids}})}
    missing_artists = [artist for artist in artist_ids if artist not in existing_data]

    if missing_artists:
        for i in range(0, len(missing_artists), 50):
            batch_ids = missing_artists[i:i+50]
            artists_data = get_artists(batch_ids, sp)
            
            for art in artists_data:
                artist_id = art["id"]
                genre = art["genres"][0] if art.get("genres") else ""
                artist_genres[artist_id] = genre
                collection.insert_one({"_id": artist_id, "genre": genre})

            time.sleep(1)

    return {**existing_data, **artist_genres}


# Define another route with path parameters
@app.get("/news-letter")
def get_newsletter_data(
    youTubeApiKey: str = Query(...),
    spotify_CLIENT_ID: str = Query(...),
    spotify_CLIENT_SECRET: str = Query(...)
    ):
    print("Getting newsletter data")
    if youTubeApiKey and spotify_CLIENT_ID and spotify_CLIENT_SECRET:
        CLIENT_ID = spotify_CLIENT_ID
        CLIENT_SECRET = spotify_CLIENT_SECRET
        # Authenticate with Spotify API
        auth_manager = SpotifyClientCredentials(client_id=CLIENT_ID, client_secret=CLIENT_SECRET)
        sp = spotipy.Spotify(auth_manager=auth_manager)

        print("Spotify authenticated")
        date_range = get_date_range()
        df = fetch_or_update_kworb_data(date_range)  
        df2 = fetch_or_update_kworb_data(date_range, 'global_weekly')
        print("Spotify data fetched")
        print(df, df2)
        df = pd.DataFrame(df)
        df2 = pd.DataFrame(df2)
        
        df = pd.concat([df, df2])
        df.drop_duplicates(subset=['artist', 'title'], inplace=True)
        
        df.sort_values('trending_percent', ascending=False, inplace=True)
        df.drop_duplicates(subset=['artist_id'], inplace=True)
        df = df.reset_index(drop=True)
        print("Start LastFM data")
        lastfm_data = get_lastfm_data()
        lastfm_data = lastfm_data.dropna(subset=['artist_name'])
        lastfm_data['artist_name'] = lastfm_data['artist_name'].str.lower()
        lastfm_data['artist_name'] = lastfm_data['artist_name'].str.strip()
        df = df.dropna(subset=['artist'])
        df['artist'] = df['artist'].str.lower()
        df['artist'] = df['artist'].str.strip()
        print(lastfm_data['country'], df['country'])

        combine_spotify_lastfm_df = pd.merge(lastfm_data, df, left_on='artist_name', right_on='artist', how='inner')
        combine_spotify_lastfm_df['trending_percent'] = (combine_spotify_lastfm_df['trending_percent'] + combine_spotify_lastfm_df['change_perc']) / 2
        
        df = combine_spotify_lastfm_df.copy()
        print(df[df['artist'].str.contains('Newjeans', case=False, na=False)])
    
        
     


        def get_artists(art_ids):
            data = sp.artists(art_ids)
            return data
        

        for i in range(0, df.shape[0], 50):
            art_ids = df.iloc[i:i+50]['artist_id'].tolist()
          
            arts_genre = get_artist_genres(art_ids, sp)
            print("arts_genre", arts_genre)
            artist_ids = list(arts_genre.keys()) 
            genres = [arts_genre.get(artist_id, 'classic') for artist_id in artist_ids]   
            genres = ['classic' if genre == '' else genre for genre in genres]
            actual_len = len(df.iloc[i:i+50])         
            df.loc[i:i+actual_len-1, 'genre'] = genres[:actual_len]
        
        print("Spotify data", df['genre'])
        

        dead_artists = [
            "Elvis Presley", "John Lennon", "Jimi Hendrix", "Jim Morrison", "Kurt Cobain", 
            "David Bowie", "Freddie Mercury", "George Harrison", "Amy Winehouse", "Prince", 
            "Whitney Houston", "Michael Jackson", "Janis Joplin", "Bob Marley", "Tupac Shakur", 
            "Biggie Smalls (The Notorious B.I.G.)", "Buddy Holly", "Ritchie Valens", "Syd Barrett", 
            "Keith Moon", "Billie Holiday", "Louis Armstrong", "Muddy Waters", "John Coltrane", 
            "Charlie Parker", "Bessie Smith", "Dizzy Gillespie", "Thelonious Monk", "Etta James", 
            "Miles Davis", "Aretha Franklin", "Marvin Gaye", "Sam Cooke", "Otis Redding", 
            "Tina Turner", "Donny Hathaway", "Al Green", "Johnny Cash", "Patsy Cline", 
            "Hank Williams", "Merle Haggard", "Waylon Jennings", "George Jones", "Kenny Rogers", 
            "Ludwig van Beethoven", "Wolfgang Amadeus Mozart", "Johann Sebastian Bach", 
            "Frédéric Chopin", "Pyotr Ilyich Tchaikovsky", "Sid Vicious", "Joey Ramone", 
            "Joe Strummer", "Lemmy Kilmister", "XXXTentacion", "Mac Miller", "Lil Peep", 
            "Aaliyah", "Big L", "Vangelis", "Glenn Gould", "Leonard Cohen", "Paco de Lucía", 
            "Celia Cruz"
        ]
        dead_artists = [artist.lower() for artist in dead_artists]
        df_obs = df[~df['artist'].isin(dead_artists)]
        print(df[df['artist'].str.contains('Newjeans', case=False, na=False)])

        df_obs = df_obs[df_obs['country_y'].isin(['US', 'CA', 'MX', 'GB', 'FR', 'DE', 'IT', 'ES', 'NL', 'BE', 'CH', 'AT', 'SE', 'NO', 'DK', 'FI', 'IE', 'PT', 'LU', 'IS'])]
        df_obs = df_obs[(df_obs['trending_percent'] != 1) & (df_obs['trending_percent'] != -1)]
        grouped = df_obs.groupby('genre')['trending_percent'].mean()
        grouped = grouped.reset_index()
        grouped = grouped.sort_values('trending_percent', ascending=False)
        top5_upward_genres = grouped.head(5)
        top5_downward_genres = grouped.tail(5).sort_values('trending_percent', ascending=True)

        # group by artist and avg trending_percent and get top 5 artist with upword trend and top 5 artist with downward trend
        # ignore trending_percent with 1 or -1
        df_obs = df_obs[(df_obs['trending_percent'] != 1) & (df_obs['trending_percent'] != -1)]
        grouped = df_obs.groupby('artist')['trending_percent'].mean()
        grouped = grouped.reset_index()
        grouped = grouped.sort_values('trending_percent', ascending=False)
        top5_upward_artists = grouped.head(5)
        top5_downward_artists = grouped.tail(5).sort_values('trending_percent', ascending=True)
    
        new_release = sp.search(q='tag:new', type='album', limit=50)
        new_release2 = sp.search(q='tag:new', type='album', limit=50, offset=50)
        new_release = new_release['albums']['items'] + new_release2['albums']['items']

        new_album_artist = pd.DataFrame()

        for album in new_release:
            artist_name = album['artists'][0]['name']
            artist_id = album['artists'][0]['id']
            album_type = album['album_type']
            album_name = album['name']
            release_date = album['release_date']
            artist_image_url = None
            genre = None

            # Fetch artist details for image, followers, and genre
            artist_details = sp.artist(artist_id)
            if 'images' in artist_details and len(artist_details['images']) > 0:
                artist_image_url = artist_details['images'][0]['url']

            artist_followers = artist_details['followers']['total']
            genre = ', '.join(artist_details.get('genres', []))  # Join genres into a single string

            track_image_url = None
            if 'images' in album and len(album['images']) > 0:
                track_image_url = album['images'][0]['url']

            # Create a DataFrame row
            df = pd.DataFrame({
                'artist': [artist_name],
                'album': [album_name],
                'release_date': [release_date],
                'artist_id': [artist_id],
                'album_type': [album_type],
                'artist_followers': [artist_followers],
                'artist_image_url': [artist_image_url],
                'track_image_url': [track_image_url],
                'genre': [genre]  # Add genre here
            })

            new_album_artist = pd.concat([new_album_artist, df])
        # drop duplicates artist
        new_album_artist.drop_duplicates(subset=['artist'], inplace=True)
        new_album_artist = new_album_artist.reset_index(drop=True)
        new_album_artist['artist'] = new_album_artist['artist'].str.lower().str.strip()
        new_album_artist['country'] = get_artist_countries(new_album_artist['artist']).values()
        new_album_artist = new_album_artist[new_album_artist['country'].isin(['US', 'CA', 'MX', 'GB', 'FR', 'DE', 'IT', 'ES', 'NL', 'BE', 'CH', 'AT', 'SE', 'NO', 'DK', 'FI', 'IE', 'PT', 'LU', 'IS'])]

        new_album_artist = new_album_artist.reset_index(drop=True)

        # Enrich data with followers and popularity in batches
        for i in range(0, new_album_artist.shape[0], 50):
            art_ids = new_album_artist.loc[i:i + 49, 'artist_id'].tolist()
            arts = get_artists(art_ids)
            arts_list = arts['artists']
            followers = [art['followers']['total'] for art in arts_list]
            popularities = [art['popularity'] for art in arts_list]
            genres = [', '.join(art.get('genres', [])) for art in arts_list]
            new_album_artist.loc[i:i + 49, ['followers', 'popularity', 'genre']] = list(zip(followers, popularities, genres))

        # Extract the top 5 emerging artists with all requested fields
        filtered_album_artist = new_album_artist[new_album_artist['album_type'] == 'single']
        lastfm_new_art = get_lastfm_new_art()
        print(lastfm_new_art[lastfm_new_art['artist'].str.contains('Newjeans', case=False, na=False)])
        
        filtered_album_artist = pd.concat([filtered_album_artist, lastfm_new_art])
        filtered_album_artist.drop_duplicates(subset=['artist'], inplace=True)
        filtered_album_artist = filtered_album_artist.reset_index(drop=True)
        filtered_album_artist['followers'] = filtered_album_artist['followers'].astype(int)
        filtered_album_artist = filtered_album_artist[filtered_album_artist['followers'] > 10000]
        filtered_album_artist = filtered_album_artist[filtered_album_artist['genre'] != 'Unknown']
        print(filtered_album_artist[filtered_album_artist['artist'].str.contains('Newjeans', case=False, na=False)])



        top_5_emerging_artists = filtered_album_artist.sort_values('popularity', ascending=True).head(10).reset_index(drop=True)
        print("top_5_emerging_artists", top_5_emerging_artists)
        

        top5_upward_genres.loc[:, 'trending_percent'] = (top5_upward_genres['trending_percent']*100).round(1)
        top5_downward_genres.loc[:, 'trending_percent'] = (top5_downward_genres['trending_percent']*100).round(1)
        top5_upward_artists.loc[:, 'trending_percent'] = (top5_upward_artists['trending_percent']*100).round(1)
        top5_downward_artists.loc[:, 'trending_percent'] = (top5_downward_artists['trending_percent']*100).round(1)

        API_KEY = youTubeApiKey
        print("API_KEY", API_KEY)
        def update_df_with_yt_data(df_yt_data):
            

            # Function to search for the channel IDs using channel names
            def get_channel_ids(channel_names, most_popular_tracks, df_yt_data):
                channel_ids = []
                related_tracks = []
                search_url = "https://www.googleapis.com/youtube/v3/search"
                
                for channel_name, track_name in zip(channel_names, most_popular_tracks):
                    # Combine channel name and most popular track for the search query
                    search_query = f"{channel_name} music official"
                    search_params = {
                        'part':'snippet',
                        'q': search_query,
                        'type': 'channel',
                        'key': API_KEY,
                        'regionCode': 'US',
                        'maxResults': 5
                    }
                    try:
                        response = requests.get(search_url, params=search_params)
                        response.raise_for_status()  # Raise exception for HTTP errors
                        data = response.json()
                        
                        if data.get('items'):
                            channel_id = data['items'][0]['snippet']['channelId']
                            channel_ids.append(channel_id)
                            related_tracks.append(track_name)  # Maintain track-channel mapping

                            # Update DataFrame properly using .loc[]
                            df_yt_data.loc[df_yt_data['artist'] == channel_name, 'channel_id'] = channel_id
                        else:
                            print(f"No channel found for {channel_name}")
                            
                            # Drop artist rows correctly
                            df_yt_data.drop(df_yt_data[df_yt_data['artist'] == channel_name].index, inplace=True)

                    except requests.exceptions.RequestException as e:
                        print(f"Error fetching search results for {channel_name}: {e}")

                # Ensure both lists are aligned and limit to 5 entries
                channel_ids = channel_ids[:5]
                popular_tracks = related_tracks[:5]
                df_yt_data = df_yt_data.head(5)  # Limit DataFrame to first 5 entries

                # Return only the first 5 channel IDs and related tracks
                return channel_ids, popular_tracks, df_yt_data  # Return updated DataFrame

            # Function to fetch channel data for multiple channel IDs
            def get_channels_data(channel_ids, df_yt_data):
                channel_url = "https://www.googleapis.com/youtube/v3/channels"
                channels_info = []
                all_channel_names = []
                all_channel_ids = []

                # Maintain original order of channel_ids
                channel_order_map = {channel_id: i for i, channel_id in enumerate(channel_ids)}
                channel_data_dict = {}  # Dictionary to store channel data temporarily

                for i in range(0, len(channel_ids), 50):  # Batch API calls in groups of 50
                    batch_ids = channel_ids[i:i + 50]
                    channel_params = {
                        'part': 'snippet,contentDetails,statistics',
                        'id': ','.join(batch_ids),
                        'key': API_KEY
                    }
                    try:
                        response = requests.get(channel_url, params=channel_params)
                        response.raise_for_status()
                        data = response.json()

                        for channel in data.get('items', []):
                            channel_id = channel['id']
                            channel_name = channel['snippet']['title']
                            channel_image_url = channel['snippet']['thumbnails']['default']['url']
                            subscriber_count = channel['statistics'].get('subscriberCount', 0)
                            video_count = channel['statistics'].get('videoCount', 0)
                            view_count = channel['statistics'].get('viewCount', 0)

                            # Store in dictionary to maintain order
                            channel_data_dict[channel_id] = {
                                "channel_name": channel_name,
                                "channel_image_url": channel_image_url,
                                "subscribers": subscriber_count,
                                "videos": video_count,
                                "views": view_count
                            }

                    except requests.exceptions.RequestException as e:
                        print(f"Error fetching channel data: {e}")

                # Sort data according to original `channel_ids` order
                sorted_channel_data = sorted(channel_data_dict.items(), key=lambda x: channel_order_map[x[0]])

                for channel_id, channel_info in sorted_channel_data:
                    channels_info.append(channel_info)
                    all_channel_ids.append(channel_id)
                    all_channel_names.append(channel_info["channel_name"])

                    # Update DataFrame in bulk
                    df_yt_data.loc[df_yt_data['channel_id'] == channel_id, ['channel_name', 'channel_image_url', 'subscribers', 'videos', 'views']] = [
                        channel_info["channel_name"], channel_info["channel_image_url"], channel_info["subscribers"], channel_info["videos"], channel_info["views"]
                    ]

                # print("all_channel_ids: ", all_channel_ids)
                # print("all_channel_names: ", all_channel_names)
                return df_yt_data, all_channel_names, all_channel_ids
            
            # Function to fetch video data for the most popular track
            def get_video_data(channel_titles, most_popular_tracks, df_yt_data):
                # print("get_video_data")
                search_url = "https://www.googleapis.com/youtube/v3/search"
                for channel_name, track_name in zip(channel_titles, most_popular_tracks):
                    # Combine channel name and most popular track for the search query
                    search_query = f"{track_name} {channel_name} song official"
                    search_params = {
                        'part': 'snippet',
                        'q': search_query,  # Search for the most popular track name
                        # 'type': 'video',  # Only search for videos
                        'key': API_KEY
                    }
                    try:
                        response = requests.get(search_url, params=search_params)
                        response.raise_for_status()
                        data = response.json()
                        # print(" video Data: ", data['items'])

                        if data.get('items'):
                            item_id = data['items'][0]['id']
                            video_id = item_id.get('videoId') or item_id.get('playlistId')
                            video_title = data['items'][0]['snippet']['title']

                            # Get video statistics
                            video_url = "https://www.googleapis.com/youtube/v3/videos"
                            video_params = {
                                'part': 'statistics',
                                'id': video_id,
                                'key': API_KEY
                            }
                            video_response = requests.get(video_url, params=video_params)
                            video_response.raise_for_status()
                            video_data = video_response.json()
                            # print("video views : ", video_data)
                            video_views = video_data['items'][0]['statistics'].get('viewCount', 0)

                            # Update DataFrame
                            df_yt_data.loc[df_yt_data['album'] == track_name, ['video_id', 'video_title', 'video_views']] = [
                                video_id, video_title, video_views
                            ]
                        else:
                            print(f"No video found for {track_name}")
                    except requests.exceptions.RequestException as e:
                        print(f"Error fetching video data for {track_name}: {e}")

                return df_yt_data

            # Main logic
            channel_names = df_yt_data['artist'].tolist()
            most_popular_tracks = df_yt_data['album'].tolist()

            # Get channel IDs and data
            channel_ids, popular_tracks, updated_df_yt_data  = get_channel_ids(channel_names, most_popular_tracks, top_5_emerging_artists)
            

            if channel_ids:
                updated_df_yt_data, channel_titles, all_channel_ids = get_channels_data(channel_ids, updated_df_yt_data)
      

            if popular_tracks:
                updated_df_yt_data = get_video_data(channel_titles, popular_tracks, updated_df_yt_data)

            print(tabulate(updated_df_yt_data, headers='keys', tablefmt='pretty', showindex=False))
            return updated_df_yt_data
        top_5_emerging_artists = update_df_with_yt_data(top_5_emerging_artists)

        def update_df_with_yt_data_top5_downward_artists(df_yt_data):
            # Function to search for the channel IDs using channel names
            def get_channel_ids(channel_names):
                channel_ids = []
                search_url = "https://www.googleapis.com/youtube/v3/search"
                
                for channel_name in channel_names:
                    search_params = {
                        'part': 'snippet',
                        'q': channel_name,  # Channel name to search for
                        'type': 'channel',  # Only search for channels
                        'key': API_KEY
                    }
                    response = requests.get(search_url, params=search_params)
                    # print("Response:", response.json)
                    if response.status_code == 200:
                        data = response.json()
                        if data['items']:
                            # Add the first matched channel's ID
                            channel_ids.append(data['items'][0]['snippet']['channelId'])
                            df_yt_data.loc[df_yt_data['artist'] == channel_name, 'channel_id'] = data['items'][0]['snippet']['channelId']
                        else:
                            print(f"Noooo channel found for {channel_name}")
                    else:
                        print(f"Error fetching search results for {channel_name}: {response.status_code}")
                        print(response.text)
                
                return channel_ids

            # Function to fetch channel data for multiple channel IDs
            def get_channels_data(channel_ids):
                channel_url = "https://www.googleapis.com/youtube/v3/channels"
                channel_params = {
                    'part': 'snippet,contentDetails,statistics',
                    'id': ','.join(channel_ids),  # Pass all channel IDs at once
                    'key': API_KEY
                }
                
                response = requests.get(channel_url, params=channel_params)
                if response.status_code == 200:
                    data = response.json()
                    for channel in data.get('items', []):
                        channel_id = channel['id']
                        channel_name = channel['snippet']['title']

                        stats = channel.get('statistics', {})
                        subscriber_count = stats.get('subscriberCount', 0)
                        video_count = stats.get('videoCount', 0)
                        view_count = stats.get('viewCount', 0)
                        df_yt_data.loc[df_yt_data['channel_id'] == channel_id, 'subscribers'] = subscriber_count
                        df_yt_data.loc[df_yt_data['channel_id'] == channel_id, 'videos'] = video_count
                        df_yt_data.loc[df_yt_data['channel_id'] == channel_id, 'views'] = view_count

                else:
                    print(f"Error fetching channel data: {response.status_code}")

            channel_names = df_yt_data['artist'].tolist()
            channel_ids = get_channel_ids(channel_names)
            if channel_ids:
                get_channels_data(channel_ids)
            # print("yt_data_top5_downward_artists: ", df_yt_data)
            return df_yt_data
        top5_downward_artists = update_df_with_yt_data_top5_downward_artists(top5_downward_artists)

        def get_recovery_strategy(trending_percent, number_of_subscribers, total_views):
            number_of_subscribers = int(number_of_subscribers) if not pd.isna(number_of_subscribers) else 0
            total_views = int(total_views) if not pd.isna(total_views) else 0
            strategies = []

            # Trending-based strategies
            if trending_percent < -20:
                strategies.append("New Album Release")
            elif trending_percent < -10:
                strategies.append("Collaborative Projects")
            elif trending_percent < 0:
                strategies.append("Social Media Push")
            else:
                strategies.append("Fan Engagement")

            # Subscriber-based strategies
            if number_of_subscribers < 1000:
                strategies.append("Grassroots Campaign")
            elif number_of_subscribers < 10000:
                strategies.append("Giveaways")
            elif number_of_subscribers < 100000:
                strategies.append("Exclusive Content")
            else:
                strategies.append("Global Expansion")

            # View-based strategies
            if total_views < 100000:
                strategies.append("Revamp Content")
            elif total_views < 1000000:
                strategies.append("Boost Ads")
            else:
                strategies.append("Tour Announcement")
            final_strategy = ', '.join(strategies)
            # print("Recovery Strategy: ", final_strategy)
            return f'Recovery Strategy: {final_strategy}'

        top5_downward_artists['recovery_strategy'] = top5_downward_artists.apply(lambda x: get_recovery_strategy(x['trending_percent'], x['subscribers'], x['views']), axis=1)



        top_5_emerging_artists['social_growth'] = top_5_emerging_artists['popularity'] * 2
        top_5_emerging_artists['views'] = (
            top_5_emerging_artists['views']
            .replace([np.inf, -np.inf], 0)
            .fillna(0)
            .astype('int64')
        )
        top_5_emerging_artists['monthly_streams'] = top_5_emerging_artists['views'] * 2


        def platform_engagement(popularity, number_of_subscribers, views, followers):
            # Handle NaN values by replacing with 0
            popularity = int(popularity) if not pd.isna(popularity) else 0
            subscribers = int(number_of_subscribers) if not pd.isna(number_of_subscribers) else 0
            views = int(views) if not pd.isna(views) else 0
            
            # High, Very High, Medium, Medium High, Low
            engagement = ''
            if popularity *2 + subscribers + views + followers > 1000000:
                engagement = 'Very High'
            elif popularity *2 + subscribers + views + followers > 1000000:
                engagement = 'High'
            elif popularity *2 + subscribers + views + followers > 500000:
                engagement = 'Medium High'
            elif popularity *2 + subscribers + views + followers > 100000:
                engagement = 'Medium'
            else:
                engagement = 'Low'

            # print("platform engagement: ", engagement)
            return engagement

        # Applying the function to the DataFrame
        top_5_emerging_artists['engagement'] = top_5_emerging_artists.apply(
            lambda x: platform_engagement(x['popularity'], x['subscribers'], x['monthly_streams'], x['followers']),
            axis=1
        )
        client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
        prompt = """
        You are an AI model tasked with providing the following information about recording artists or groups for near-term media projects. Respond strictly in JSON format.
        Artitst should ONLY be from these countries (['US', 'CA', 'MX', 'GB', 'FR', 'DE', 'IT', 'ES', 'NL', 'BE', 'CH', 'AT', 'SE', 'NO', 'DK', 'FI', 'IE', 'PT', 'LU', 'IS'])

        1. Identify five {{artist_type}} recording artists or groups that would be suitable for films currently in development or production, slated for release within the next 18 months year 2025.
        2. These artists or groups should have an AI-predicted likelihood of maintaining or increasing their popularity over the next 18 months of from year 2025.
        3. If specific information cannot be determined, return an empty JSON for that field.


        Your response should be a JSON object like this:

        {
        "artists": [
            {
            "name": "Artist or Group Name",
            "projected_growth": number,
            "genre": "Genre of music",
            "genre_compatibility": from [LOW, MEDIUM, HIGH, VERY HIGH],
            "country": "Country of the artistm in short form"
            }
        ]
        }
        """
        def get_claude(artist_type, prompt=prompt):
            try:
                message = client.messages.create(
                    model="claude-3-5-haiku-20241022",
                    max_tokens=1000,
                    temperature=0.7,
                    system="You are an expert in music and media analysis.",
                    messages=[
                        {
                            "role": "user",
                            "content":  prompt.replace("{{artist_type}}", artist_type)
                        }
                    ]
                )
                return message
            except:
                return None

        def process_artists(artists):
            processed_artists = []
            for artist in artists:
                if not artist.get("name"):
                    raise NameError("Artist name is missing or empty.")

                artist["projected_growth"] = artist.get("projected_growth", 50)
                artist["genre"] = artist.get("genre", "Mix")
                artist["genre_compatibility"] = artist.get("genre_compatibility", "MEDIUM")
                processed_artists.append(artist)
            
            return processed_artists

        def get_artist_future_process(message):
            try:
                artists_data = json.loads(message.content[0].text)["artists"]

                for artist in artists_data:
                    if artist["projected_growth"] < 1:
                        artist["projected_growth"] *= 100

                sorted_artists = sorted(artists_data, key=lambda x: x["projected_growth"], reverse=True)
                sorted_artists = process_artists(sorted_artists)

            except:
                sorted_artists = {}
            return sorted_artists


        def get_artist_future_data(artist_type):
            message = get_claude(artist_type)
            if message:
                sorted_artists = get_artist_future_process(message)
            else:
                sorted_artists = {}
            return sorted_artists

        resp_emerg = get_artist_future_data('emerging/unknown')
        resp_est= get_artist_future_data('established')

        top5_upward_genres_req = top5_upward_genres[['genre', 'trending_percent']]
        top5_downward_genres_req = top5_downward_genres[['genre', 'trending_percent']]
        top5_upward_artists_req = top5_upward_artists[['artist', 'trending_percent']]
        top5_downward_artists_req = top5_downward_artists[['artist', 'trending_percent', 'recovery_strategy']]
        top_5_emerging_artists_req = top_5_emerging_artists[['artist', 'album', 'genre', 'release_date', 'artist_followers', 'artist_image_url', 'track_image_url', 'channel_name', 'channel_image_url', 'subscribers', 'videos', 'views', 'video_id', 'video_title', 'video_views', 'social_growth', 'monthly_streams', 'engagement']]


        # Get additional data for emerging and established artists
        resp_emerg = get_artist_future_data('emerging/unknown')
        resp_est = get_artist_future_data('established')


        def get_final_object(top5_upward_genres, top5_downward_genres, top5_upward_artists, top5_downward_artists, top_5_emerging_artists, emerging_artists_for_film=None, established_artists_for_film=None):
            final_object = {
                'top5_upward_genres': top5_upward_genres.fillna('None').to_dict(orient='records'),
                'top5_downward_genres': top5_downward_genres.fillna('None').to_dict(orient='records'),
                'top5_upward_artists': top5_upward_artists.fillna('None').to_dict(orient='records'),
                'top5_downward_artists': top5_downward_artists.fillna('None').to_dict(orient='records'),
                'top_5_emerging_artists': top_5_emerging_artists.fillna('None').to_dict(orient='records'),
            }

            # Add emerging and established artists to the final object
            if emerging_artists_for_film is not None:
                final_object['emerging_artists_for_film'] = emerging_artists_for_film
            if established_artists_for_film is not None:
                final_object['established_artists_for_film'] = established_artists_for_film

            return final_object

        final_object = get_final_object(top5_upward_genres_req, top5_downward_genres_req, top5_upward_artists_req, top5_downward_artists_req, top_5_emerging_artists_req,  emerging_artists_for_film=resp_emerg,
    established_artists_for_film=resp_est)
        

        print("final_object", final_object)    
        return final_object

    else:
        raise HTTPException(status_code=400, detail="API key is required")


