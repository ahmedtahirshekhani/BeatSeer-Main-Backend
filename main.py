import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from tabulate import tabulate
import requests
import pandas as pd
from bs4 import BeautifulSoup
from io import StringIO
from datetime import datetime

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
musicbrainzngs.set_useragent("BeetSeer_AI_Backend", "1.0", "ahmedtahir.developer@gmail.com")

# Function to get artist country
def get_artist_country(artist_name):
    try:
        result = musicbrainzngs.search_artists(artist=artist_name, limit=1)
        if 'artist-list' in result and len(result['artist-list']) > 0:
            return result['artist-list'][0].get('country', 'Unknown')
    except Exception as e:
        print(f"Error fetching country for {artist_name}: {e}")
    return 'Unknown'


app = FastAPI()

# Allow git ORS for all origins (or set specific origins for better security)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all origins, change for more restrictive policy
    allow_credentials=True,
    allow_methods=["*"],  # Allow all HTTP methods (GET, POST, etc.)
    allow_headers=["*"],  # Allow all headers
)

# Example endpoint
@app.get("/")
def read_root():
    return {"message": "Hello, FastAPI!"}

# Define another route with path parameters
@app.get("/news-letter")

def get_newsletter_data(
    youTubeApiKey: str = Query(...),
    spotify_CLIENT_ID: str = Query(...),
    spotify_CLIENT_SECRET: str = Query(...)
    ):
    if youTubeApiKey and spotify_CLIENT_ID and spotify_CLIENT_SECRET:
        CLIENT_ID = spotify_CLIENT_ID
        CLIENT_SECRET = spotify_CLIENT_SECRET
        # Authenticate with Spotify API
        auth_manager = SpotifyClientCredentials(client_id=CLIENT_ID, client_secret=CLIENT_SECRET)
        sp = spotipy.Spotify(auth_manager=auth_manager)


        def get_kworb_spotify_data(country='global_daily'):
            url = f'https://kworb.net/spotify/country/{country}.html'
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
            df_f['country'] = df_f['artist'].apply(get_artist_country)
            df_filtered = df_f[~df_f['country'].isin(['RU', 'CN'])]

            return df_filtered

        df = get_kworb_spotify_data()
        df2 = get_kworb_spotify_data('global_weekly')
        df = pd.concat([df, df2])
        df.drop_duplicates(subset=['artist', 'title'], inplace=True)
        # ascending=False will sort the values in descending order
        df.sort_values('trending_percent', ascending=False, inplace=True)
        df.drop_duplicates(subset=['artist_id'], inplace=True)
        df = df.reset_index(drop=True)


        def get_artists(art_ids):
            data = sp.artists(art_ids)
            return data

        for i in range(0, df.shape[0], 50):
            art_ids = df.loc[i:i+49, 'artist_id'].tolist()
            arts = get_artists(art_ids)
            arts_list = arts['artists']
            genres = [art['genres'][0] if len(art['genres']) > 0 else '' for art in arts_list]
            df.loc[i:i+49, 'genre'] = genres

        # List of dead artists (from previous Python list)
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

        # Filter out dead artists
        df_obs = df[~df['artist'].isin(dead_artists)]


        # group by genre and avg trending_percent and get top 5 genres with upword trend and top 5 genres with downward trend
        # ignore trending_percent with 1 or -1
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
        top_5_emerging_artists = filtered_album_artist.sort_values('popularity', ascending=True).head(10).reset_index(drop=True)
        # print("Spotify data", top_5_emerging_artists)

        top5_upward_genres.loc[:, 'trending_percent'] = (top5_upward_genres['trending_percent']*100).round(1)
        top5_downward_genres.loc[:, 'trending_percent'] = (top5_downward_genres['trending_percent']*100).round(1)
        top5_upward_artists.loc[:, 'trending_percent'] = (top5_upward_artists['trending_percent']*100).round(1)
        top5_downward_artists.loc[:, 'trending_percent'] = (top5_downward_artists['trending_percent']*100).round(1)

        API_KEY = youTubeApiKey
        def update_df_with_yt_data(df_yt_data):

            # Function to search for the channel IDs using channel names
            def get_channel_ids(channel_names, most_popular_tracks, df_yt_data):
                channel_ids = []
                related_tracks = []
                search_url = "https://www.googleapis.com/youtube/v3/search"
                
                for channel_name, track_name in zip(channel_names, most_popular_tracks):
                    # Combine channel name and most popular track for the search query
                    search_query = f"{track_name} {channel_name}"
                    search_params = {
                        'part': 'snippet',
                        'q': search_query,
                        'type': 'channel',
                        'key': API_KEY
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
                    search_query = f"{track_name} {channel_name}"
                    search_params = {
                        'part': 'snippet',
                        'q': search_query,  # Search for the most popular track name
                        'type': 'video',  # Only search for videos
                        'key': API_KEY
                    }
                    try:
                        response = requests.get(search_url, params=search_params)
                        response.raise_for_status()
                        data = response.json()
                        # print(" video Data: ", data['items'])

                        if data.get('items'):
                            video_id = data['items'][0]['id']['videoId']
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
        # print("top_5_emerging_artists: ", top_5_emerging_artists)
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
                    for channel in data['items']:
                        channel_id = channel['id']
                        channel_name = channel['snippet']['title']
                        subscriber_count = channel['statistics']['subscriberCount']
                        video_count = channel['statistics']['videoCount']
                        view_count = channel['statistics']['viewCount']
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



        # social_growth = popularity * 2
        top_5_emerging_artists['social_growth'] = top_5_emerging_artists['popularity'] * 2
        # Monthy streams = views * 2
        top_5_emerging_artists['views'] = top_5_emerging_artists['views'].astype(int)
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
            "genre_compatibility": from [LOW, MEDIUM, HIGH, VERY HIGH]
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
            # print("IN final OBJECT")
            final_object = {}
            final_object['top5_upward_genres'] = top5_upward_genres.to_dict(orient='records')
            final_object['top5_downward_genres'] = top5_downward_genres.to_dict(orient='records')
            final_object['top5_upward_artists'] = top5_upward_artists.to_dict(orient='records')
            final_object['top5_downward_artists'] = top5_downward_artists.to_dict(orient='records')
            final_object['top_5_emerging_artists'] = top_5_emerging_artists.to_dict(orient='records')
             # Add emerging and established artists to the final object
            if emerging_artists_for_film is not None:
                final_object['emerging_artists_for_film'] = emerging_artists_for_film
            if established_artists_for_film is not None:
                final_object['established_artists_for_film'] = established_artists_for_film
            # if rising_star is not None:
            #     final_object['rising_star'] = rising_star
            # if rising_star_data is not None:
            #     final_object['rising_star_data'] = rising_star_data
            return final_object

        final_object = get_final_object(top5_upward_genres_req, top5_downward_genres_req, top5_upward_artists_req, top5_downward_artists_req, top_5_emerging_artists_req,  emerging_artists_for_film=resp_emerg,
    established_artists_for_film=resp_est)

        # print("final_object", final_object)    
        return final_object

    else:
        raise HTTPException(status_code=400, detail="API key is required")

# get_newsletter_data()
