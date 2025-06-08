import sys

import pandas as pd
import requests
import time

# Load your Excel file
df = pd.read_excel("1000_netflix_titles.xlsx")

# Replace with your real API endpoint and any auth/token as needed
API_BASE_URL = "https://api.themoviedb.org"
API_TOKEN = "eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiJhM2FjOTgzNGIwOTA0YzliMTNhYzVhNTk2MWQ3NmFlYSIsIm5iZiI6MTc0OTA1NjQwNC40NjQsInN1YiI6IjY4NDA3Yjk0YTgxMDA4YzI3NDdiYjc4MiIsInNjb3BlcyI6WyJhcGlfcmVhZCJdLCJ2ZXJzaW9uIjoxfQ.MhKJvGywax0UXL5o54ix8AB9wh43gQh48E9m1MCqtIc"  # if needed

# Create new columns to store enriched data
df['popularity'] = None
df['vote_count'] = None
df['vote_average'] = None
df['genres'] = None

all_genres = []

def get_genres():
    url = f"{API_BASE_URL}/3/genre/movie/list?language=en"
    headers = {
        "Authorization": f"Bearer {API_TOKEN}",  # or use "X-API-Key": API_KEY, etc.
        "Accept": "application/json"
    }
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        all_genres.extend(response.json()["genres"])
    else:
        print(f"Error {response.status_code}: {response.text}")

get_genres()

genre_map = {g['id']: g['name'] for g in all_genres}

# Helper function to call the API
def get_api_data(title, year):
    url = f"{API_BASE_URL}/3/search/movie?query={title}&language=en&year={year}&page=1"
    headers = {
        "Authorization": f"Bearer {API_TOKEN}",  # or use "X-API-Key": API_KEY, etc.
        "Accept": "application/json"
    }
    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"API error: {response.status_code}")
            return None
    except requests.RequestException as e:
        print(f"Request failed : {e}")
        return None


# Enrich each row
for idx, row in df.iterrows():
    print(idx)
    title = row['title']  # or 'show_id' or other column
    year = row['release_year']  # or 'show_id' or other column
    data = get_api_data(title, year)
    if data and data["results"]:
        genre_names = [genre_map.get(gid, f"Unknown({gid})") for gid in data["results"][0]["genre_ids"]]
        all_genres = ",".join(genre_names)
        df.at[idx, 'genres'] = all_genres
        df.at[idx, 'popularity'] = data["results"][0]["popularity"]
        df.at[idx, 'vote_count'] = data["results"][0]["vote_count"]
        df.at[idx, 'vote_average'] = data["results"][0]["vote_average"]

    time.sleep(0.05)  # optional: be nice to the API with a short delay

# Save the enriched data
df.to_excel("enriched_data.xlsx", index=False)
