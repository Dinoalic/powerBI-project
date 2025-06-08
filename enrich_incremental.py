import sys
import pandas as pd
import requests
import time

# Load your Excel file
df = pd.read_excel("incremental_load.xlsx")

# Replace with your real API endpoint and token
API_BASE_URL = "https://api.themoviedb.org"
API_TOKEN = "eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiJhM2FjOTgzNGIwOTA0YzliMTNhYzVhNTk2MWQ3NmFlYSIsIm5iZiI6MTc0OTA1NjQwNC40NjQsInN1YiI6IjY4NDA3Yjk0YTgxMDA4YzI3NDdiYjc4MiIsInNjb3BlcyI6WyJhcGlfcmVhZCJdLCJ2ZXJzaW9uIjoxfQ.MhKJvGywax0UXL5o54ix8AB9wh43gQh48E9m1MCqtIc"

# Create new columns to store enriched data
df['popularity'] = None
df['vote_count'] = None
df['vote_average'] = None
df['genres'] = None

all_genres = []

def get_genres():
    url = f"{API_BASE_URL}/3/genre/movie/list?language=en"
    headers = {
        "Authorization": f"Bearer {API_TOKEN}",
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
        "Authorization": f"Bearer {API_TOKEN}",
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
    print(f"Processing row {idx}")
    title = row['title']
    year = row['release_year']
    state = str(row.get('state'))  # Ensure it's a string for comparison
    data = get_api_data(title, year)

    if data and data["results"]:
        movie = data["results"][0]

        # Extract genres
        genre_names = [genre_map.get(gid, f"Unknown({gid})") for gid in movie.get("genre_ids", [])]
        genre_str = ",".join(genre_names)
        df.at[idx, 'genres'] = genre_str

        # Handle popularity and vote count
        df.at[idx, 'popularity'] = movie.get("popularity")
        df.at[idx, 'vote_count'] = movie.get("vote_count")

        # Handle vote_average logic based on state
        rating = movie.get("vote_average")
        if rating is None:
            rating = 10
        elif state == "2":
            rating *= 3

        df.at[idx, 'vote_average'] = rating

    time.sleep(0.05)  # Short delay to avoid API rate limiting

# Save the enriched data
df.to_excel("enriched_incremental_data.xlsx", index=False)
print("✅ Enrichment complete and file saved.")
