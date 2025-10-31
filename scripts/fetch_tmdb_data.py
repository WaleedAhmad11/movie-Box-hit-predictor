import pandas as pd
import requests
import time

TMDB_API_KEY = "8ec51db19c2537a6ef80f4816df58717"
OUTPUT_CSV = "data_cleaned/tmdb_api_movies.csv"

START_YEAR = 2015
END_YEAR = 2023  # inclusive
MAX_PAGES = 200  # max pages per year
SLEEP_TIME = 0.1  # delay between requests to avoid rate limits

all_movies = []

def fetch_movies(year, page):
    url = "https://api.themoviedb.org/3/discover/movie"
    params = {
        "api_key": TMDB_API_KEY,
        "language": "en-US",
        "sort_by": "popularity.desc",
        "primary_release_year": year,
        "page": page
    }
    response = requests.get(url, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching year {year}, page {page}: {response.status_code}")
        return {}

# Loop through years
for year in range(START_YEAR, END_YEAR + 1):
    print(f"\nFetching movies for year: {year}")
    
    # First request to get total pages
    first_page_data = fetch_movies(year, 1)
    total_pages = min(first_page_data.get("total_pages", 1), MAX_PAGES)
    all_movies.extend(first_page_data.get("results", []))
    
    # Loop through remaining pages
    for page in range(2, total_pages + 1):
        data = fetch_movies(year, page)
        all_movies.extend(data.get("results", []))
        print(f"Year {year}, Page {page}/{total_pages} fetched")
        time.sleep(SLEEP_TIME)

# Save to CSV
df = pd.DataFrame(all_movies)
df.to_csv(OUTPUT_CSV, index=False)
print(f"\nTMDB data saved to {OUTPUT_CSV}, total movies fetched: {len(df)}")
