import pandas as pd
import requests
import time
import math
import os
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

# ====== CONFIG ======
TMDB_API_KEY = "275dba03fdc2e0550032ae189c33a322"   # <- your key
INPUT_CSV = "tmdb_api_movies_filtered.csv"           # file with 'id' column (36k file)
OUTPUT_CSV = "tmdb_api_movies_enriched.csv"
TEMP_PROGRESS = "tmdb_enriched_progress.csv"

MAX_WORKERS = 4          # safe parallelism (reduce if you hit 429)
SAVE_INTERVAL = 200      # save every N enriched movies
REQUEST_SLEEP = 0.25     # pause between requests in same thread (seconds)
MAX_RETRIES = 5          # per request retry attempts
BACKOFF_BASE = 2         # exponential backoff base (on 429/5xx)
FALLBACK_SEARCH = False  # if True, attempt search by title/year when id is missing

# ====== HELPERS ======
def safe_get(url, params, max_retries=MAX_RETRIES):
    """GET with handling for 429 and transient errors. Returns response.json() or None."""
    wait = 1.0
    for attempt in range(1, max_retries + 1):
        try:
            r = requests.get(url, params=params, timeout=15)
        except requests.RequestException as e:
            # network error -> backoff and retry
            if attempt == max_retries:
                print(f" RequestException final: {e}")
                return None
            time.sleep(wait)
            wait *= BACKOFF_BASE
            continue

        if r.status_code == 200:
            return r.json()
        if r.status_code == 429:
            # Rate limited -> read Retry-After if present, else backoff
            retry_after = r.headers.get("Retry-After")
            if retry_after:
                sleep_for = int(retry_after) + 1
            else:
                sleep_for = wait
            print(f"  [429] rate limited. sleeping {sleep_for}s (attempt {attempt}/{max_retries})")
            time.sleep(sleep_for)
            wait *= BACKOFF_BASE
            continue
        if 500 <= r.status_code < 600:
            # server error -> backoff
            print(f"  [5xx] server error {r.status_code}, attempt {attempt}/{max_retries}")
            time.sleep(wait)
            wait *= BACKOFF_BASE
            continue
        # other client error (404, 401, etc.) -> don't retry
        # print(f"  [HTTP {r.status_code}] {url} -> skipping")
        return None
    return None

def fetch_by_id(movie_id):
    """Fetch detail + credits for a single movie id. Returns dict or None."""
    base = "https://api.themoviedb.org/3"
    params = {"api_key": TMDB_API_KEY}

    # details
    details = safe_get(f"{base}/movie/{movie_id}", params)
    if not details:
        return None

    # small pause to avoid bursting
    time.sleep(REQUEST_SLEEP)

    # credits
    credits = safe_get(f"{base}/movie/{movie_id}/credits", params)
    if not credits:
        # still return details if credits missing? we choose to return None,
        # you can change to return partial info by commenting the next line.
        return None

    # director
    director = None
    for c in credits.get("crew", []):
        if c.get("job") == "Director":
            director = c.get("name")
            break

    # top 3 cast average 'popularity' as proxy
    top_cast = credits.get("cast", [])[:3]
    top_cast_avg = None
    if top_cast:
        vals = [c.get("popularity", 0) for c in top_cast]
        top_cast_avg = sum(vals) / len(vals)

    return {
        "id": int(movie_id),
        "budget": details.get("budget"),
        "revenue": details.get("revenue"),
        "director_encoded": director,
        "top_cast_avg_rating": top_cast_avg
    }

def search_and_get(title, year=None):
    """Optional fallback: search TMDB by title+year and return first match id (or None)."""
    base = "https://api.themoviedb.org/3/search/movie"
    params = {"api_key": TMDB_API_KEY, "query": title}
    if year and not pd.isna(year):
        params["year"] = int(year)
    data = safe_get(base, params)
    if not data:
        return None
    results = data.get("results", [])
    if not results:
        return None
    return results[0].get("id")

# ====== MAIN ======
def main():
    os.makedirs("data_cleaned", exist_ok=True)

    # load input
    df = pd.read_csv(INPUT_CSV)
    # normalize id column (some may be float or missing)
    if "id" in df.columns:
        df["id"] = pd.to_numeric(df["id"], errors="coerce")
    else:
        df["id"] = pd.NA

    # determine which rows to enrich:
    # default: enrich rows where budget or revenue is missing
    to_enrich = df[(df["budget"].isna() | df["revenue"].isna())].copy()
    print(f"Total movies with missing budget/revenue: {len(to_enrich)}")

    # Build list of (id,title,year) to process
    tasks = []
    for _, row in to_enrich.iterrows():
        tasks.append({
            "id": int(row["id"]) if pd.notna(row["id"]) else None,
            "title": row.get("title"),
            "year": row.get("release_year") if "release_year" in row else None
        })

    # load progress if exists
    enriched = []
    if os.path.exists(TEMP_PROGRESS):
        print("Resuming from temp progress file...")
        pdf = pd.read_csv(TEMP_PROGRESS)
        enriched = pdf.to_dict(orient="records")
        done_ids = {int(x["id"]) for x in enriched if pd.notna(x.get("id"))}
    else:
        done_ids = set()

    # prepare job list: either ids from tasks that are not done, or fallback search
    jobs = []
    for t in tasks:
        if t["id"] is not None and t["id"] in done_ids:
            continue
        jobs.append(t)

    print(f"Jobs to fetch: {len(jobs)} (skipping {len(done_ids)} already done)")

    # Threaded fetching with cautious concurrency
    # We'll submit jobs that use the id if present; if id absent and FALLBACK_SEARCH True, we will search then fetch
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as exe:
        future_to_job = {}
        for job in jobs:
            mid = job["id"]
            if mid is not None:
                future = exe.submit(fetch_by_id, mid)
                future_to_job[future] = job
            else:
                if FALLBACK_SEARCH:
                    # find id via search then fetch
                    def _search_and_fetch(job_inner):
                        found = search_and_get(job_inner["title"], job_inner.get("year"))
                        if not found:
                            return None
                        return fetch_by_id(found)
                    future = exe.submit(_search_and_fetch, job)
                    future_to_job[future] = job
                else:
                    # skip if no id and no fallback
                    continue

        processed = 0
        for future in tqdm(as_completed(future_to_job), total=len(future_to_job), desc="Enriching"):
            job = future_to_job[future]
            try:
                res = future.result()
            except Exception as e:
                print(f" Exception for job {job}: {e}")
                res = None

            if res:
                enriched.append(res)
            processed += 1

            # Save periodically
            if processed % SAVE_INTERVAL == 0:
                pd.DataFrame(enriched).to_csv(TEMP_PROGRESS, index=False)
                print(f" Saved progress after {processed} processed jobs.")
                # small cooldown to be safe
                time.sleep(SLEEP_BETWEEN_BATCHES if 'SLEEP_BETWEEN_BATCHES' in globals() else 1)

    # final save of progress
    pd.DataFrame(enriched).to_csv(TEMP_PROGRESS, index=False)
    print(f"Done fetching. New enriched count: {len(enriched)}")

    # Merge enriched back into original df
    if len(enriched) > 0:
        enriched_df = pd.DataFrame(enriched)
        # ensure id dtype consistent
        enriched_df["id"] = pd.to_numeric(enriched_df["id"], errors="coerce")
        df["id"] = pd.to_numeric(df["id"], errors="coerce")
        merged = df.merge(enriched_df, on="id", how="left", suffixes=("", "_new"))

        for col in ["budget", "revenue", "director_encoded", "top_cast_avg_rating"]:
            if f"{col}_new" in merged.columns:
                merged[col] = merged[col].combine_first(merged[f"{col}_new"])
                merged.drop(columns=[f"{col}_new"], inplace=True, errors="ignore")

        # recompute engineered cols
        merged["budget_log"] = merged["budget"].apply(lambda x: math.log1p(x) if pd.notnull(x) and x>0 else None)
        merged["revenue_log"] = merged["revenue"].apply(lambda x: math.log1p(x) if pd.notnull(x) and x>0 else None)
        merged["profit_ratio"] = merged.apply(lambda r: r["revenue"]/r["budget"] if pd.notnull(r["budget"]) and r["budget"]>0 else None, axis=1)

        merged.to_csv(OUTPUT_CSV, index=False)
        print(f"Enriched dataset saved to: {OUTPUT_CSV}")
    else:
        print("No new enrichment data fetched. Nothing to merge.")

if __name__ == "__main__":
    main()
