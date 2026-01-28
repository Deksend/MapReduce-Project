"""
Problem 2: Compute per-genre statistics of explicit lyrics

Author: [Your Name]

For each genre, count the total number of tracks and how many have explicit lyrics,
then compute the fraction of explicit tracks.

Optional Feature:
  --popularity    Include average popularity comparison (explicit vs non-explicit)

Usage:
  python engine/worker.py 1 user_app_problem2                # basic
  python engine/worker.py 1 user_app_problem2 --popularity   # with popularity analysis
"""

import csv
import io

# --- FEATURE FLAG ---
POPULARITY_ENABLED = False


def configure_features(args):
    """Parse CLI arguments to enable optional features"""
    global POPULARITY_ENABLED
    
    if args is None:
        args = []
    
    print(f"[PROBLEM 2] Configuring with args: {args}")
    
    if "--popularity" in args:
        POPULARITY_ENABLED = True
        print("[PROBLEM 2] Optional feature enabled: popularity analysis")
    else:
        print("[PROBLEM 2] Running with basic features only")


# --- 1. MAP FUNCTION ---
def map_function(document_line):
    """
    Input: A single CSV line (string)
    Output: List of (Key, Value) tuples
    
    CSV columns:
    Index 5: popularity
    Index 7: explicit (True/False)
    Index 20: track_genre
    """
    try:
        f = io.StringIO(document_line)
        reader = csv.reader(f, delimiter=',')
        row = next(reader)

        # if len(row) < 21:
        #     return []

        # Skip header row
        if row[3] == 'explicit':
            return []

        # Extract required fields
        genre = row[17].strip()
        explicit_str = row[3].strip()
        explicit_flag = 1 if explicit_str.lower() == 'true' else 0

        # Base value data
        value_data = {
            "track_count": 1,
            "explicit_count": explicit_flag,
        }

        # --- OPTIONAL: Popularity analysis ---
        if POPULARITY_ENABLED:
            try:
                popularity = int(row[6])
                if explicit_flag:
                    value_data["explicit_popularity_sum"] = popularity
                    value_data["explicit_popularity_count"] = 1
                    value_data["clean_popularity_sum"] = 0
                    value_data["clean_popularity_count"] = 0
                else:
                    value_data["explicit_popularity_sum"] = 0
                    value_data["explicit_popularity_count"] = 0
                    value_data["clean_popularity_sum"] = popularity
                    value_data["clean_popularity_count"] = 1
            except:
                value_data["explicit_popularity_sum"] = 0
                value_data["explicit_popularity_count"] = 0
                value_data["clean_popularity_sum"] = 0
                value_data["clean_popularity_count"] = 0

        return [(genre, value_data)]

    except Exception as e:
        return []


# --- 2. REDUCE FUNCTION ---
def reduce_function(key, values_list):
    """
    Input:
      key: Genre name
      values_list: List of dicts with track data
      
    Output:
      Dict with genre statistics
    """
    total_tracks = 0
    explicit_tracks = 0
    
    # Popularity accumulators
    explicit_pop_sum = 0
    explicit_pop_count = 0
    clean_pop_sum = 0
    clean_pop_count = 0

    for item in values_list:
        total_tracks += item['track_count']
        explicit_tracks += item['explicit_count']
        
        # --- OPTIONAL: Popularity ---
        if POPULARITY_ENABLED and "explicit_popularity_sum" in item:
            explicit_pop_sum += item.get("explicit_popularity_sum", 0)
            explicit_pop_count += item.get("explicit_popularity_count", 0)
            clean_pop_sum += item.get("clean_popularity_sum", 0)
            clean_pop_count += item.get("clean_popularity_count", 0)

    if total_tracks == 0:
        return None

    # --- BUILD RESULT ---
    explicit_ratio = explicit_tracks / total_tracks
    
    result = {
        "genre": key,
        "total_tracks": total_tracks,
        "explicit_tracks": explicit_tracks,
        "explicit_ratio": round(explicit_ratio, 4),
        "explicit_percentage": round(explicit_ratio * 100, 2),
    }

    # --- OPTIONAL: Popularity stats ---
    if POPULARITY_ENABLED:
        if explicit_pop_count > 0:
            result["avg_popularity_explicit"] = round(explicit_pop_sum / explicit_pop_count, 2)
        else:
            result["avg_popularity_explicit"] = None
            
        if clean_pop_count > 0:
            result["avg_popularity_clean"] = round(clean_pop_sum / clean_pop_count, 2)
        else:
            result["avg_popularity_clean"] = None
        
        # Difference: positive = explicit more popular
        if result["avg_popularity_explicit"] and result["avg_popularity_clean"]:
            result["popularity_diff"] = round(
                result["avg_popularity_explicit"] - result["avg_popularity_clean"], 2
            )

    return result


# --- TESTING ---
if __name__ == "__main__":
    print("=" * 60)
    print("Problem 2: Per-Genre Explicit Lyrics Statistics")
    print("=" * 60)
    
    # Test with popularity enabled
    print("\n[Testing with --popularity enabled]\n")
    POPULARITY_ENABLED = True
    
    sample_lines = [
        '0,ID1,Artist1,Album,Song,73,230666,False,0.6,0.4,1,-6.7,0,0.1,0.03,0.0,0.3,0.7,87.9,4,pop',
        '1,ID2,Artist2,Album,Song,95,180000,True,0.8,0.9,5,-4.5,1,0.2,0.01,0.0,0.1,0.6,120.0,4,pop',
        '2,ID3,Artist3,Album,Song,88,200000,True,0.7,0.7,3,-5.0,1,0.1,0.02,0.0,0.2,0.7,110.0,4,pop',
        '3,ID4,Artist4,Album,Song,60,210000,False,0.6,0.6,2,-6.0,1,0.1,0.03,0.0,0.1,0.8,100.0,4,rock',
    ]
    
    all_mapped = []
    for line in sample_lines:
        mapped = map_function(line)
        all_mapped.extend(mapped)
    
    # Group by genre
    grouped = {}
    for genre, value in all_mapped:
        if genre not in grouped:
            grouped[genre] = []
        grouped[genre].append(value)
    
    print("Results:")
    for genre, values in grouped.items():
        result = reduce_function(genre, values)
        print(f"\n{genre}: {result}")
    
    print("\n" + "=" * 60)
    print("Usage:")
    print("  python worker.py 1 user_app_problem2              # basic")
    print("  python worker.py 1 user_app_problem2 --popularity # + popularity")
    print("=" * 60)
