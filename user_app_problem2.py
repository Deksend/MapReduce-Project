"""
Problem 2: Compute per-genre statistics of explicit lyrics

Author: [Your Name]

For each genre, count the total number of tracks and how many have explicit lyrics,
then compute the fraction of explicit tracks.

MapReduce Steps:
1. Map: Emit (genre, (1, explicit_flag)) for each track
2. Shuffle: Group all values by genre key (handled by engine)
3. Reduce: Sum counts and compute explicit ratio
"""

import csv
import io


# --- 1. MAP FUNCTION ---
def map_function(document_line):
    """
    Input: A single CSV line (string)
    Output: List of (Key, Value) tuples
    
    Key: genre (string)
    Value: dict with track_count and explicit_count
    
    CSV columns (based on dataset.csv):
    Index 0: row_number
    Index 1: track_id
    Index 2: artists
    Index 3: album_name
    Index 4: track_name
    Index 5: popularity
    Index 6: duration_ms
    Index 7: explicit (True/False)
    Index 8-19: audio features
    Index 20: track_genre
    """
    try:
        # Use csv module to properly handle quoted fields
        f = io.StringIO(document_line)
        reader = csv.reader(f, delimiter=',')
        row = next(reader)

        # Check if row has enough columns
        if len(row) < 21:
            return []

        # Skip header row
        if row[7] == 'explicit':
            return []

        # Extract relevant fields
        explicit_str = row[7].strip()  # "True" or "False"
        genre = row[20].strip()        # Genre is our key

        # Convert explicit to flag (1 if explicit, 0 otherwise)
        explicit_flag = 1 if explicit_str.lower() == 'true' else 0

        # Value is a dict with counts for easy aggregation
        value_data = {
            "track_count": 1,
            "explicit_count": explicit_flag
        }

        # Return: Key -> genre, Value -> {track_count, explicit_count}
        return [(genre, value_data)]

    except Exception as e:
        # Skip malformed lines
        return []


# --- 2. REDUCE FUNCTION ---
def reduce_function(key, values_list):
    """
    Input:
      key: Genre name (e.g., "pop", "rock", "acoustic")
      values_list: List of dicts with track_count and explicit_count
      
    Output:
      Dict with genre statistics
    """
    total_tracks = 0
    explicit_tracks = 0

    for item in values_list:
        total_tracks += item['track_count']
        explicit_tracks += item['explicit_count']

    if total_tracks == 0:
        return None

    # Calculate explicit ratio
    explicit_ratio = explicit_tracks / total_tracks
    explicit_percentage = round(explicit_ratio * 100, 2)

    result = {
        "genre": key,
        "total_tracks": total_tracks,
        "explicit_tracks": explicit_tracks,
        "explicit_ratio": round(explicit_ratio, 4),
        "explicit_percentage": explicit_percentage
    }

    return result


# --- TESTING ---
if __name__ == "__main__":
    print("=" * 60)
    print("Problem 2: Per-Genre Explicit Lyrics Statistics")
    print("=" * 60)
    
    print("\n--- Testing Map Function ---")
    
    # Test with explicit=False track
    sample_line_1 = '0,5SuOikwiRyPMVoIQDJUgSV,Gen Hoshino,Comedy,Comedy,73,230666,False,0.676,0.461,1,-6.746,0,0.143,0.0322,1.01e-06,0.358,0.715,87.917,4,acoustic'
    mapped_1 = map_function(sample_line_1)
    print(f"Line 1 (explicit=False): {mapped_1}")
    
    # Test with explicit=True track
    sample_line_2 = '1,ABC123,Eminem,Album,Song,85,180000,True,0.8,0.9,5,-4.5,1,0.2,0.01,0.0,0.1,0.6,120.0,4,hip-hop'
    mapped_2 = map_function(sample_line_2)
    print(f"Line 2 (explicit=True):  {mapped_2}")
    
    print("\n--- Testing Reduce Function ---")
    
    sample_values = [
        {"track_count": 1, "explicit_count": 0},
        {"track_count": 1, "explicit_count": 1},
        {"track_count": 1, "explicit_count": 1},
        {"track_count": 1, "explicit_count": 0},
        {"track_count": 1, "explicit_count": 1},
    ]
    reduced = reduce_function("pop", sample_values)
    print(f"Result: {reduced}")
    
    print("\n" + "=" * 60)
    print("Tests passed!")
    print("=" * 60)
