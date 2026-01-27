import csv
import io

FILTER_START_YEAR = 1990  
FILTER_END_YEAR = 2023    
INTERVAL_SIZE = 5
INTERVAL_OFFSET = 0

def get_interval_key(year):
    """Calculate time interval based on size and offset"""
    shifted_year = year - INTERVAL_OFFSET
    interval_start = (shifted_year // INTERVAL_SIZE) * INTERVAL_SIZE + INTERVAL_OFFSET
    interval_end = interval_start + INTERVAL_SIZE - 1
    return f"{interval_start}-{interval_end}"

def map_function(document_line):
    try:
        f = io.StringIO(document_line)
        reader = csv.reader(f, delimiter=',')
        row = next(reader)

        if len(row) < 18: return []
        if row[2] == 'duration_ms': return []
        artist_name = row[0]
        
        try:
            duration_ms = int(row[2])
            year = int(row[4])
        except ValueError:
            return [] 

        if year < FILTER_START_YEAR or year > FILTER_END_YEAR:
            return []
        key = get_interval_key(year)

        value_data = {
            "duration": duration_ms,
            "artist": artist_name,
            "original_year": year,
            "genre": row[17]
        }
        
        return [(key, value_data)]
        
    except Exception as e:
        return []

def reduce_function(key, values_list):
    total_duration = 0
    count = 0
    artist_counts = {}
    years_found = set()
    
    for item in values_list:
        total_duration += item['duration']
        count += 1
        art = item['artist']
        artist_counts[art] = artist_counts.get(art, 0) + 1
        if 'original_year' in item:
            years_found.add(item['original_year'])
            
    if count == 0: return None
    avg_duration_min = (total_duration / count) / 60000 
    top_artist = max(artist_counts, key=artist_counts.get)
    sorted_years = sorted(list(years_found))
    return {
        "interval": key,
        "years_included": sorted_years,
        "avg_duration_minutes": round(avg_duration_min, 2),
        "top_artist": top_artist,
        "total_tracks": count
    }

# test
if __name__ == "__main__":
    print(f"--- Configuration: {INTERVAL_SIZE}-year intervals, Offset: {INTERVAL_OFFSET} ---")
    line = 'Britney Spears,Song,200000,False,2002,80,0.5,0.5,1,-5,0,0.05,0.3,0,0.3,0.9,95,pop'
    print(f"Mapped: {map_function(line)}")