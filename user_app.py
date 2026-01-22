import csv
import io

# ==========================================
# ⚙️ CONFIGURATION (ΟΙ ΠΑΡΑΜΕΤΡΟΙ ΤΟΥ ΚΑΘΗΓΗΤΗ)
# ==========================================

# "Begging and end year... for filtering"
FILTER_START_YEAR = 1990  
FILTER_END_YEAR = 2023    

# "Parameters for specifying details... Intervals"
INTERVAL_SIZE = 5         # Π.χ. 5 για πενταετίες, 10 για δεκαετίες

# "Shifting the beginning... +-1"
INTERVAL_OFFSET = 0       # Π.χ. 0 για 2000-2004, 1 για 2001-2005
# ==========================================

def get_interval_key(year):
    """Υπολογίζει δυναμικά το διάστημα με βάση το Size και το Offset"""
    shifted_year = year - INTERVAL_OFFSET
    interval_start = (shifted_year // INTERVAL_SIZE) * INTERVAL_SIZE + INTERVAL_OFFSET
    interval_end = interval_start + INTERVAL_SIZE - 1
    return f"{interval_start}-{interval_end}"

# --- 1. MAP FUNCTION ---
def map_function(document_line):
    try:
        f = io.StringIO(document_line)
        reader = csv.reader(f, delimiter=',')
        row = next(reader)

        # Έλεγχος για το dataset με τις 18 στήλες
        if len(row) < 18: return []
        if row[2] == 'duration_ms': return [] # Skip Header

        # Indexes: 0=Artist, 2=Duration, 4=Year, 17=Genre
        artist_name = row[0]
        
        try:
            duration_ms = int(row[2])
            year = int(row[4])
        except ValueError:
            return [] 

        # --- ΥΛΟΠΟΙΗΣΗ ΦΙΛΤΡΟΥ ---
        if year < FILTER_START_YEAR or year > FILTER_END_YEAR:
            return []

        # --- ΥΛΟΠΟΙΗΣΗ INTERVAL/SHIFTING ---
        key = get_interval_key(year)

        value_data = {
            "duration": duration_ms,
            "artist": artist_name,
            "original_year": year, # Κρατάμε τη χρονιά για να φτιάξουμε τη λίστα μετά
            "genre": row[17]
        }
        
        return [(key, value_data)]
        
    except Exception as e:
        return []

# --- 2. REDUCE FUNCTION ---
def reduce_function(key, values_list):
    total_duration = 0
    count = 0
    artist_counts = {} 
    
    # "List of the years" -> Εδώ μαζεύουμε ποιες χρονιές βρήκαμε
    years_found = set()
    
    for item in values_list:
        total_duration += item['duration']
        count += 1
        
        # Καταμέτρηση καλλιτέχνη
        art = item['artist']
        artist_counts[art] = artist_counts.get(art, 0) + 1
        
        # Προσθήκη χρονιάς στη λίστα
        if 'original_year' in item:
            years_found.add(item['original_year'])
            
    if count == 0: return None

    # Υπολογισμοί
    avg_duration_min = (total_duration / count) / 60000 
    top_artist = max(artist_counts, key=artist_counts.get)
    
    # Ταξινομούμε τη λίστα ετών για να είναι ευανάγνωστη
    sorted_years = sorted(list(years_found))

    return {
        "interval": key,
        "years_included": sorted_years, # <--- ΕΔΩ ΕΙΝΑΙ Η ΛΙΣΤΑ ΠΟΥ ΖΗΤΗΣΕ
        "avg_duration_minutes": round(avg_duration_min, 2),
        "top_artist": top_artist,
        "total_tracks": count
    }

# --- TESTING ---
if __name__ == "__main__":
    print(f"--- Configuration: {INTERVAL_SIZE}-year intervals, Offset: {INTERVAL_OFFSET} ---")
    line = 'Britney Spears,Song,200000,False,2002,80,0.5,0.5,1,-5,0,0.05,0.3,0,0.3,0.9,95,pop'
    print(f"Mapped: {map_function(line)}")