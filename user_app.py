import csv
import io

# --- 1. MAP FUNCTION ---
def map_function(document_line):
    """
    Input: Μια γραμμή CSV (string)
    Output: Μια λίστα από (Key, Value) tuples
    """
    try:
        # Χρησιμοποιούμε το csv module για σωστό διάβασμα (χειρίζεται τα quotes "")
        f = io.StringIO(document_line)
        reader = csv.reader(f, delimiter=',')
        row = next(reader)

        # Βάσει του αρχείου dataset.csv που ανέβασες:
        # Index 2: Artists
        # Index 6: Duration_ms
        # Index 20: Track Genre (Το Κλειδί μας)
        
        if len(row) < 21: # Έλεγχος αν η γραμμή είναι ελλιπής
            return []

        # Αγνοούμε την επικεφαλίδα αν πέσουμε πάνω της
        if row[6] == 'duration_ms':
            return []

        artist_name = row[2]
        duration_ms = int(row[6])
        genre = row[20] # Αυτό είναι το Κλειδί μας πλέον

        # Φτιάχνουμε το Value Object
        value_data = {
            "duration": duration_ms,
            "artist": artist_name
        }
        
        # Επιστρέφουμε: Key -> Genre, Value -> {duration, artist}
        return [(genre, value_data)]
        
    except Exception as e:
        # Αν υπάρξει λάθος (π.χ. χαλασμένη γραμμή), την αγνοούμε
        return []

# --- 2. REDUCE FUNCTION ---
def reduce_function(key, values_list):
    """
    Input: 
      key: Το είδος μουσικής (π.χ. "pop", "rock")
      values_list: Λίστα με dictionaries
    Output: 
      Στατιστικά για αυτό το είδος
    """
    total_duration = 0
    count = 0
    artist_counts = {} 
    
    for item in values_list:
        # 1. Άθροισμα διάρκειας
        total_duration += item['duration']
        count += 1
        
        # 2. Καταμέτρηση καλλιτεχνών για να βρούμε τον πιο δημοφιλή
        art = item['artist']
        if art in artist_counts:
            artist_counts[art] += 1
        else:
            artist_counts[art] = 1
            
    if count == 0:
        return None

    # Υπολογισμοί
    avg_duration_ms = total_duration / count
    avg_duration_min = avg_duration_ms / 60000 # Μετατροπή ms σε λεπτά
    
    # Εύρεση του καλλιτέχνη με τα περισσότερα τραγούδια στο είδος
    top_artist = max(artist_counts, key=artist_counts.get)
    
    result = {
        "genre": key,
        "avg_duration_minutes": round(avg_duration_min, 2),
        "top_artist": top_artist,
        "total_tracks_processed": count
    }
    
    return result

# --- TESTING (Τρέξε το για να δεις αν δουλεύει) ---
if __name__ == "__main__":
    print("--- Testing Map Function ---")
    # Μια πραγματική γραμμή από το αρχείο σου (Gen Hoshino - Comedy)
    sample_line = '0,5SuOikwiRyPMVoIQDJUgSV,Gen Hoshino,Comedy,Comedy,73,230666,False,0.676,0.461,1,-6.746,0,0.143,0.0322,1.01e-06,0.358,0.715,87.917,4,acoustic'
    
    mapped = map_function(sample_line)
    print(f"Mapped Result: {mapped}")
    
    print("\n--- Testing Reduce Function ---")
    # Ψεύτικα δεδομένα για τεστ
    sample_values = [
        {"duration": 230000, "artist": "Gen Hoshino"},
        {"duration": 150000, "artist": "Ben Woodward"},
        {"duration": 240000, "artist": "Gen Hoshino"}
    ]
    reduced = reduce_function("acoustic", sample_values)
    print(f"Reduced Result: {reduced}")