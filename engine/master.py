# Εδώ θα μπει ο κώδικας του Master Node
# engine/master.py
# engine/master.py
import socket
import threading
import json
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils import load_config

# Global λίστα για να θυμόμαστε τους Workers
connected_workers = {} # {worker_id: connection_socket}
lock = threading.Lock()

def handle_worker(conn, addr):
    """Διαχειρίζεται τη σύνδεση με έναν Worker"""
    worker_id = None
    try:
        while True:
            # Περιμένουμε μήνυμα
            msg_bytes = conn.recv(4096 * 4) # Μεγάλο buffer για δεδομένα
            if not msg_bytes: break
            
            msg = json.loads(msg_bytes.decode('utf-8'))
            
            if msg['type'] == 'register':
                worker_id = msg['worker_id']
                with lock:
                    connected_workers[worker_id] = conn
                print(f"[MASTER] Worker {worker_id} registered successfully.")
                
            elif msg['type'] == 'map_done':
                print(f"[MASTER] Worker {worker_id} finished MAP phase!")
                
    except Exception as e:
        print(f"[MASTER] Error/Disconnect worker {worker_id}: {e}")
    finally:
        with lock:
            if worker_id in connected_workers:
                del connected_workers[worker_id]
        conn.close()

def distribute_map_tasks():
    """Διαβάζει το CSV και το μοιράζει στους Workers"""
    print("\n[MASTER] Reading dataset...")
    
    # Διαβάζουμε το αρχείο
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_path = os.path.join(base_dir, 'data', 'dataset.csv')
    
    try:
        with open(data_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
    except FileNotFoundError:
        print("ERROR: dataset.csv not found!")
        return

    # Αφαίρεση Header (αν υπάρχει)
    header = lines[0]
    data_lines = lines[1:]
    
    # Round-Robin διανομή (Μοίρασμα τράπουλας)
    worker_ids = list(connected_workers.keys())
    if not worker_ids:
        print("[MASTER] No workers connected to distribute tasks!")
        return

    chunks = {wid: [] for wid in worker_ids}
    
    for i, line in enumerate(data_lines):
        # Επιλέγουμε worker κυκλικά: 0, 1, 2, 0, 1...
        target_worker = worker_ids[i % len(worker_ids)]
        chunks[target_worker].append(line)

    print(f"[MASTER] Splitting {len(data_lines)} lines among {len(worker_ids)} workers.")

    # Αποστολή στους Workers
    for wid, lines_chunk in chunks.items():
        conn = connected_workers[wid]
        task_msg = json.dumps({
            "type": "map_task",
            "data": lines_chunk # Στέλνουμε τις γραμμές ως λίστα
        })
        # Προσοχή: Σε μεγάλα αρχεία αυτό θέλει chunking στο socket, 
        # αλλά για τώρα το στέλνουμε μια κι έξω.
        conn.sendall(task_msg.encode('utf-8'))
        print(f"[MASTER] Sent {len(lines_chunk)} lines to Worker {wid}")

def start_master():
    config = load_config()
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((config['master_node']['ip'], config['master_node']['port']))
    server.listen()
    
    # Thread για να αποδέχεται συνδέσεις στο παρασκήνιο
    def accept_connections():
        print(f"[MASTER] Listening on {config['master_node']['port']}...")
        while True:
            conn, addr = server.accept()
            t = threading.Thread(target=handle_worker, args=(conn, addr))
            t.start()
            
    threading.Thread(target=accept_connections, daemon=True).start()
    
    # Κύριο μενού ελέγχου
    while True:
        cmd = input("Type 'start' to distribute tasks, or 'exit': ")
        if cmd == 'start':
            distribute_map_tasks()
        elif cmd == 'exit':
            break

if __name__ == "__main__":
    start_master()
#print('Master node module ready')