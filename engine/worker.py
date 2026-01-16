# engine/worker.py
import socket
import threading
import json
import sys
import os
import hashlib

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils import load_config
from user_app import map_function, reduce_function # Φέραμε και το reduce!

# Global list για να μαζεύουμε τα δεδομένα που μας στέλνουν οι άλλοι
incoming_shuffle_data = []
lock = threading.Lock()

def handle_peer_connection(conn, addr):
    """Δέχεται δεδομένα από ΑΛΛΟΝ Worker (P2P)"""
    print(f"[WORKER SERVER] Receiving data from peer {addr}")
    buffer = ""
    try:
        while True:
            chunk = conn.recv(4096).decode('utf-8')
            if not chunk: break
            buffer += chunk
            
        # Μόλις κλείσει η σύνδεση, αποθηκεύουμε τα δεδομένα
        data = json.loads(buffer)
        with lock:
            incoming_shuffle_data.extend(data)
        print(f"[WORKER SERVER] Received {len(data)} items from peer.")
        
    except Exception as e:
        print(f"[WORKER SERVER] Error receiving data: {e}")
    finally:
        conn.close()

def start_worker_server(my_ip, my_port):
    """Ανοίγει πόρτα για να ακούει άλλους Workers"""
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((my_ip, my_port))
    server.listen()
    
    while True:
        conn, addr = server.accept()
        t = threading.Thread(target=handle_peer_connection, args=(conn, addr))
        t.start()

def start_worker(worker_id):
    config = load_config()
    my_config = config['worker_nodes'][worker_id - 1]
    
    # 1. Ξεκινάμε τον Server μας σε ΞΕΧΩΡΙΣΤΟ Thread
    server_thread = threading.Thread(
        target=start_worker_server, 
        args=(my_config['ip'], my_config['port']),
        daemon=True
    )
    server_thread.start()
    
    print(f"[WORKER {worker_id}] Listening for peers on {my_config['port']}...")
    print(f"[WORKER {worker_id}] Connecting to Master...")
    
    # 2. Σύνδεση στον Master
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect((config['master_node']['ip'], config['master_node']['port']))
    
    # Εγγραφή
    reg_msg = json.dumps({"type": "register", "worker_id": worker_id, "address": my_config})
    client.send(reg_msg.encode('utf-8'))
    
    buffer = ""
    while True:
        try:
            chunk = client.recv(4096 * 10).decode('utf-8')
            if not chunk: break
            buffer += chunk
            
            try:
                # Προσπάθεια να διαβάσουμε JSON (μπορεί να είναι κολλημένα μηνύματα)
                while "{" in buffer and "}" in buffer:
                    start = buffer.find("{")
                    # Αυτό είναι απλοϊκό parsing για το παράδειγμα. 
                    # Σε production θέλει μετρητή brackets.
                    # Εδώ υποθέτουμε ότι το μήνυμα είναι έγκυρο JSON.
                    try:
                        msg = json.loads(buffer)
                        # Αν πετύχει το load, καθαρίζουμε το buffer (επικίνδυνο αν υπάρχουν πολλά μηνύματα, αλλά ΟΚ για τώρα)
                        buffer = "" 
                    except:
                        break # Περιμένουμε κι άλλα δεδομένα

                    # --- ENTOLEΣ MASTER ---
                    
                    # A. MAP PHASE
                    if msg['type'] == 'map_task':
                        data_lines = msg['data']
                        print(f"[WORKER {worker_id}] Starting MAP on {len(data_lines)} lines...")
                        map_results = []
                        for line in data_lines:
                            map_results.extend(map_function(line))
                        
                        # Save local
                        with open(f"map_results_{worker_id}.json", 'w') as f:
                            json.dump(map_results, f)
                        
                        client.send(json.dumps({"type": "map_done", "worker_id": worker_id}).encode('utf-8'))

                    # B. SHUFFLE PHASE
                    elif msg['type'] == 'start_shuffle':
                        all_workers = msg['workers'] # Λίστα με IPs/Ports όλων
                        print(f"[WORKER {worker_id}] Starting SHUFFLE...")
                        
                        # Φόρτωση των δικών μας map results
                        with open(f"map_results_{worker_id}.json", 'r') as f:
                            my_data = json.load(f)
                            
                        # Partitioning (Bucket Sort)
                        buckets = {wid: [] for wid in range(1, len(all_workers) + 1)}
                        
                        for key, value in my_data:
                            # HASHING: hash("Pop") % 4 + 1 -> Worker 2
                            # Χρησιμοποιούμε MD5 για σταθερό hash
                            hash_val = int(hashlib.md5(key.encode()).hexdigest(), 16)
                            target_id = (hash_val % len(all_workers)) + 1
                            buckets[target_id].append((key, value))
                            
                        # Αποστολή στους άλλους
                        for target_id, data_part in buckets.items():
                            if target_id == worker_id:
                                # Αν είναι για εμάς, τα βάζουμε απευθείας στη λίστα
                                with lock:
                                    incoming_shuffle_data.extend(data_part)
                            else:
                                # Αν είναι για άλλον, σύνδεση και αποστολή
                                target_info = config['worker_nodes'][target_id - 1]
                                try:
                                    p_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                                    p_sock.connect((target_info['ip'], target_info['port']))
                                    p_sock.send(json.dumps(data_part).encode('utf-8'))
                                    p_sock.close()
                                    print(f" -> Sent {len(data_part)} items to Worker {target_id}")
                                except Exception as e:
                                    print(f" -> Failed to send to Worker {target_id}: {e}")

                        client.send(json.dumps({"type": "shuffle_done", "worker_id": worker_id}).encode('utf-8'))

                    # C. REDUCE PHASE
                    elif msg['type'] == 'start_reduce':
                        print(f"[WORKER {worker_id}] Starting REDUCE on {len(incoming_shuffle_data)} items...")
                        
                        # Group by Key locally
                        grouped = {}
                        for key, val in incoming_shuffle_data:
                            if key not in grouped: grouped[key] = []
                            grouped[key].append(val)
                            
                        # Run User Reduce Function
                        final_results = []
                        for key, values in grouped.items():
                            res = reduce_function(key, values)
                            if res: final_results.append(res)
                            
                        # Save Final Output
                        out_file = f"reduce_results_{worker_id}.json"
                        with open(out_file, 'w') as f:
                            json.dump(final_results, f, indent=2)
                            
                        print(f"[WORKER {worker_id}] REDUCE DONE! Saved to {out_file}")
                        client.send(json.dumps({"type": "reduce_done", "worker_id": worker_id}).encode('utf-8'))

            except json.JSONDecodeError:
                continue
                
        except Exception as e:
            print(f"Connection Error: {e}")
            break

if __name__ == "__main__":
    start_worker(int(sys.argv[1]))