# Εδώ θα μπει ο κώδικας του Worker Node
# engine/worker.py
# engine/worker.py
import socket
import json
import sys
import os
import time

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils import load_config
# Φέρνουμε τον αλγόριθμό σου!
from user_app import map_function

def start_worker(worker_id):
    config = load_config()
    my_config = config['worker_nodes'][worker_id - 1]
    
    print(f"[WORKER {worker_id}] Connecting to Master...")
    
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect((config['master_node']['ip'], config['master_node']['port']))
    
    # Εγγραφή
    reg_msg = json.dumps({"type": "register", "worker_id": worker_id, "address": my_config})
    client.send(reg_msg.encode('utf-8'))
    
    # Buffer για να μαζεύουμε τα δεδομένα
    buffer = ""
    
    while True:
        try:
            # Λαμβάνουμε δεδομένα (μπορεί να έρθουν σε κομμάτια)
            chunk = client.recv(4096 * 10).decode('utf-8')
            if not chunk: break
            
            buffer += chunk
            
            # Προσπαθούμε να δούμε αν ήρθε ολόκληρο JSON
            try:
                msg = json.loads(buffer)
                buffer = "" # Καθαρισμός αν πετύχει
                
                if msg['type'] == 'map_task':
                    data_lines = msg['data']
                    print(f"[WORKER {worker_id}] Received {len(data_lines)} lines. Processing...")
                    
                    # --- EXECUTE MAP ---
                    map_results = []
                    for line in data_lines:
                        # Καλούμε τη συνάρτηση που έγραψες στο user_app
                        results = map_function(line)
                        map_results.extend(results)
                    
                    print(f"[WORKER {worker_id}] Generated {len(map_results)} key-value pairs.")
                    
                    # --- SAVE INTERMEDIATE RESULTS ---
                    # Αποθηκεύουμε τα αποτελέσματα τοπικά σε αρχείο JSON
                    output_file = f"map_results_{worker_id}.json"
                    with open(output_file, 'w') as f:
                        json.dump(map_results, f)
                        
                    print(f"[WORKER {worker_id}] Saved results to {output_file}")
                    
                    # Ειδοποιούμε τον Master
                    done_msg = json.dumps({"type": "map_done", "worker_id": worker_id})
                    client.send(done_msg.encode('utf-8'))
                    
            except json.JSONDecodeError:
                # Αν αποτύχει, σημαίνει ότι δεν ήρθε όλο το μήνυμα ακόμα. Συνεχίζουμε λήψη.
                continue
                
        except Exception as e:
            print(f"Error: {e}")
            break

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python worker.py <worker_id>")
    else:
        start_worker(int(sys.argv[1]))
#print('Worker node module ready')