# Εδώ θα μπει ο κώδικας του Worker Node
# engine/worker.py
import socket
import json
import sys
import os
import time

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils import load_config

def start_worker(worker_id):
    config = load_config()
    
    # Πληροφορίες Master
    master_ip = config['master_node']['ip']
    master_port = config['master_node']['port']
    
    # Πληροφορίες αυτού του Worker (από το config με βάση το ID)
    # Προσοχή: Στη λίστα το ID 1 είναι στο index 0
    my_config = config['worker_nodes'][worker_id - 1]
    print(f"[WORKER {worker_id}] Starting on {my_config['ip']}:{my_config['port']}")
    
    try:
        # Σύνδεση στον Master
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.connect((master_ip, master_port))
        
        # Αποστολή μηνύματος εγγραφής
        reg_msg = json.dumps({
            "type": "register",
            "worker_id": worker_id,
            "address": my_config
        })
        client.send(reg_msg.encode('utf-8'))
        
        # Αναμονή απάντησης
        response = client.recv(1024).decode('utf-8')
        print(f"[WORKER {worker_id}] Master said: {response}")
        
        client.close()
        
    except ConnectionRefusedError:
        print(f"[WORKER {worker_id}] Could not connect to Master. Is it running?")

if __name__ == "__main__":
    # Διαβάζουμε το ID του worker από τα arguments (π.χ. python worker.py 1)
    if len(sys.argv) < 2:
        print("Usage: python worker.py <worker_id>")
        sys.exit(1)
        
    w_id = int(sys.argv[1])
    start_worker(w_id)
#print('Worker node module ready')