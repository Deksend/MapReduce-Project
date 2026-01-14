# Εδώ θα μπει ο κώδικας του Master Node
# engine/master.py
import socket
import threading
import json
import sys
import os

# Προσθήκη του parent directory στο path για να βλέπουμε το utils.py
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils import load_config

def handle_worker(conn, addr):
    print(f"[MASTER] Worker connected from: {addr}")
    
    try:
        # Περιμένουμε το πρώτο μήνυμα από τον Worker
        msg = conn.recv(1024).decode('utf-8')
        print(f"[MASTER] Received: {msg}")
        
        # Στέλνουμε απάντηση (ACK)
        response = json.dumps({"status": "OK", "message": "Welcome Worker!"})
        conn.send(response.encode('utf-8'))
        
    except Exception as e:
        print(f"[MASTER] Error with worker {addr}: {e}")
    finally:
        conn.close()

def start_master():
    config = load_config()
    master_ip = config['master_node']['ip']
    master_port = config['master_node']['port']
    
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((master_ip, master_port))
    server.listen()
    
    print(f"[MASTER] Listening on {master_ip}:{master_port}...")
    
    while True:
        # Αποδοχή νέων συνδέσεων
        conn, addr = server.accept()
        # Ξεκινάμε νέο thread για κάθε worker που συνδέεται
        thread = threading.Thread(target=handle_worker, args=(conn, addr))
        thread.start()

if __name__ == "__main__":
    start_master()
#print('Master node module ready')