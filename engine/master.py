import socket
import threading
import json
import sys
import os
import time

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils import load_config

connected_workers = {} 
worker_status = {}
lock = threading.Lock()

def handle_worker(conn, addr):
    worker_id = None
    try:
        while True:
            msg_bytes = conn.recv(4096)
            if not msg_bytes: break
            msg = json.loads(msg_bytes.decode('utf-8'))
            
            if msg['type'] == 'register':
                worker_id = msg['worker_id']
                with lock:
                    connected_workers[worker_id] = conn
                    worker_status[worker_id] = 'IDLE'
                print(f"[MASTER] Worker {worker_id} registered.")
                
            elif msg['type'] == 'map_done':
                print(f"[MASTER] Worker {worker_id} finished MAPPING.")
                with lock: worker_status[worker_id] = 'MAP_DONE'
                
            elif msg['type'] == 'shuffle_done':
                print(f"[MASTER] Worker {worker_id} finished SHUFFLING.")
                with lock: worker_status[worker_id] = 'SHUFFLE_DONE'

            elif msg['type'] == 'reduce_done':
                print(f"[MASTER] Worker {worker_id} finished REDUCING.")
                with lock: worker_status[worker_id] = 'REDUCE_DONE'
                
    except Exception as e:
        print(f"[MASTER] Worker {worker_id} disconnected.")
    finally:
        conn.close()

def orchestrate_job():
    config = load_config()
    total_workers = len(config['worker_nodes'])
    
    # 1. Start Mapping
    input("Press Enter to start MAP PHASE > ")
    
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_path = os.path.join(base_dir, 'data', 'dataset.csv')
    with open(data_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()[1:]
        
    worker_ids = list(connected_workers.keys())
    chunks = {wid: [] for wid in worker_ids}
    for i, line in enumerate(lines):
        chunks[worker_ids[i % len(worker_ids)]].append(line)
        
    for wid, chunk in chunks.items():
        msg = json.dumps({"type": "map_task", "data": chunk})
        connected_workers[wid].sendall(msg.encode('utf-8'))
        with lock: worker_status[wid] = 'MAPPING'
        
    print("[MASTER] Map tasks sent. Waiting for completion...")
    
    # Wait for Map Done
    while True:
        with lock:
            if all(s == 'MAP_DONE' for s in worker_status.values()) and len(worker_status) == len(worker_ids):
                break
        time.sleep(1)
    print("[MASTER] --- MAP PHASE COMPLETE ---")
    
    # 2. Start Shuffle
    input("Press Enter to start SHUFFLE PHASE > ")
    all_workers_list = config['worker_nodes']
    shuffle_msg = json.dumps({"type": "start_shuffle", "workers": all_workers_list})
    
    for conn in connected_workers.values():
        conn.sendall(shuffle_msg.encode('utf-8'))
        
    print("[MASTER] Shuffle started. Waiting for completion...")
    
    while True:
        with lock:
            if all(s == 'SHUFFLE_DONE' for s in worker_status.values()):
                break
        time.sleep(1)
    print("[MASTER] --- SHUFFLE PHASE COMPLETE ---")
    
    # 3. Start Reduce
    input("Press Enter to start REDUCE PHASE > ")
    
    reduce_msg = json.dumps({"type": "start_reduce"})
    for conn in connected_workers.values():
        conn.sendall(reduce_msg.encode('utf-8'))
        
    while True:
        with lock:
            if all(s == 'REDUCE_DONE' for s in worker_status.values()):
                break
        time.sleep(1)
    print("[MASTER] --- JOB COMPLETE ---")
    print("Check reduce_results_X.json files for output!")

def start_master():
    config = load_config()
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((config['master_node']['ip'], config['master_node']['port']))
    server.listen()
    
    threading.Thread(target=orchestrate_job).start()
    
    print(f"[MASTER] Listening on {config['master_node']['port']}...")
    while True:
        conn, addr = server.accept()
        threading.Thread(target=handle_worker, args=(conn, addr)).start()

if __name__ == "__main__":
    start_master()