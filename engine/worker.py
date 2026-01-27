import socket
import threading
import json
import sys
import os
import hashlib
import importlib

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils import load_config

PROBLEM_MODULE = "user_app"

def load_problem_module(problem_name, extra_args=None):
    """Load map/reduce functions from specified module"""
    global map_function, reduce_function
    module = importlib.import_module(problem_name)
    print(f"[WORKER] Extra args received: {extra_args}")
    if hasattr(module, 'configure_features'):
        module.configure_features(extra_args if extra_args else [])
    map_function = module.map_function
    reduce_function = module.reduce_function
    print(f"[WORKER] Loaded problem module: {problem_name}")

incoming_shuffle_data = []
lock = threading.Lock()

def handle_peer_connection(conn, addr):
    """Receive data from other workers"""
    print(f"[WORKER SERVER] Receiving data from peer {addr}")
    buffer = ""
    try:
        while True:
            chunk = conn.recv(4096).decode('utf-8')
            if not chunk: break
            buffer += chunk
            
        # When connection closes, store the data
        data = json.loads(buffer)
        with lock:
            incoming_shuffle_data.extend(data)
        print(f"[WORKER SERVER] Received {len(data)} items from peer.")
        
    except Exception as e:
        print(f"[WORKER SERVER] Error receiving data: {e}")
    finally:
        conn.close()

def start_worker_server(my_ip, my_port):
    """Listen for incoming worker connections"""
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((my_ip, my_port))
    server.listen()
    
    while True:
        conn, addr = server.accept()
        t = threading.Thread(target=handle_peer_connection, args=(conn, addr))
        t.start()

def start_worker(worker_id, problem_module="user_app", extra_args=None):
    load_problem_module(problem_module, extra_args)
    config = load_config()
    my_config = config['worker_nodes'][worker_id - 1]
    
    server_thread = threading.Thread(
        target=start_worker_server, 
        args=(my_config['ip'], my_config['port']),
        daemon=True
    )
    server_thread.start()
    
    print(f"[WORKER {worker_id}] Listening for peers on {my_config['port']}...")
    print(f"[WORKER {worker_id}] Connecting to Master...")
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect((config['master_node']['ip'], config['master_node']['port']))
    reg_msg = json.dumps({"type": "register", "worker_id": worker_id, "address": my_config})
    client.send(reg_msg.encode('utf-8'))
    
    buffer = ""
    while True:
        try:
            chunk = client.recv(4096 * 10).decode('utf-8')
            if not chunk: break
            buffer += chunk
            
            try:
                while "{" in buffer and "}" in buffer:
                    start = buffer.find("{")
                    try:
                        msg = json.loads(buffer)
                        buffer = "" 
                    except:
                        break

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
                    elif msg['type'] == 'start_shuffle':
                        all_workers = msg['workers']
                        print(f"[WORKER {worker_id}] Starting SHUFFLE...")
                        
                        with open(f"map_results_{worker_id}.json", 'r') as f:
                            my_data = json.load(f)
                        buckets = {wid: [] for wid in range(1, len(all_workers) + 1)}
                        for key, value in my_data:
                            hash_val = int(hashlib.md5(key.encode()).hexdigest(), 16)
                            target_id = (hash_val % len(all_workers)) + 1
                            buckets[target_id].append((key, value))
                        for target_id, data_part in buckets.items():
                            if target_id == worker_id:
                                with lock:
                                    incoming_shuffle_data.extend(data_part)
                            else:
                                # If for another, connect and send
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
    # Usage: python worker.py <worker_id> [problem_module] [--options]
    # Examples:
    #   python worker.py 1                              -> runs with user_app (problem 1)
    #   python worker.py 1 user_app                     -> runs with user_app (problem 1)
    #   python worker.py 1 user_app_problem2            -> runs with user_app_problem2 (problem 2)
    #   python worker.py 1 user_app_problem2 --all      -> problem 2 with all features
    #   python worker.py 1 user_app_problem2 --popularity --top-artists  -> specific features
    
    worker_id = int(sys.argv[1])
    problem_module = sys.argv[2] if len(sys.argv) > 2 and not sys.argv[2].startswith('--') else "user_app"
    
    # Collect extra arguments (--flags)
    extra_args = [arg for arg in sys.argv[2:] if arg.startswith('--')]
    
    start_worker(worker_id, problem_module, extra_args)
