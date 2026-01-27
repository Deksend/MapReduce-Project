import json
import os

def load_config():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(base_dir, 'conf', 'config.json')
    
    with open(config_path, 'r') as f:
        return json.load(f)