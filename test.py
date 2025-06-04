from datetime import datetime, timedelta
import os
import json

def get_last_update_date():
    """Get last update from local file"""
    if os.path.exists('data/metadata.json'):
        with open('data/metadata.json', 'r') as f:
            metadata = json.load(f)
            return datetime.fromisoformat(metadata['last_update'])
    return datetime.now() - timedelta(days=8)

last_update = get_last_update_date()

print(last_update)
