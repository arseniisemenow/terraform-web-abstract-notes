#!/usr/bin/env python3
import json
import sys
from datetime import datetime

def health_check():
    """Simple health check for worker container"""
    try:
        health_data = {
            'status': 'healthy',
            'timestamp': datetime.now().isoformat(),
            'service': 'lecture-notes-worker',
            'version': '1.0.0'
        }
        print(json.dumps(health_data))
        sys.exit(0)
    except Exception as e:
        print(json.dumps({
            'status': 'unhealthy',
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }))
        sys.exit(1)

if __name__ == '__main__':
    health_check()