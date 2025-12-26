"""
Schedule automatic database updates.

This script can be run:
1. Manually: python schedule_update.py
2. Via Windows Task Scheduler
3. Via cron (Linux/Mac)
4. As a service

For Windows Task Scheduler:
- Create a new task
- Set trigger (e.g., daily at 2 AM)
- Action: Start a program
- Program: python
- Arguments: "C:\Users\user\Documents\Projects\cooperation parsing\schedule_update.py"
- Start in: "C:\Users\user\Documents\Projects\cooperation parsing"
"""

import subprocess
import sys
import os
from datetime import datetime

def run_update():
    """Run the update script"""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    update_script = os.path.join(script_dir, "update_database.py")
    
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Starting update...")
    
    try:
        result = subprocess.run(
            [sys.executable, update_script],
            cwd=script_dir,
            capture_output=True,
            text=True,
            timeout=3600  # 1 hour timeout
        )
        
        if result.returncode == 0:
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Update completed successfully")
            print(result.stdout)
        else:
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Update failed with error:")
            print(result.stderr)
            return False
            
    except subprocess.TimeoutExpired:
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Update timed out")
        return False
    except Exception as e:
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Error running update: {e}")
        return False
    
    return True

if __name__ == "__main__":
    success = run_update()
    sys.exit(0 if success else 1)




