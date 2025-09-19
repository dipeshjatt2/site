import os
import subprocess
import time
import threading
from flask import Flask

app = Flask(__name__)

APP_SCRIPT = "app.py"
CHECK_INTERVAL = 300  # 5 minutes
process = None

def is_process_running(name):
    try:
        # Use pgrep to check if script is running
        output = subprocess.check_output(["pgrep", "-f", name])
        return bool(output.strip())
    except subprocess.CalledProcessError:
        return False

def start_app():
    global process
    print(f"Starting {APP_SCRIPT}...")
    process = subprocess.Popen(["python3", APP_SCRIPT])

def monitor_app():
    while True:
        if not is_process_running(APP_SCRIPT):
            print(f"{APP_SCRIPT} is not running. Restarting...")
            start_app()
        else:
            print(f"{APP_SCRIPT} is running.")
        time.sleep(CHECK_INTERVAL)

@app.route("/")
def status():
    running = is_process_running(APP_SCRIPT)
    return f"{APP_SCRIPT} is {'running ✅' if running else 'not running ❌'}."

if __name__ == "__main__":
    # Start monitor in background
    monitor_thread = threading.Thread(target=monitor_app)
    monitor_thread.daemon = True
    monitor_thread.start()

    # Start Flask app
    app.run(host="0.0.0.0", port=8000)
