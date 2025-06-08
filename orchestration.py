import os
from datetime import datetime
import subprocess

LAST_LOAD_FILE = "last_load.txt"

def write_current_time():
    now = datetime.utcnow().isoformat()
    with open(LAST_LOAD_FILE, "w") as f:
        f.write(now)

def run_script(script_name):
    print(f"Running {script_name}...")
    subprocess.run(["python3", script_name], check=True)

def main():
    if not os.path.exists(LAST_LOAD_FILE):
        print("First run: creating last_load.txt")
        write_current_time()
        run_script("initial2.py")
    else:
        run_script("incremental2.py")

if __name__ == "__main__":
    main()
