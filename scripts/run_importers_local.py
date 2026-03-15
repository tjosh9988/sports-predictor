# Run this script from your computer (not Render)
# Make sure your .env file is in the same folder
# Command: python scripts/upload_to_supabase_storage.py

import os
import subprocess
import sys

def run():
    print("--- Bet Hero Local Data Uploader ---")
    print("This script will upload all data in your 'stats/' folder to Supabase Storage.")
    print("Make sure you have created the 'sports-data' bucket in Supabase first.")
    
    confirm = input("Proceed? (y/n): ")
    if confirm.lower() != 'y':
        print("Aborted.")
        return

    script_path = os.path.join("scripts", "upload_to_supabase_storage.py")
    if not os.path.exists(script_path):
        print(f"Error: Could not find {script_path}")
        return

    try:
        subprocess.run([sys.executable, script_path], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Upload failed: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    run()
