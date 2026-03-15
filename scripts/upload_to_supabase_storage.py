import os
import sys
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
from supabase import create_client

# Load environment variables
load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    print("Error: SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must be set in .env")
    sys.exit(1)

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

MAX_FILE_SIZE_MB = 40  # Keep under 50MB limit
MAX_FILE_SIZE_BYTES = MAX_FILE_SIZE_MB * 1024 * 1024
BUCKET_NAME = "sports-data"

def upload_file(local_path: str, storage_path: str):
    """Uploads a file to Supabase Storage, skipping if it already exists."""
    try:
        storage_path = storage_path.replace("\\", "/")
        # Check if file exists
        files = supabase.storage.from_(BUCKET_NAME).list(os.path.dirname(storage_path))
        filename = os.path.basename(storage_path)
        
        if any(f['name'] == filename for f in files):
            print(f"  SKIP (exists): {storage_path}")
            return True

        with open(local_path, "rb") as f:
            supabase.storage.from_(BUCKET_NAME).upload(
                path=storage_path,
                file=f,
                file_options={
                    "content-type": "text/csv" if local_path.endswith(".csv") else "application/json",
                    "upsert": "true"
                }
            )
        print(f"  OK: {storage_path}")
        return True
    except Exception as e:
        print(f"  FAILED: {storage_path} — {e}")
        return False

def upload_large_csv(local_path: str, storage_path: str):
    """Split large CSV into chunks and upload each"""
    print(f"  Large file detected ({os.path.getsize(local_path) / (1024*1024):.1f} MB) — splitting into chunks...")
    
    try:
        # Read the CSV
        df = pd.read_csv(local_path, low_memory=False)
        total_rows = len(df)
        
        # Calculate rows per chunk based on file size
        file_size = os.path.getsize(local_path)
        num_chunks = (file_size // MAX_FILE_SIZE_BYTES) + 1
        rows_per_chunk = total_rows // num_chunks
        
        print(f"  Splitting {total_rows} rows into {num_chunks} chunks of ~{rows_per_chunk} rows")
        
        base_name = storage_path.replace(".csv", "")
        
        success_count = 0
        for i in range(num_chunks):
            start = i * rows_per_chunk
            end = start + rows_per_chunk if i < num_chunks - 1 else total_rows
            
            chunk = df.iloc[start:end]
            chunk_temp_path = f"temp_chunk_{i}.csv"
            chunk.to_csv(chunk_temp_path, index=False)
            
            chunk_storage_path = f"{base_name}_part{i+1}.csv"
            if upload_file(chunk_temp_path, chunk_storage_path):
                success_count += 1
            
            # Clean up temp file
            if os.path.exists(chunk_temp_path):
                os.remove(chunk_temp_path)
        
        return success_count == num_chunks
    except Exception as e:
        print(f"  FAILED to split: {e}")
        return False

def main():
    stats_dir = Path("stats")
    if not stats_dir.exists():
        print("Error: 'stats/' directory not found in the current folder.")
        return

    success = 0
    failed = 0
    print(f"Starting upload to Supabase bucket: {BUCKET_NAME}")
    print(f"Max file size: {MAX_FILE_SIZE_MB}MB")
    print("-" * 50)

    for root, dirs, files in os.walk(stats_dir):
        for filename in files:
            if not filename.endswith((".csv", ".json")):
                continue
                
            local_path = os.path.join(root, filename)
            # Maintain folder structure
            storage_path = os.path.relpath(local_path, stats_dir).replace("\\", "/")
            
            file_size = os.path.getsize(local_path)
            
            print(f"\nProcessing: {storage_path} ({file_size / (1024*1024):.1f} MB)")
            
            if filename.endswith(".csv") and file_size > MAX_FILE_SIZE_BYTES:
                result = upload_large_csv(local_path, storage_path)
            else:
                result = upload_file(local_path, storage_path)
            
            if result:
                success += 1
            else:
                failed += 1

    print("-" * 50)
    print("UPLOAD COMPLETE")
    print(f"Success: {success}")
    print(f"Failed:  {failed}")
    print("-" * 50)

if __name__ == "__main__":
    main()
