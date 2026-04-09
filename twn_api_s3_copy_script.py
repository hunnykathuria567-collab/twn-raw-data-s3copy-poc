import subprocess
import concurrent.futures
import os

# Configuration
BUCKET_NAME = "amazon-s3-raw-data-bucket-461802173595-eu-north-1-an"
TOTAL_FILES = 10000
THREADS = 50  # Balanced for GitHub Actions Runner cores

def upload_file(file_id):
    # Simulate the Short Video durations: 10s, 15s, 20s
    duration = [10, 15, 20][file_id % 3]
    file_name = f"samples/short_vid_{file_id}_{duration}s.mp4"
    s3_path = f"s3://{BUCKET_NAME}/{file_name}"
    
    # Using echo to simulate a small file stream directly to S3
    # This avoids disk I/O on the GitHub runner
    cmd = f"echo 'metadata_content_for_video_{file_id}' | aws s3 cp - {s3_path} --quiet"
    
    try:
        subprocess.run(cmd, shell=True, check=True)
        if file_id % 500 == 0:
            print(f"✅ Ingested {file_id} files...")
        return True
    except Exception as e:
        print(f"❌ Error at {file_id}: {e}")
        return False

def main():
    print(f"🚀 Starting Parallel Ingestion of {TOTAL_FILES} files to {BUCKET_NAME}")
    
    # Setting AWS CLI optimization for concurrency
    subprocess.run("aws configure set default.s3.max_concurrent_requests 50", shell=True)

    with concurrent.futures.ThreadPoolExecutor(max_workers=THREADS) as executor:
        list(executor.map(upload_file, range(1, TOTAL_FILES + 1)))

    print("🏁 Ingestion Completed.")

if __name__ == "__main__":
    main()
