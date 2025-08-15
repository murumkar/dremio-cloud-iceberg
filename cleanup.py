#!/usr/bin/env python3

import requests
import re
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
import subprocess
from urllib.parse import quote

# Encode the table path for safe URL use with percent encoding for special characters.
def encode_nessie_path(table_path):
    """
    Encode the table name for safe URL use with percent encoding for special characters.
    For example converts: "my_folder".subfolder.mytable -> %my%5Ffolder%22%2Esubfolder%2Emytable
    """
    return quote(table_path, safe='')

# Load config
config_path = Path("config.json")
with open(config_path, "r") as f:
    config = json.load(f)

catalogEndpoint = config['catalogEndpoint'].rstrip("/")  # remove trailing slash
s3_root_path = config["s3RootPath"].rstrip("/")  # remove trailing slash
token = config["token"]
days = int(config.get("days", 1))  # default to 1 day if missing
dryrun = bool(config.get("dryrun", True)) # default to True if missing

# Calculate timestamp for filter (UTC, ISO 8601 format)
# Example: 2025-08-12T18:45:50Z
timestamp_since = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%dT%H:%M:%SZ")

# headers for Nessie API
headers = {
    "Authorization": f"Bearer {token}"
}

# Nessie filter expression
filter_expr = (
    f"commit.message.contains('DROP TABLE') && "
    f"timestamp(commit.commitTime) > timestamp('{timestamp_since}')"
)

params = {
    "filter": filter_expr
}

# Fetch history for last N days, based on the config.json file
historyUrl = f"{catalogEndpoint}/trees/main/history"
resp = requests.get(historyUrl, headers=headers, params=params)

if resp.status_code != 200:
    raise RuntimeError(f"Failed to fetch: {resp.status_code}, {resp.text}")

data = resp.json()

# Extract dropped tables using regex
dropped_tables = []
for entry in data.get("logEntries", []):
    message = entry.get("commitMeta", {}).get("message", "")
    parentCommitHash = entry.get("parentCommitHash", "")
    commitTime = entry.get("commitMeta", {}).get("commitTime", "")
    match = re.match(r"(?i)DROP\s+TABLE\s+(.+)", message.strip())
    if match:
        table_name = match.group(1)
        dropped_tables.append((table_name, commitTime, parentCommitHash))  # store as tuple

print(f"Cleaning up S3 storage for iceberg tables that were dropped in the catalog in the last {days} days (since {timestamp_since} UTC):")
# Let's get the table location for the dropped tables
for table_name, commitTime, parentCommitHash in dropped_tables:
    table_metadata_url = f"{catalogEndpoint}/trees/@{parentCommitHash}/contents/{encode_nessie_path(table_name)}"
    # print(f"Fetching metadata for table: {table_name} at commit: {parentCommitHash}")
    response = requests.get(table_metadata_url, headers=headers)
    
    if response.status_code != 200:
        print(f"Failed to fetch metadata for {table_name}: {response.status_code}, {response.text}")
        continue
    
    data = response.json()
    
    metadata_location = None
    if "content" in data and "ICEBERG_TABLE" in data["content"]["type"]:
        metadata_location = data["content"]["metadataLocation"]
    else:
        print(f"Could not find ICEBERG_TABLE content for {table_name}")
    
    if metadata_location:
        # Get the path to the data directory
        data_path = metadata_location.split('/metadata/')[0]

        # Conditionally add --dryrun
        dryrun_flag = "--dryrun" if dryrun else ""
        
        # Delete using AWS CLI
        print(f" -  Table: {table_name} \n     - S3: {data_path}")
        cmd = f"aws s3 rm \"{data_path}\" --recursive {dryrun_flag}"
        
        # Run the command
        subprocess.run(cmd, shell=True, check=True)
