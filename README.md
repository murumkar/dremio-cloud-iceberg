## Arctic table Cleanup
This Python script is designed to clean up data in Amazon S3 that corresponds to tables that have been dropped from a Dremio Nessie catalog. It fetches a list of dropped tables from the Nessie API and uses the AWS CLI to delete their associated S3 directories.

### Getting Started
#### Prerequisites
To run this script, you need to have the following installed:

* Python 3.x: The script is written in Python 3
* pip: Python's package installer
* AWS CLI: For managing S3 resources

##### Python Dependencies
The script relies on the following libraries to communicate with the Nessie API. You can install it using pip:

```
python3 -m pip install requests datetime pathlib
```

##### AWS CLI Setup
The script uses the AWS CLI to perform S3 operations. It's crucial to have it properly installed and configured.

Installation: Follow the official AWS CLI installation guide for your operating system.

Configuration: Configure your AWS credentials with sufficient permissions to access and delete files from your S3 bucket. Run the following command and enter your credentials when prompted:

```
aws configure
```
The IAM user or role you use must have the following permissions for the target S3 bucket:

```
s3:ListBucket
s3:DeleteObject
s3:DeleteObjectVersion
```

### Configuration
Before running the script, you need to create a config.json file in the same directory. This file holds all the necessary parameters.

config.json example:

```
{
  "catalogEndpoint": "https://nessie.dremio.cloud/repositories/<catalog ID>/api/v2",
  "s3RootPath": "s3://your-bucket-name/",
  "token": "your_dremio_api_token",
  "days": 7,
  "dryrun": true
}

catalogEndpoint: Get the URL from Arctic catalog settings.
s3RootPath: The root S3 path where your Iceberg tables are stored. Get from Arctic catalog settings.
token: A Dremio API token with permissions to read the catalog history.
days: The number of days of history to check for dropped tables. Defaults to 1.
dryrun: A boolean value (true or false). If true, the aws s3 rm command will be executed with the --dryrun flag, simulating the deletion without actually removing any data. This is useful for testing.
```

### Usage
Once the dependencies are installed and the config.json file is configured, you can run the script from your terminal:

```
python3 cleanup.py
```

The script will:
- Fetch the commit history of your Nessie catalog.
- Filter for DROP TABLE commands within the specified number of days.
- For each dropped table, it will retrieve the last known metadata location.
- It will then construct and run an aws s3 rm command to recursively delete the associated data directory in S3.
- If dryrun is set to true, the script will only print the commands it would have run without performing any deletions.
